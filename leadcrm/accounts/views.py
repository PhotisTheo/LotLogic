import json
import logging
import uuid

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse, reverse_lazy
from django.utils import timezone
from django.views.decorators.http import require_POST
from django.views.generic import TemplateView

from .emails import send_beta_signup_notification, send_email_verification
from .forms import ProfileUpdateForm, TeamInviteForm, UserSignupForm, StyledPasswordChangeForm
from .models import (
    BetaSignupRequest,
    EmailVerification,
    TeamInvite,
    UserProfile,
    get_workspace_owner,
)
from .plans import (
    ACCOUNT_INDIVIDUAL,
    ACCOUNT_TEAM_LEAD,
    ACCOUNT_TEAM_MEMBER,
    DEFAULT_PLAN_ID,
    PLAN_CATALOG,
    PLAN_GROUPS,
    PUBLIC_SIGNUP_PLAN_IDS,
    get_plan,
    plans_for_account_type,
)

from leads.forms import MailerTemplateForm
from leads.models import MailerTemplate

try:
    import stripe
except ImportError:
    stripe = None

if stripe and getattr(settings, "STRIPE_SECRET_KEY", ""):
    stripe.api_key = settings.STRIPE_SECRET_KEY


logger = logging.getLogger(__name__)


def _mailer_placeholder_hints() -> list[tuple[str, str]]:
    return [
        ("{salutation_name}", "Owner-friendly greeting (e.g., Patricia)"),
        ("{property_address}", "Full mailing address for the parcel"),
        ("{property_city_friendly}", "City or area the parcel belongs to"),
        ("{property_descriptor}", "Readable property description (e.g., 3-family home)"),
        ("{focus_area}", "Neighborhood or focus area derived from parcel data"),
        ("{equity_sentence}", "Equity talking point tailored per parcel"),
        ("{agent_intro_sentence}", "Sentence introducing you with name/role/company"),
        ("{agent_name}", "Your name from workspace identity"),
        ("{agent_title_line}", "Role or title from workspace identity"),
        ("{agent_company_line}", "Company name from workspace identity"),
        ("{contact_phone}", "Primary phone pulled from your profile"),
        ("{text_keyword_upper}", "SMS keyword in uppercase (if configured)"),
    ]


def _get_invite(token: str | None) -> TeamInvite | None:
    if not token:
        return None
    try:
        invite = TeamInvite.objects.select_related(
            "team_lead", "team_lead__profile"
        ).get(token=token)
    except (TeamInvite.DoesNotExist, ValueError):
        return None
    return invite


def _plan_options_for_display() -> list[dict[str, object]]:
    options = []
    plan_ids: tuple[str, ...] = PUBLIC_SIGNUP_PLAN_IDS or (DEFAULT_PLAN_ID,)
    seen: set[str] = set()
    for plan_id in plan_ids:
        if plan_id in seen:
            continue
        seen.add(plan_id)
        plan = PLAN_CATALOG.get(plan_id)
        if not plan:
            continue
        options.append(
            {
                "id": plan["id"],
                "label": plan["label"],
                "subtitle": plan.get("subtitle", ""),
                "price_display": plan.get("price_display", ""),
                "price_cents": plan.get("price_cents", 0),
                "account_type": plan.get("account_type"),
                "seat_limit": plan.get("seat_limit"),
                "features": plan.get("features", []),
                "cta": plan.get("cta", ""),
                "highlight": plan.get("highlight", False),
                "badge": plan.get("badge"),
                "badge_style": plan.get("badge_style"),
                "upgrade_note": plan.get("upgrade_note"),
            }
        )
    if not options:
        plan = PLAN_CATALOG.get(DEFAULT_PLAN_ID)
        if plan:
            options.append(
                {
                    "id": plan["id"],
                    "label": plan["label"],
                    "subtitle": plan.get("subtitle", ""),
                    "price_display": plan.get("price_display", ""),
                    "price_cents": plan.get("price_cents", 0),
                    "account_type": plan.get("account_type"),
                    "seat_limit": plan.get("seat_limit"),
                    "features": plan.get("features", []),
                    "cta": plan.get("cta", ""),
                    "highlight": plan.get("highlight", False),
                    "badge": plan.get("badge"),
                    "badge_style": plan.get("badge_style"),
                    "upgrade_note": plan.get("upgrade_note"),
                }
            )
    return options


def signup(request):
    if request.user.is_authenticated:
        return redirect("accounts:profile")

    invite_token = request.GET.get("invite") or request.POST.get("invite_token")
    invite = _get_invite(invite_token)

    plan_options = _plan_options_for_display()
    plan_catalog_map = {plan["id"]: plan for plan in PLAN_CATALOG.values()}

    stripe_enabled = bool(
        getattr(settings, "STRIPE_SECRET_KEY", "")
        and getattr(settings, "STRIPE_PUBLISHABLE_KEY", "")
    )

    payment_intent_endpoint = (
        reverse("accounts:signup_payment_intent") if stripe_enabled else ""
    )

    if request.method == "POST":
        form = UserSignupForm(request.POST, invite=invite, plan_groups=PLAN_GROUPS)
        selected_plan_id = form.data.get("plan_id") or DEFAULT_PLAN_ID
        plan = get_plan(selected_plan_id)
        price_cents = plan.get("price_cents", 0)
        requires_payment = invite is None and price_cents > 0
        plan_requires_manual_approval = bool(plan.get("requires_manual_approval"))
        requires_manual_approval = (
            not invite
            and plan_requires_manual_approval
            and getattr(settings, "BETA_REQUIRE_APPROVAL", True)
        )

        if form.is_valid():
            plan_id = form.cleaned_data.get("plan_id") or selected_plan_id
            plan = get_plan(plan_id)
            account_type = plan.get("account_type") or form.cleaned_data.get(
                "account_type"
            )
            price_cents = plan.get("price_cents", 0)
            requires_payment = invite is None and price_cents > 0
            plan_requires_manual_approval = bool(plan.get("requires_manual_approval"))
            requires_manual_approval = (
                not invite
                and plan_requires_manual_approval
                and getattr(settings, "BETA_REQUIRE_APPROVAL", True)
            )

            subscription_id = (request.POST.get("stripe_subscription_id") or "").strip()
            customer_id_from_form = (
                request.POST.get("stripe_customer_id") or ""
            ).strip()
            payment_intent_id = (request.POST.get("payment_intent_id") or "").strip()
            invoice_id_from_form = (
                request.POST.get("stripe_invoice_id") or ""
            ).strip()
            invoice_id = invoice_id_from_form

            subscription = None
            payment_intent = None
            payment_method_id = ""
            invoice_status = ""
            invoice_data = {}

            if requires_payment:
                if not stripe_enabled or not stripe:
                    form.add_error(
                        None,
                        "Online payments are not currently available. Please contact support to complete your signup.",
                    )
                elif not subscription_id or not payment_intent_id:
                    form.add_error(
                        None,
                        "We couldn't capture your payment details. Please re-enter your card and try again.",
                    )
                else:
                    try:
                        subscription = stripe.Subscription.retrieve(
                            subscription_id,
                            expand=[
                                "latest_invoice.payment_intent",
                                "items.data.price",
                            ],
                        )
                    except Exception:
                        subscription = None

                    if not subscription:
                        form.add_error(
                            None,
                            "We couldn't verify your subscription with Stripe. Please try again.",
                        )
                    else:
                        invoice_data = subscription.get("latest_invoice") or {}
                        invoice_status = invoice_data.get("status") if invoice_data else ""
                        if not invoice_id:
                            invoice_id = invoice_data.get("id") or invoice_id
                        payment_intent = invoice_data.get("payment_intent")
                        if (
                            not payment_intent
                            or payment_intent.get("id") != payment_intent_id
                        ):
                            try:
                                payment_intent = stripe.PaymentIntent.retrieve(
                                    payment_intent_id
                                )
                            except Exception:
                                payment_intent = None

                        if not payment_intent:
                            form.add_error(
                                None,
                                "We couldn't verify your payment. Please try again.",
                            )
                        else:
                            expected_amount = int(price_cents)
                            actual_amount = int(payment_intent.get("amount") or 0)
                            if actual_amount != expected_amount:
                                form.add_error(
                                    None,
                                    "The payment amount did not match the selected plan. Please try again.",
                                )
                            else:
                                expected_currency = getattr(
                                    settings, "STRIPE_CURRENCY", "usd"
                                ).lower()
                                payment_currency = (
                                    payment_intent.get("currency") or ""
                                ).lower()
                                if payment_currency != expected_currency:
                                    form.add_error(
                                        None,
                                        "Unexpected payment currency. Please contact support.",
                                    )
                                else:
                                    intent_metadata = (
                                        payment_intent.get("metadata") or {}
                                    )
                                    if not invoice_id:
                                        invoice_id = (
                                            intent_metadata.get("invoice_id") or invoice_id
                                        )
                                    if (
                                        intent_metadata.get("plan_id")
                                        and intent_metadata["plan_id"] != plan_id
                                    ):
                                        form.add_error(
                                            None,
                                            "The payment plan did not match your selection. Please try again.",
                                        )
                                    else:
                                        intent_status = payment_intent.get("status")
                                        if intent_status not in {
                                            "succeeded",
                                            "requires_capture",
                                            "processing",
                                        }:
                                            form.add_error(
                                                None,
                                                "Payment has not completed yet. Please confirm the charge and try again.",
                                            )
                                        payment_method_id = (
                                            payment_intent.get("payment_method")
                                            or payment_method_id
                                        )

                                        subscription_items = subscription.get(
                                            "items", {}
                                        ).get("data", [])
                                        price_ids = {
                                            item.get("price", {}).get("id")
                                            for item in subscription_items
                                            if item.get("price")
                                        }
                                        expected_price_id = plan.get("stripe_price_id")
                                        if (
                                            expected_price_id
                                            and expected_price_id not in price_ids
                                        ):
                                            form.add_error(
                                                None,
                                                "The subscription price did not match your selected plan. Please try again.",
                                            )

            if (
                requires_payment
                and not form.errors
                and invoice_id
                and stripe
                and payment_intent
                and payment_intent.get("status") == "succeeded"
                and invoice_status not in {"paid", "void"}
            ):
                try:
                    pay_kwargs = {}
                    if payment_method_id:
                        pay_kwargs["payment_method"] = payment_method_id
                    stripe.Invoice.pay(invoice_id, **pay_kwargs)
                    invoice_data = stripe.Invoice.retrieve(invoice_id)
                    invoice_status = invoice_data.get("status") if invoice_data else ""
                    # refresh subscription state after payment
                    subscription = stripe.Subscription.retrieve(
                        subscription_id,
                        expand=["latest_invoice.payment_intent", "items.data.price"],
                    )
                    try:
                        payment_intent = stripe.PaymentIntent.retrieve(
                            payment_intent_id
                        )
                        payment_method_id = (
                            payment_intent.get("payment_method") or payment_method_id
                        )
                    except Exception as exc:
                        logger.warning(
                            "Unable to refresh payment intent after invoice payment",
                            exc_info=exc,
                            extra={
                                "payment_intent_id": payment_intent_id,
                                "invoice_id": invoice_id,
                            },
                        )
                except Exception as exc:
                    logger.error(
                        "Failed to mark invoice paid for subscription signup",
                        exc_info=exc,
                        extra={
                            "subscription_id": subscription_id,
                            "invoice_id": invoice_id,
                            "payment_intent_id": payment_intent_id,
                        },
                    )
                    form.add_error(
                        None,
                        "We collected your payment but could not finalize the subscription automatically. Please contact support with your receipt.",
                    )

            if not form.errors:
                user = form.save(commit=False)
                user.is_active = False
                user.save()

                profile = user.profile
                plan_account_type = plan.get("account_type", account_type)

                if invite:
                    profile.account_type = UserProfile.ACCOUNT_TEAM_MEMBER
                    profile.team_lead = invite.team_lead
                    invite.accepted_user = user
                    invite.accepted_at = timezone.now()
                    invite.save(update_fields=["accepted_user", "accepted_at"])
                elif plan_account_type == UserProfile.ACCOUNT_TEAM_LEAD:
                    profile.account_type = UserProfile.ACCOUNT_TEAM_LEAD
                    profile.team_lead = None
                else:
                    profile.account_type = UserProfile.ACCOUNT_INDIVIDUAL
                    profile.team_lead = None

                profile.plan_id = plan_id
                profile.plan_amount_cents = price_cents

                if requires_payment:
                    subscription_status = (
                        subscription.get("status") if subscription else ""
                    )
                    customer_id_resolved = (
                        subscription.get("customer")
                        or customer_id_from_form
                        or (payment_intent.get("customer") if payment_intent else None)
                    )
                    if (
                        stripe
                        and customer_id_resolved
                        and payment_method_id
                    ):
                        try:
                            stripe.Customer.modify(
                                customer_id_resolved,
                                invoice_settings={
                                    "default_payment_method": payment_method_id
                                },
                            )
                        except Exception as exc:
                            logger.warning(
                                "Unable to set default payment method for customer",
                                exc_info=exc,
                                extra={
                                    "customer_id": customer_id_resolved,
                                    "payment_method_id": payment_method_id,
                                },
                            )
                    profile.billing_status = (
                        "active"
                        if subscription_status in {"active", "trialing"}
                        else "incomplete"
                    )
                    profile.payment_intent_id = (
                        payment_intent.get("id") if payment_intent else None
                    )
                    profile.stripe_customer_id = (
                        customer_id_resolved
                    )
                    profile.stripe_subscription_id = (
                        subscription.get("id") if subscription else None
                    )
                else:
                    profile.billing_status = "included"
                    profile.payment_intent_id = None
                    profile.stripe_subscription_id = None
                if requires_manual_approval:
                    profile.billing_status = "beta_pending"

                profile.save(
                    update_fields=[
                        "account_type",
                        "team_lead",
                        "plan_id",
                        "plan_amount_cents",
                        "billing_status",
                        "payment_intent_id",
                        "stripe_customer_id",
                        "stripe_subscription_id",
                    ]
                )

                verification, _ = EmailVerification.objects.get_or_create(user=user)
                verification.email = user.email
                verification.token = uuid.uuid4()
                verification.confirmed_at = None
                verification.save()

                confirmation_url = request.build_absolute_uri(
                    reverse("accounts:confirm_email", args=[verification.token])
                )
                try:
                    send_email_verification(verification, confirmation_url)
                except Exception:
                    logger.exception(
                        "Failed to send email verification",
                        extra={"user_id": user.id},
                    )

                if requires_manual_approval:
                    beta_request, _ = BetaSignupRequest.objects.get_or_create(
                        user=user
                    )
                    beta_request.plan_id = plan_id
                    beta_request.status = BetaSignupRequest.STATUS_PENDING
                    beta_request.approved_at = None
                    beta_request.approved_by = ""
                    beta_request.token = uuid.uuid4()
                    beta_request.save()

                    approval_url = request.build_absolute_uri(
                        reverse("accounts:beta_request_approve", args=[beta_request.token])
                    )
                    try:
                        send_beta_signup_notification(beta_request, approval_url)
                    except Exception:
                        logger.exception(
                            "Failed to send beta signup notification",
                            extra={"user_id": user.id, "plan_id": plan_id},
                        )

                    messages.info(
                        request,
                        "Check your inbox to confirm your email. Once it's verified, we'll approve your beta access and let you know when you can log in.",
                    )
                else:
                    messages.success(
                        request,
                        "We sent a confirmation link to your email—verify it to finish setting up your account.",
                    )
                return redirect("accounts:login")

        selected_plan_id = form.data.get("plan_id") or DEFAULT_PLAN_ID
        plan = get_plan(selected_plan_id)
        requires_payment = invite is None and plan.get("price_cents", 0) > 0
    else:
        form = UserSignupForm(invite=invite, plan_groups=PLAN_GROUPS)
        selected_plan_id = "team_member_included" if invite else DEFAULT_PLAN_ID
        plan = get_plan(selected_plan_id)
        requires_payment = invite is None and plan.get("price_cents", 0) > 0

    return render(
        request,
        "accounts/signup.html",
        {
            "form": form,
            "invite": invite,
            "invite_token": invite.token if invite else "",
            "plan_options": plan_options,
            "plan_catalog": plan_catalog_map,
            "plan_groups": PLAN_GROUPS,
            "selected_plan_id": selected_plan_id,
            "stripe_enabled": stripe_enabled,
            "payment_intent_endpoint": payment_intent_endpoint,
            "requires_payment": requires_payment,
            "stripe_currency": getattr(settings, "STRIPE_CURRENCY", "usd"),
            # 🔑 THIS IS WHAT WAS MISSING:
            "stripe_publishable_key": getattr(settings, "STRIPE_PUBLISHABLE_KEY", ""),
        },
    )


class ProfileView(LoginRequiredMixin, TemplateView):
    template_name = "accounts/profile.html"
    login_url = reverse_lazy("accounts:login")

    def get_profile_form(self):
        if self.request.method == "POST":
            return ProfileUpdateForm(self.request.POST, user=self.request.user)
        return ProfileUpdateForm(user=self.request.user)

    def post(self, request, *args, **kwargs):
        form = ProfileUpdateForm(request.POST, user=request.user)
        if form.is_valid():
            form.save()
            messages.success(request, "Profile updated.")
            return redirect("accounts:profile")
        context = self.get_context_data(profile_form=form)
        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        from leads.models import (  # local import to avoid circular dependency
            Lead,
            SavedParcelList,
        )
        from leads.mailers import get_mailer_script_options

        profile_form = kwargs.get("profile_form") or self.get_profile_form()
        context = super().get_context_data(**kwargs)
        user = self.request.user
        profile = user.profile
        workspace_owner = get_workspace_owner(user)

        saved_lists_qs = SavedParcelList.objects.filter(
            created_by=workspace_owner
        )
        saved_list_count = saved_lists_qs.count()
        active_lead_count = Lead.objects.filter(created_by=workspace_owner).count()

        mailer_saved_lists: list[dict[str, object]] = []
        if workspace_owner:
            for saved_list in saved_lists_qs.filter(archived_at__isnull=True):
                loc_ids = saved_list.loc_ids or []
                parcel_count = len(loc_ids) if isinstance(loc_ids, list) else 0
                mailer_saved_lists.append(
                    {
                        "id": saved_list.pk,
                        "name": saved_list.name,
                        "town_name": saved_list.town_name,
                        "parcel_count": parcel_count,
                        "endpoint": reverse(
                            "saved_parcel_list_mailers", args=[saved_list.pk]
                        ),
                    }
                )

        custom_template_qs = (
            MailerTemplate.objects.filter(owner=workspace_owner)
            if workspace_owner
            else MailerTemplate.objects.none()
        )

        mailer_script_options_raw = get_mailer_script_options(workspace_owner)
        mailer_script_options = [
            {
                "id": option.id,
                "label": option.label,
                "description": option.summary,
                "sector": option.sector,
                "promptText": option.prompt_text or "",
                "is_custom": bool(option.custom_template_id),
            }
            for option in mailer_script_options_raw
        ]
        mailer_default_script = (
            mailer_script_options[0]["id"] if mailer_script_options else ""
        )

        team_members = []
        pending_invites = []
        invite_form = None

        if profile.account_type == UserProfile.ACCOUNT_TEAM_LEAD:
            team_members = (
                UserProfile.objects.select_related("user")
                .filter(team_lead=user)
                .order_by("user__username")
            )
            pending_invites = TeamInvite.objects.filter(
                team_lead=user, accepted_at__isnull=True
            ).order_by("-created_at")
            invite_form = TeamInviteForm()

        elif (
            profile.account_type == UserProfile.ACCOUNT_TEAM_MEMBER
            and profile.team_lead
        ):
            team_members = (
                UserProfile.objects.select_related("user")
                .filter(team_lead=profile.team_lead)
                .order_by("user__username")
            )
            pending_invites = TeamInvite.objects.filter(
                team_lead=profile.team_lead, accepted_at__isnull=True
            ).order_by("-created_at")

        last_invite_url = self.request.session.pop("last_invite_url", None)
        last_invite_accept_url = self.request.session.pop(
            "last_invite_accept_url", None
        )

        from .plans import PLAN_CATALOG

        upgrade_options = []
        if profile.account_type != UserProfile.ACCOUNT_TEAM_MEMBER:
            for plan in PLAN_CATALOG.values():
                plan_account_type = plan.get("account_type")
                if plan["id"] == profile.plan_id:
                    continue
                if (
                    profile.account_type == UserProfile.ACCOUNT_INDIVIDUAL
                    and plan_account_type == UserProfile.ACCOUNT_INDIVIDUAL
                    and plan["price_cents"] <= profile.plan.get("price_cents", 0)
                ):
                    continue
                upgrade_options.append(
                    {
                        **plan,
                        "url": f"{reverse('accounts:signup')}?plan={plan['id']}",
                    }
                )

        context.update(
            {
                "profile": profile,
                "workspace_owner": workspace_owner,
                "saved_list_count": saved_list_count,
                "active_lead_count": active_lead_count,
                "team_members": team_members,
                "pending_invites": pending_invites,
                "invite_form": invite_form,
                "remaining_invites": (
                    profile.remaining_invites
                    if profile.account_type == UserProfile.ACCOUNT_TEAM_LEAD
                    else 0
                ),
                "last_invite_url": last_invite_url,
                "last_invite_accept_url": last_invite_accept_url,
                "plan_details": profile.plan,
                "profile_form": profile_form,
                "upgrade_options": upgrade_options,
                "mailer_saved_lists": mailer_saved_lists,
                "mailer_script_options": mailer_script_options,
                "mailer_default_script": mailer_default_script,
                "custom_mailer_template_count": custom_template_qs.count(),
            }
        )
        return context


class TermsView(TemplateView):
    template_name = "accounts/terms.html"


class SettingsView(LoginRequiredMixin, TemplateView):
    template_name = "accounts/settings.html"
    login_url = reverse_lazy("accounts:login")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        user = self.request.user
        profile = user.profile

        email_verification = None
        try:
            email_verification = EmailVerification.objects.get(user=user)
        except EmailVerification.DoesNotExist:
            pass

        context.update(
            {
                "profile": profile,
                "plan_details": profile.plan,
                "email_verification": email_verification,
            }
        )
        return context


@login_required
def change_password(request):
    """
    Handle password change requests.
    """
    if request.method == "POST":
        form = StyledPasswordChangeForm(user=request.user, data=request.POST)
        if form.is_valid():
            user = form.save()
            # Update session to prevent logout after password change
            from django.contrib.auth import update_session_auth_hash
            update_session_auth_hash(request, user)

            logger.info(
                f"Password changed for user {user.username}",
                extra={"user_id": user.id},
            )
            messages.success(request, "Your password has been changed successfully.")
            return redirect("accounts:settings")
    else:
        form = StyledPasswordChangeForm(user=request.user)

    return render(request, "accounts/change_password.html", {"form": form})


@login_required
@require_POST
def delete_account(request):
    """
    Delete user account and cancel Stripe subscription if applicable.
    """
    confirmation = request.POST.get("confirmation", "").strip()
    if confirmation != "DELETE":
        messages.error(request, "Account deletion cancelled. Confirmation text did not match.")
        return redirect("accounts:settings")

    user = request.user
    profile = user.profile

    # Cancel Stripe subscription if exists
    if profile.stripe_subscription_id and stripe:
        try:
            stripe.Subscription.delete(profile.stripe_subscription_id)
            logger.info(
                f"Cancelled Stripe subscription for user {user.username}",
                extra={
                    "user_id": user.id,
                    "subscription_id": profile.stripe_subscription_id,
                },
            )
        except Exception as exc:
            logger.error(
                f"Failed to cancel Stripe subscription for user {user.username}",
                exc_info=exc,
                extra={
                    "user_id": user.id,
                    "subscription_id": profile.stripe_subscription_id,
                },
            )
            messages.warning(
                request,
                "We couldn't cancel your Stripe subscription automatically. Please contact support to ensure billing is stopped.",
            )

    # If team lead, handle team members
    if profile.account_type == UserProfile.ACCOUNT_TEAM_LEAD:
        team_members = UserProfile.objects.filter(team_lead=user)
        for member_profile in team_members:
            member_profile.account_type = UserProfile.ACCOUNT_INDIVIDUAL
            member_profile.team_lead = None
            member_profile.plan_id = "individual_free"
            member_profile.plan_amount_cents = 0
            member_profile.billing_status = "active"
            member_profile.save(
                update_fields=[
                    "account_type",
                    "team_lead",
                    "plan_id",
                    "plan_amount_cents",
                    "billing_status",
                ]
            )
        logger.info(
            f"Removed {team_members.count()} team members from team lead {user.username}",
            extra={"user_id": user.id, "team_member_count": team_members.count()},
        )

    # Delete user (this will cascade to profile and related data)
    username = user.username
    user.delete()

    logger.info(
        f"Account deleted for user {username}",
        extra={"username": username},
    )

    messages.success(
        request,
        "Your account has been permanently deleted. We're sorry to see you go.",
    )
    return redirect("accounts:login")


class MailerTemplateListView(LoginRequiredMixin, TemplateView):
    template_name = "accounts/mailer_templates.html"
    login_url = reverse_lazy("accounts:login")

    def _workspace_owner(self):
        return get_workspace_owner(self.request.user)

    def get_queryset(self):
        owner = self._workspace_owner()
        if not owner:
            return MailerTemplate.objects.none()
        return MailerTemplate.objects.filter(owner=owner).order_by("-updated_at")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        templates = self.get_queryset()
        context.update(
            {
                "templates": templates,
                "create_form": kwargs.get("create_form") or MailerTemplateForm(),
                "placeholder_hints": _mailer_placeholder_hints(),
                "workspace_owner": self._workspace_owner(),
            }
        )
        return context

    def post(self, request, *args, **kwargs):
        owner = self._workspace_owner()
        if owner is None:
            messages.error(
                request,
                "We couldn't determine your workspace owner. Please try again or contact support.",
            )
            return redirect("accounts:profile")

        form = MailerTemplateForm(request.POST)
        if form.is_valid():
            template = form.save(commit=False)
            template.owner = owner
            template.created_by = request.user
            template.save()
            messages.success(request, "Mailer template created.")
            return redirect("accounts:mailer_templates")

        context = self.get_context_data(create_form=form)
        return self.render_to_response(context)


@login_required
def mailer_template_edit(request, pk):
    owner = get_workspace_owner(request.user)
    if owner is None:
        messages.error(
            request,
            "We couldn't determine your workspace owner. Please try again or contact support.",
        )
        return redirect("accounts:profile")

    template = get_object_or_404(MailerTemplate, pk=pk, owner=owner)

    if request.method == "POST":
        form = MailerTemplateForm(request.POST, instance=template)
        if form.is_valid():
            updated_template = form.save(commit=False)
            updated_template.owner = owner
            updated_template.save()
            messages.success(request, "Mailer template updated.")
            return redirect("accounts:mailer_templates")
    else:
        form = MailerTemplateForm(instance=template)

    return render(
        request,
        "accounts/mailer_template_edit.html",
        {
            "form": form,
            "template_obj": template,
            "placeholder_hints": _mailer_placeholder_hints(),
        },
    )


@login_required
@require_POST
def mailer_template_delete(request, pk):
    owner = get_workspace_owner(request.user)
    if owner is None:
        messages.error(
            request,
            "We couldn't determine your workspace owner. Please try again or contact support.",
        )
        return redirect("accounts:profile")

    template = get_object_or_404(MailerTemplate, pk=pk, owner=owner)
    template.delete()
    messages.success(request, f"Deleted template '{template.name}'.")
    return redirect("accounts:mailer_templates")


@login_required
def team_invite_create(request):
    profile = request.user.profile
    if profile.account_type != UserProfile.ACCOUNT_TEAM_LEAD:
        messages.error(request, "Only team leads can invite teammates.")
        return redirect("accounts:profile")

    if request.method != "POST":
        return redirect("accounts:profile")

    # Superusers bypass seat limit check
    if not request.user.is_superuser and profile.remaining_invites <= 0:
        messages.error(
            request, "You have reached the maximum number of team invitations."
        )
        return redirect("accounts:profile")

    form = TeamInviteForm(request.POST)
    if not form.is_valid():
        messages.error(request, "Please enter a valid email address.")
        return redirect("accounts:profile")

    email = form.cleaned_data["email"].strip().lower()

    lead_emails = set()
    if request.user.email:
        lead_emails.add(request.user.email.strip().lower())
    lead_emails.add(request.user.username.strip().lower())
    if email in lead_emails:
        messages.error(request, "You can't send an invite to yourself.")
        return redirect("accounts:profile")

    invite, created = TeamInvite.objects.get_or_create(
        team_lead=request.user,
        email=email,
    )

    if not created and invite.is_accepted:
        messages.info(request, f"{email} is already on your team.")
        return redirect("accounts:profile")

    if not created and invite.accepted_at is None:
        messages.info(request, f"An invite is already pending for {email}.")
    else:
        if not created:
            invite.token = uuid.uuid4()
            invite.accepted_at = None
            invite.accepted_user = None
            invite.save(update_fields=["token", "accepted_at", "accepted_user"])
        messages.success(
            request,
            f"Invite created for {email}. Share the link below to have them join your team.",
        )

    invite_url = request.build_absolute_uri(
        f"{reverse('accounts:signup')}?invite={invite.token}"
    )
    accept_url = request.build_absolute_uri(
        reverse("accounts:team_invite_accept", args=[invite.token])
    )
    request.session["last_invite_url"] = invite_url
    request.session["last_invite_accept_url"] = accept_url

    return redirect("accounts:profile")


@login_required
def team_invite_accept(request, token):
    invite = get_object_or_404(
        TeamInvite.objects.select_related("team_lead", "team_lead__profile"),
        token=token,
    )

    if not invite.can_accept():
        messages.error(request, "This invite has expired or the team is already full.")
        return redirect("accounts:profile")

    profile = request.user.profile
    if (
        profile.account_type == UserProfile.ACCOUNT_TEAM_LEAD
        and profile.team_lead is None
    ):
        messages.error(request, "Team leads cannot join another team.")
        return redirect("accounts:profile")
    if (
        profile.account_type == UserProfile.ACCOUNT_TEAM_MEMBER
        and profile.team_lead not in {None, invite.team_lead}
    ):
        messages.error(request, "You're already part of a different team.")
        return redirect("accounts:profile")

    profile.account_type = UserProfile.ACCOUNT_TEAM_MEMBER
    profile.team_lead = invite.team_lead
    profile.plan_id = "team_member_included"
    profile.plan_amount_cents = 0
    profile.billing_status = "included"
    profile.payment_intent_id = None
    profile.stripe_subscription_id = None
    profile.stripe_customer_id = None
    profile.save(
        update_fields=[
            "account_type",
            "team_lead",
            "plan_id",
            "plan_amount_cents",
            "billing_status",
            "payment_intent_id",
            "stripe_subscription_id",
            "stripe_customer_id",
        ]
    )

    invite.accepted_user = request.user
    invite.accepted_at = timezone.now()
    invite.save(update_fields=["accepted_user", "accepted_at"])

    messages.success(
        request,
        f"You've joined {invite.team_lead.get_full_name() or invite.team_lead.username}'s team.",
    )
    return redirect("accounts:profile")


def beta_request_approve(request, token):
    beta_request = get_object_or_404(BetaSignupRequest, token=token)
    approved_now = False
    already_approved = beta_request.status == BetaSignupRequest.STATUS_APPROVED
    invalid_link = beta_request.status == BetaSignupRequest.STATUS_REVOKED

    if beta_request.status == BetaSignupRequest.STATUS_PENDING:
        approver_identifier = ""
        if request.user.is_authenticated and request.user.email:
            approver_identifier = request.user.email
        else:
            approver_identifier = (
                request.GET.get("approver")
                or request.GET.get("by")
                or request.META.get("REMOTE_ADDR", "")
            )
        approved_now = beta_request.approve(approver_identifier)
        already_approved = False
        invalid_link = False

    context = {
        "beta_request": beta_request,
        "approved_now": approved_now,
        "already_approved": already_approved,
        "invalid_link": invalid_link,
    }
    return render(request, "accounts/beta_request_approval.html", context)


def confirm_email(request, token):
    verification = get_object_or_404(
        EmailVerification.objects.select_related("user", "user__profile"), token=token
    )
    user = verification.user
    plan = user.profile.plan
    requires_manual = bool(plan.get("requires_manual_approval"))
    newly_confirmed = verification.mark_confirmed()
    if newly_confirmed:
        if user.is_active:
            messages.success(request, "Email confirmed! You can log in now.")
        elif requires_manual:
            messages.success(
                request,
                "Email confirmed! We'll activate your account as soon as the team approves your beta request.",
            )
        else:
            messages.success(
                request, "Email confirmed! You can now log in to Lead CRM."
            )
    else:
        if verification.confirmed_at:
            if user.is_active:
                messages.info(request, "This email was already confirmed—you can log in.")
            elif requires_manual:
                messages.info(
                    request,
                    "Your email is confirmed. We're just waiting on the beta approval.",
                )
            else:
                messages.info(
                    request,
                    "Your email is already confirmed. Try logging in.",
                )
        else:
            messages.error(
                request,
                "We couldn't confirm this email address. Please request a new link.",
            )
    return redirect("accounts:login")


@require_POST
def signup_payment_intent(request):
    if not stripe or not getattr(settings, "STRIPE_SECRET_KEY", ""):
        return JsonResponse({"error": "Payments are not configured."}, status=400)

    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        payload = {}

    plan_id = payload.get("planId") or DEFAULT_PLAN_ID
    email = (payload.get("email") or "").strip()
    account_type = payload.get("accountType") or ACCOUNT_INDIVIDUAL

    plan = get_plan(plan_id)
    plan_account_type = plan.get("account_type")
    allowed_plans = set(PLAN_GROUPS.get(account_type, [])) | set(
        PLAN_GROUPS.get(plan_account_type, [])
    )
    if plan_id not in allowed_plans:
        return JsonResponse(
            {"error": "Plan is not available for the selected account type."},
            status=400,
        )

    price_cents = plan.get("price_cents", 0)

    if price_cents <= 0:
        return JsonResponse(
            {"error": "No payment required for the selected plan."}, status=400
        )

    price_id = plan.get("stripe_price_id")
    if not price_id:
        return JsonResponse(
            {
                "error": "Selected plan is not configured for Stripe billing yet. Please contact support."
            },
            status=400,
        )
    if isinstance(price_id, str) and price_id.startswith("prod_"):
        try:
            product = stripe.Product.retrieve(price_id, expand=["default_price"])
            default_price = product.get("default_price")
            if isinstance(default_price, dict):
                price_id = default_price.get("id")
            else:
                price_id = default_price
        except Exception as exc:
            return JsonResponse(
                {"error": f"Unable to resolve Stripe price for the selected plan: {exc}"},
                status=502,
            )
    if not price_id:
        return JsonResponse(
            {
                "error": "Plan is missing a Stripe price. Please contact support."
            },
            status=400,
        )

    currency = getattr(settings, "STRIPE_CURRENCY", "usd")

    metadata = {
        "plan_id": plan_id,
        "plan_label": plan.get("label", ""),
        "account_type": account_type,
    }
    if email:
        metadata["email"] = email

    try:
        customer = stripe.Customer.create(
            email=email or None,
            metadata={k: v for k, v in metadata.items() if v},
        )
    except Exception as exc:
        return JsonResponse(
            {"error": f"Unable to create Stripe customer: {exc}"}, status=502
        )

    try:
        subscription = stripe.Subscription.create(
            customer=customer["id"],
            items=[{"price": price_id}],
            metadata=metadata,
            payment_behavior="default_incomplete",
            collection_method="charge_automatically",
            payment_settings={
                "save_default_payment_method": "on_subscription",
                "payment_method_types": ["card"],
            },
            expand=["latest_invoice.payment_intent"],
        )
    except Exception as exc:
        return JsonResponse({"error": f"Unable to start payment: {exc}"}, status=502)

    invoice = subscription.get("latest_invoice") or {}
    payment_intent = invoice.get("payment_intent")
    if not payment_intent and invoice.get("id"):
        logger.warning(
            "Stripe subscription missing payment intent; creating manually",
            extra={
                "plan_id": plan_id,
                "account_type": account_type,
                "price_id": price_id,
                "subscription_id": subscription.get("id"),
                "invoice_id": invoice.get("id"),
                "subscription_status": subscription.get("status"),
                "invoice_status": invoice.get("status"),
            },
        )
        amount_due = int(invoice.get("amount_due") or 0)
        invoice_currency = (invoice.get("currency") or currency).lower()
        if amount_due <= 0:
            logger.info(
                "Invoice has no amount due; skipping payment intent creation",
                extra={"invoice_id": invoice.get("id")},
            )
        else:
            manual_intent = None
            try:
                manual_intent = stripe.PaymentIntent.create(
                    amount=amount_due,
                    currency=invoice_currency,
                    customer=customer["id"],
                    payment_method_types=["card"],
                    setup_future_usage="off_session",
                    metadata={
                        **metadata,
                        "invoice_id": invoice.get("id"),
                        "subscription_id": subscription.get("id"),
                    },
                )
                payment_intent = manual_intent
                logger.info(
                    "Created manual payment intent for invoice",
                    extra={
                        "invoice_id": invoice.get("id"),
                        "payment_intent_id": manual_intent.get("id")
                        if manual_intent
                        else None,
                    },
                )
            except Exception as exc:
                payment_intent = None
                logger.error(
                    "Failed to create manual payment intent",
                    exc_info=exc,
                    extra={
                        "invoice_id": invoice.get("id"),
                        "subscription_id": subscription.get("id"),
                    },
                )

    if not payment_intent:
        logger.error(
            "Stripe payment intent still unavailable after finalize",
            extra={
                "plan_id": plan_id,
                "account_type": account_type,
                "price_id": price_id,
                "subscription_id": subscription.get("id"),
                "invoice_id": invoice.get("id") if invoice else None,
                "invoice_status": invoice.get("status") if invoice else None,
                "subscription_raw": json.dumps(subscription, default=str),
                "invoice_raw": json.dumps(invoice, default=str),
            },
        )
        try:
            payment_intent = (
                stripe.PaymentIntent.retrieve(invoice.get("payment_intent"))
                if invoice.get("payment_intent")
                else None
            )
        except Exception:
            payment_intent = None

    if not payment_intent:
        logger.error(
            "Stripe payment intent still unavailable after retrieval: plan=%s account_type=%s price=%s subscription=%s invoice=%s invoice_status=%s subscription_raw=%s",
            plan_id,
            account_type,
            price_id,
            subscription.get("id"),
            invoice.get("id"),
            invoice.get("status"),
            json.dumps(subscription, default=str),
        )
        return JsonResponse(
            {
                "error": "Stripe did not return a payment intent for this subscription. Please try again."
            },
            status=502,
        )

    try:
        stripe.PaymentIntent.modify(payment_intent["id"], metadata=metadata)
    except Exception:
        # Metadata update is best-effort; continue if it fails.
        pass

    return JsonResponse(
        {
            "clientSecret": payment_intent.get("client_secret"),
            "amount": price_cents,
            "currency": currency,
            "planId": plan_id,
            "subscriptionId": subscription.get("id"),
            "customerId": customer.get("id"),
            "paymentIntentId": payment_intent.get("id"),
            "invoiceId": invoice.get("id"),
        }
    )
