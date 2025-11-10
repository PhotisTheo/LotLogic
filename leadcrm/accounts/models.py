import uuid
from typing import Optional

from django.conf import settings
from django.contrib.auth.models import User
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone


class UserProfile(models.Model):
    ACCOUNT_INDIVIDUAL = "individual"
    ACCOUNT_TEAM_LEAD = "team_lead"
    ACCOUNT_TEAM_MEMBER = "team_member"

    ACCOUNT_TYPE_CHOICES = [
        (ACCOUNT_INDIVIDUAL, "Individual"),
        (ACCOUNT_TEAM_LEAD, "Team Lead"),
        (ACCOUNT_TEAM_MEMBER, "Team Member"),
    ]

    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name="profile")
    account_type = models.CharField(
        max_length=20, choices=ACCOUNT_TYPE_CHOICES, default=ACCOUNT_INDIVIDUAL
    )
    team_lead = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="team_memberships",
        null=True,
        blank=True,
    )
    plan_id = models.CharField(max_length=50, default="individual_standard")
    plan_amount_cents = models.PositiveIntegerField(default=2500)
    billing_status = models.CharField(max_length=20, default="active")
    payment_intent_id = models.CharField(max_length=100, blank=True, null=True)
    stripe_customer_id = models.CharField(max_length=100, blank=True, null=True)
    stripe_subscription_id = models.CharField(max_length=100, blank=True, null=True)
    company_name = models.CharField(max_length=120, blank=True)
    job_title = models.CharField(max_length=120, blank=True)
    work_phone = models.CharField(max_length=32, blank=True)
    mobile_phone = models.CharField(max_length=32, blank=True)
    bio = models.TextField(blank=True)
    email_confirmed_at = models.DateTimeField(null=True, blank=True)

    # Notification preferences
    notify_qr_scan = models.BooleanField(default=True, help_text="Email when owner scans QR code")
    notify_call_request = models.BooleanField(default=True, help_text="Email when owner submits call request")
    notify_lead_activity = models.BooleanField(default=True, help_text="Email for lead status changes")
    notify_team_activity = models.BooleanField(default=True, help_text="Email for team collaboration")

    def __str__(self) -> str:
        label = dict(self.ACCOUNT_TYPE_CHOICES).get(self.account_type, self.account_type)
        return f"{self.user.username} ({label})"

    @property
    def plan(self) -> dict:
        from .plans import PLAN_CATALOG, DEFAULT_PLAN_ID

        return PLAN_CATALOG.get(self.plan_id or DEFAULT_PLAN_ID, PLAN_CATALOG[DEFAULT_PLAN_ID])

    @property
    def plan_name(self) -> str:
        return self.plan.get("label", "Individual")

    @property
    def plan_price_display(self) -> str:
        return self.plan.get("price_display", "$0")

    @property
    def team_member_limit(self) -> int:
        seat_limit = self.plan.get("seat_limit")
        if seat_limit is not None:
            return seat_limit
        return 1

    @property
    def workspace_owner(self) -> User:
        if self.account_type == self.ACCOUNT_TEAM_MEMBER and self.team_lead:
            return self.team_lead
        return self.user

    @property
    def remaining_invites(self) -> int:
        if self.account_type != self.ACCOUNT_TEAM_LEAD:
            return 0
        accepted = UserProfile.objects.filter(team_lead=self.user).count()
        pending = TeamInvite.objects.filter(
            team_lead=self.user, accepted_at__isnull=True
        ).count()
        remaining = self.team_member_limit - (accepted + pending)
        return max(0, remaining)


class TeamInvite(models.Model):
    team_lead = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="team_invites",
    )
    email = models.EmailField()
    token = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    accepted_at = models.DateTimeField(null=True, blank=True)
    accepted_user = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="accepted_team_invites",
    )

    class Meta:
        ordering = ["-created_at"]
        unique_together = [("team_lead", "email")]

    def __str__(self) -> str:
        return f"Invite from {self.team_lead.username} to {self.email}"

    @property
    def is_accepted(self) -> bool:
        return self.accepted_at is not None

    @property
    def remaining_slots(self) -> int:
        profile = getattr(self.team_lead, "profile", None)
        if not profile:
            return 0
        return profile.remaining_invites

    def can_accept(self) -> bool:
        if self.is_accepted:
            return False
        profile = getattr(self.team_lead, "profile", None)
        if not profile:
            return False
        # Superusers bypass seat limits
        if self.team_lead.is_superuser:
            return True
        accepted = UserProfile.objects.filter(team_lead=self.team_lead).count()
        return accepted < profile.team_member_limit


class BetaSignupRequest(models.Model):
    STATUS_PENDING = "pending"
    STATUS_APPROVED = "approved"
    STATUS_REVOKED = "revoked"
    STATUS_CHOICES = [
        (STATUS_PENDING, "Pending"),
        (STATUS_APPROVED, "Approved"),
        (STATUS_REVOKED, "Revoked"),
    ]

    user = models.OneToOneField(
        User,
        on_delete=models.CASCADE,
        related_name="beta_signup_request",
    )
    plan_id = models.CharField(max_length=50, default="beta_tester")
    token = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    status = models.CharField(
        max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDING
    )
    created_at = models.DateTimeField(auto_now_add=True)
    approved_at = models.DateTimeField(null=True, blank=True)
    approved_by = models.CharField(max_length=120, blank=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"Beta request for {self.user.username} ({self.plan_id})"

    @property
    def plan(self) -> dict:
        from .plans import get_plan

        return get_plan(self.plan_id)

    @property
    def plan_label(self) -> str:
        plan = self.plan
        return plan.get("label", self.plan_id)

    def approve(self, approver: str | None = None) -> bool:
        if self.status == self.STATUS_APPROVED:
            return False
        self.status = self.STATUS_APPROVED
        self.approved_at = timezone.now()
        self.approved_by = approver or ""
        self.save(
            update_fields=[
                "status",
                "approved_at",
                "approved_by",
            ]
        )
        user = self.user
        attempt_activate_user(user)
        return True

    def reset(self):
        self.status = self.STATUS_PENDING
        self.approved_at = None
        self.approved_by = ""
        self.token = uuid.uuid4()
        self.save(
            update_fields=[
                "status",
                "approved_at",
                "approved_by",
                "token",
            ]
        )

class EmailVerification(models.Model):
    user = models.OneToOneField(
        User,
        on_delete=models.CASCADE,
        related_name="email_verification",
    )
    email = models.EmailField()
    token = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    confirmed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"Email verification for {self.user.username}"

    def regenerate(self):
        self.token = uuid.uuid4()
        self.email = self.user.email
        self.confirmed_at = None
        self.save(update_fields=["token", "email", "confirmed_at"])

    def mark_confirmed(self) -> bool:
        if self.confirmed_at:
            return False
        now = timezone.now()
        self.confirmed_at = now
        self.save(update_fields=["confirmed_at"])
        profile = self.user.profile
        if not profile.email_confirmed_at:
            profile.email_confirmed_at = now
            profile.save(update_fields=["email_confirmed_at"])
        attempt_activate_user(self.user)
        return True


def attempt_activate_user(user: User) -> bool:
    profile = getattr(user, "profile", None)
    if not profile or not profile.email_confirmed_at:
        return False

    from .plans import get_plan

    plan = get_plan(profile.plan_id)
    requires_manual = bool(plan.get("requires_manual_approval"))
    if requires_manual:
        beta_request = getattr(user, "beta_signup_request", None)
        if not beta_request or beta_request.status != BetaSignupRequest.STATUS_APPROVED:
            return False
        if profile.billing_status == "beta_pending":
            profile.billing_status = "included"
            profile.save(update_fields=["billing_status"])

    if not user.is_active:
        user.is_active = True
        user.save(update_fields=["is_active"])
    return True


def get_workspace_owner(user: Optional[User]) -> Optional[User]:
    if not user or not isinstance(user, User):
        return None
    profile = getattr(user, "profile", None)
    if not profile:
        return user
    return profile.workspace_owner


@receiver(post_save, sender=User)
def ensure_user_profile(sender, instance: User, created: bool, **kwargs):
    if created:
        UserProfile.objects.create(user=instance)
    else:
        UserProfile.objects.get_or_create(user=instance)
