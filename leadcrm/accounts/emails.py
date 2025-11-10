from __future__ import annotations

from django.conf import settings
from django.core.mail import send_mail
from django.template.loader import render_to_string
from django.utils.html import strip_tags


def send_beta_signup_notification(beta_request, approval_url: str) -> None:
    """Send an email to the ops team when a new beta request comes in."""
    recipients = getattr(settings, "BETA_REQUEST_NOTIFICATION_EMAILS", [])
    if not recipients:
        return

    user = beta_request.user
    context = {
        "beta_request": beta_request,
        "approval_url": approval_url,
        "user_display": user.get_full_name() or user.username,
        "user_email": user.email,
        "plan_label": beta_request.plan_label,
    }
    subject = f"New beta signup request: {context['user_display']}"
    text_body = render_to_string(
        "accounts/emails/beta_request_notification.txt", context
    )
    html_body = render_to_string(
        "accounts/emails/beta_request_notification.html", context
    )
    send_mail(
        subject,
        strip_tags(text_body),
        settings.DEFAULT_FROM_EMAIL,
        recipients,
        html_message=html_body,
    )


def send_email_verification(verification, confirmation_url: str) -> None:
    """Send email confirmation link to the end user."""
    user = verification.user
    context = {
        "user_display": user.get_full_name() or user.username,
        "confirmation_url": confirmation_url,
    }
    subject = "Confirm your Lead CRM email"
    text_body = render_to_string(
        "accounts/emails/email_verification.txt", context
    )
    html_body = render_to_string(
        "accounts/emails/email_verification.html", context
    )
    send_mail(
        subject,
        strip_tags(text_body),
        settings.DEFAULT_FROM_EMAIL,
        [verification.email or user.email],
        html_message=html_body,
    )


def send_qr_scan_notification(user, parcel, scan_time, crm_url: str) -> None:
    """Send notification when an owner scans a QR code from a mailer."""
    # Check if user has this notification enabled
    profile = getattr(user, "profile", None)
    if not profile or not profile.notify_qr_scan:
        return

    if not user.email:
        return

    context = {
        "user": user,
        "parcel": parcel,
        "scan_time": scan_time,
        "crm_url": crm_url,
        "settings_url": f"{settings.DEFAULT_FROM_EMAIL.split('@')[1]}/accounts/settings/",
    }

    subject = f"🔔 Owner viewed your mailer - {parcel.street_name}"
    html_body = render_to_string(
        "accounts/emails/qr_scan_notification.html", context
    )

    send_mail(
        subject,
        strip_tags(html_body),
        settings.DEFAULT_FROM_EMAIL,
        [user.email],
        html_message=html_body,
    )


def send_call_request_notification(user, lead, lead_url: str) -> None:
    """Send notification when an owner submits a call request."""
    # Check if user has this notification enabled
    profile = getattr(user, "profile", None)
    if not profile or not profile.notify_call_request:
        return

    if not user.email:
        return

    context = {
        "user": user,
        "lead": lead,
        "lead_url": lead_url,
        "settings_url": f"{settings.DEFAULT_FROM_EMAIL.split('@')[1]}/accounts/settings/",
    }

    subject = f"📞 New call request from {lead.owner_name}"
    html_body = render_to_string(
        "accounts/emails/call_request_notification.html", context
    )

    send_mail(
        subject,
        strip_tags(html_body),
        settings.DEFAULT_FROM_EMAIL,
        [user.email],
        html_message=html_body,
    )
