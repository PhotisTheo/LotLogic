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
