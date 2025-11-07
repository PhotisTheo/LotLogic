from django.conf import settings


def stripe_settings(request):
    """
    Expose Stripe publishable key so templates can initialise Stripe.js safely.
    """
    return {
        "stripe_publishable_key": getattr(settings, "STRIPE_PUBLISHABLE_KEY", "") or "",
    }

