from django.contrib import admin
from django.db.models import Count
from django.utils.html import format_html
from django.contrib.auth import get_user_model

from .models import TeamInvite, UserProfile

User = get_user_model()


@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    list_display = ("user", "account_type", "plan_name", "billing_status", "team_lead", "leads_count", "lists_count", "skip_traces_count")
    search_fields = ("user__username", "user__email")
    list_filter = ("account_type", "billing_status", "plan_id")
    readonly_fields = ("stripe_customer_id", "stripe_subscription_id", "payment_intent_id")

    def leads_count(self, obj):
        """Count of leads created by this user"""
        from leads.models import Lead
        count = Lead.objects.filter(created_by=obj.user).count()
        return format_html('<span style="font-weight: bold;">{}</span>', count)
    leads_count.short_description = "Leads"
    leads_count.admin_order_field = "leads_count"

    def lists_count(self, obj):
        """Count of saved lists created by this user"""
        from leads.models import SavedParcelList
        count = SavedParcelList.objects.filter(created_by=obj.user).count()
        return format_html('<span style="font-weight: bold;">{}</span>', count)
    lists_count.short_description = "Lists"
    lists_count.admin_order_field = "lists_count"

    def skip_traces_count(self, obj):
        """Count of skip traces performed by this user"""
        from leads.models import SkipTraceRecord
        count = SkipTraceRecord.objects.filter(created_by=obj.user).count()
        return format_html('<span style="font-weight: bold; color: green;">{}</span>', count)
    skip_traces_count.short_description = "Skip Traces"
    skip_traces_count.admin_order_field = "skip_traces_count"

    def get_queryset(self, request):
        """Add annotations for sorting by counts"""
        qs = super().get_queryset(request)
        from leads.models import Lead, SavedParcelList, SkipTraceRecord
        qs = qs.annotate(
            leads_count=Count('user__lead', distinct=True),
            lists_count=Count('user__savedparcellist', distinct=True),
            skip_traces_count=Count('user__skiptrace_records', distinct=True)
        )
        return qs

    fieldsets = (
        ("User Information", {
            "fields": ("user", "account_type", "team_lead")
        }),
        ("Plan & Billing", {
            "fields": ("plan_id", "plan_amount_cents", "billing_status")
        }),
        ("Stripe Integration", {
            "fields": ("stripe_customer_id", "stripe_subscription_id", "payment_intent_id"),
            "classes": ("collapse",)
        }),
    )


@admin.register(TeamInvite)
class TeamInviteAdmin(admin.ModelAdmin):
    list_display = ("email", "team_lead", "created_at", "accepted_at")
    search_fields = ("email", "team_lead__username")
    list_filter = ("accepted_at",)
