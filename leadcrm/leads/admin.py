from django.contrib import admin
from django.db.models import Count, Q
from django.utils.html import format_html
from django.urls import reverse
from django.contrib.auth import get_user_model

from .models import Lead, SavedParcelList, LienRecord, LegalAction, LienSearchAttempt, SkipTraceRecord, AttomData

User = get_user_model()


@admin.register(Lead)
class LeadAdmin(admin.ModelAdmin):
    list_display = (
        "site_address",
        "site_city",
        "zoning",
        "owner_name",
        "status",
        "created_at",
    )
    search_fields = ("site_address", "owner_name", "loc_id", "site_city", "zoning")
    list_filter = ("status", "site_city", "zoning")
    ordering = ("-created_at",)


@admin.register(SavedParcelList)
class SavedParcelListAdmin(admin.ModelAdmin):
    list_display = ("name", "town_name", "created_at", "archived_at")
    search_fields = ("name", "town_name")
    ordering = ("-created_at",)
    list_filter = ("town_name", "archived_at")


@admin.register(LienRecord)
class LienRecordAdmin(admin.ModelAdmin):
    list_display = ("lien_holder", "lien_type", "amount", "status", "recording_date", "town_id", "loc_id", "created_at")
    search_fields = ("lien_holder", "loc_id", "instrument_number", "notes")
    list_filter = ("lien_type", "status", "town_id", "recording_date")
    ordering = ("-recording_date", "-created_at")
    readonly_fields = ("created_at", "updated_at")

    fieldsets = (
        ("Parcel Information", {
            "fields": ("town_id", "loc_id")
        }),
        ("Lien Details", {
            "fields": ("lien_type", "status", "lien_holder", "amount")
        }),
        ("Recording Information", {
            "fields": ("recording_date", "book_number", "page_number", "instrument_number")
        }),
        ("Release Information", {
            "fields": ("release_date", "release_book_number", "release_page_number", "release_instrument_number"),
            "classes": ("collapse",)
        }),
        ("Source & Documentation", {
            "fields": ("source", "source_url", "notes")
        }),
        ("Metadata", {
            "fields": ("created_by", "created_at", "updated_at"),
            "classes": ("collapse",)
        }),
    )


@admin.register(LegalAction)
class LegalActionAdmin(admin.ModelAdmin):
    list_display = ("case_number", "action_type", "status", "court", "plaintiff", "defendant", "filing_date", "town_id", "loc_id")
    search_fields = ("case_number", "plaintiff", "defendant", "loc_id", "description")
    list_filter = ("action_type", "status", "court", "filing_date")
    ordering = ("-filing_date", "-created_at")
    readonly_fields = ("created_at", "updated_at")

    fieldsets = (
        ("Parcel Information", {
            "fields": ("town_id", "loc_id")
        }),
        ("Case Details", {
            "fields": ("action_type", "status", "court", "case_number", "description")
        }),
        ("Parties", {
            "fields": ("plaintiff", "defendant")
        }),
        ("Important Dates", {
            "fields": ("filing_date", "hearing_date", "judgment_date", "closed_date")
        }),
        ("Financial Information", {
            "fields": ("amount_claimed", "judgment_amount"),
            "classes": ("collapse",)
        }),
        ("Source & Documentation", {
            "fields": ("source", "source_url", "pacer_case_id", "notes")
        }),
        ("Metadata", {
            "fields": ("created_by", "created_at", "updated_at"),
            "classes": ("collapse",)
        }),
    )



@admin.register(LienSearchAttempt)
class LienSearchAttemptAdmin(admin.ModelAdmin):
    list_display = ("town_id", "loc_id", "created_by", "searched_at", "found_liens", "found_legal_actions")
    list_filter = ("found_liens", "found_legal_actions", "searched_at")
    search_fields = ("town_id", "loc_id")
    ordering = ("-searched_at",)
    readonly_fields = ("searched_at",)


@admin.register(SkipTraceRecord)
class SkipTraceRecordAdmin(admin.ModelAdmin):
    list_display = ("loc_id", "town_id", "owner_name", "email", "phone_count", "created_by", "created_at")
    search_fields = ("loc_id", "owner_name", "email")
    list_filter = ("town_id", "created_at", "created_by")
    ordering = ("-created_at",)
    readonly_fields = ("created_at", "updated_at", "raw_payload")

    def phone_count(self, obj):
        """Display count of phone numbers found"""
        if obj.phones:
            return len(obj.phones) if isinstance(obj.phones, list) else 0
        return 0
    phone_count.short_description = "Phone #s"

    fieldsets = (
        ("Property Information", {
            "fields": ("town_id", "loc_id")
        }),
        ("Skip Trace Results", {
            "fields": ("owner_name", "email", "phones")
        }),
        ("Metadata", {
            "fields": ("created_by", "created_at", "updated_at", "raw_payload"),
            "classes": ("collapse",)
        }),
    )


@admin.register(AttomData)
class AttomDataAdmin(admin.ModelAdmin):
    list_display = ("loc_id", "town_id", "has_mortgage", "has_foreclosure", "tax_default", "last_updated")
    search_fields = ("loc_id", "mortgage_lender_name")
    list_filter = ("tax_default", "mortgage_default", "pre_foreclosure", "last_updated")
    ordering = ("-last_updated",)
    readonly_fields = ("created_at", "last_updated", "raw_response")

    def has_mortgage(self, obj):
        """Display if mortgage data exists"""
        if obj.mortgage_loan_amount and obj.mortgage_loan_amount > 0:
            return format_html('<span style="color: green;">✓ ${:,.0f}</span>', obj.mortgage_loan_amount)
        return format_html('<span style="color: gray;">—</span>')
    has_mortgage.short_description = "Mortgage"

    def has_foreclosure(self, obj):
        """Display foreclosure status"""
        if obj.pre_foreclosure or obj.foreclosure_stage:
            return format_html('<span style="color: red;">⚠ {}</span>', obj.foreclosure_stage or "Yes")
        return format_html('<span style="color: gray;">—</span>')
    has_foreclosure.short_description = "Foreclosure"

    fieldsets = (
        ("Property Information", {
            "fields": ("town_id", "loc_id", "saved_list")
        }),
        ("Foreclosure Information", {
            "fields": (
                "pre_foreclosure", "foreclosure_recording_date", "foreclosure_auction_date",
                "foreclosure_stage", "foreclosure_document_type", "foreclosure_estimated_value",
                "foreclosure_judgment_amount", "foreclosure_default_amount"
            ),
            "classes": ("collapse",)
        }),
        ("Mortgage Information", {
            "fields": (
                "mortgage_loan_amount", "mortgage_loan_type", "mortgage_lender_name",
                "mortgage_interest_rate", "mortgage_term_years", "mortgage_recording_date",
                "mortgage_due_date", "mortgage_loan_number"
            )
        }),
        ("Tax Information", {
            "fields": (
                "tax_default", "tax_assessment_year", "tax_assessed_value",
                "tax_amount_annual", "tax_delinquent_year"
            ),
            "classes": ("collapse",)
        }),
        ("Risk Scoring", {
            "fields": ("propensity_to_default_score", "propensity_to_default_decile"),
            "classes": ("collapse",)
        }),
        ("Metadata", {
            "fields": ("created_at", "last_updated", "raw_response"),
            "classes": ("collapse",)
        }),
    )

