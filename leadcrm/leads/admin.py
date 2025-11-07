from django.contrib import admin

from .models import Lead, SavedParcelList, LienRecord, LegalAction, LienSearchAttempt


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

