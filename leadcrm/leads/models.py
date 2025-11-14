from django.conf import settings
from django.db import models


class Lead(models.Model):
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="leads",
        null=True,
        blank=True,
    )
    # Basic Location Info
    loc_id = models.CharField(max_length=100, blank=True, null=True)
    site_address = models.CharField(max_length=200, blank=True, null=True)
    site_city = models.CharField(max_length=100, blank=True, null=True)
    site_zip = models.CharField(max_length=20, blank=True, null=True)
    zoning = models.CharField(max_length=50, blank=True, null=True)
    use_description = models.CharField(max_length=255, blank=True, null=True)

    # Physical Attributes
    style = models.CharField(max_length=100, blank=True, null=True)
    stories = models.CharField(max_length=10, blank=True, null=True)
    year_built = models.IntegerField(blank=True, null=True)
    lot_size = models.FloatField(blank=True, null=True)
    lot_units = models.CharField(max_length=20, blank=True, null=True)
    bld_area = models.IntegerField(blank=True, null=True)
    units = models.IntegerField(blank=True, null=True)

    # Valuation
    building_value = models.IntegerField(blank=True, null=True)
    land_value = models.IntegerField(blank=True, null=True)
    total_value = models.IntegerField(blank=True, null=True)

    # Sale Info
    sale_date = models.CharField(max_length=20, blank=True, null=True)
    sale_price = models.IntegerField(blank=True, null=True)
    sale_book = models.CharField(max_length=20, blank=True, null=True)
    sale_page = models.CharField(max_length=20, blank=True, null=True)

    # Owner Info
    owner_name = models.CharField(max_length=100, blank=True, null=True)
    owner_name_2 = models.CharField(
        max_length=100, blank=True, null=True
    )  # Optional second owner

    owner_street = models.CharField(max_length=200, blank=True, null=True)
    owner_city = models.CharField(max_length=100, blank=True, null=True)
    owner_state = models.CharField(max_length=10, blank=True, null=True)
    owner_zip = models.CharField(max_length=20, blank=True, null=True)
    mailing_owner = models.CharField(
        max_length=100, blank=True, null=True
    )  # <-- This holds the value from 'Owner Name'

    # Contact Info
    phone_1 = models.CharField(max_length=30, blank=True, null=True)
    phone_2 = models.CharField(max_length=30, blank=True, null=True)
    phone_3 = models.CharField(max_length=30, blank=True, null=True)
    dnc_1 = models.CharField(max_length=10, blank=True, null=True)
    dnc_2 = models.CharField(max_length=10, blank=True, null=True)
    dnc_3 = models.CharField(max_length=10, blank=True, null=True)
    email = models.EmailField(blank=True, null=True)
    bedrooms = models.DecimalField(
        max_digits=5, decimal_places=2, blank=True, null=True
    )
    bathrooms = models.DecimalField(
        max_digits=5, decimal_places=2, blank=True, null=True
    )

    # CRM Functionality
    status = models.CharField(
        max_length=20,
        choices=[("Cold", "Cold"), ("Warm", "Warm"), ("Qualified", "Qualified")],
        default="Cold",
    )
    notes = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.site_address or 'Lead'} ({self.owner_name or 'Unknown Owner'})"

class SavedParcelList(models.Model):
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="saved_parcel_lists",
        null=True,
        blank=True,
    )
    name = models.CharField(max_length=200)
    town_id = models.IntegerField()
    town_name = models.CharField(max_length=100)
    criteria = models.JSONField()
    loc_ids = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
    archived_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.name} ({self.town_name})"

    @property
    def is_archived(self) -> bool:
        return bool(self.archived_at)


class SkipTraceRecord(models.Model):
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="skiptrace_records",
        null=True,
        blank=True,
    )
    town_id = models.IntegerField(null=True, blank=True)
    loc_id = models.CharField(max_length=200)
    owner_name = models.CharField(max_length=255, blank=True)
    email = models.CharField(max_length=255, blank=True)
    phones = models.JSONField(default=list, blank=True)
    raw_payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("created_by", "town_id", "loc_id")
        indexes = [
            models.Index(fields=["created_by", "town_id", "loc_id"]),
        ]

    def __str__(self):
        return f"SkipTrace {self.town_id or 'N/A'} - {self.loc_id}"


class ParcelMarketValue(models.Model):
    """Hybrid hedonic/comparable market value snapshot for a parcel."""

    METHODOLOGY_HYBRID_V1 = "hybrid_v1"
    METHODOLOGY_CHOICES = [
        (METHODOLOGY_HYBRID_V1, "Hybrid Hedonic + Comps v1"),
    ]

    town_id = models.IntegerField(db_index=True)
    loc_id = models.CharField(max_length=200)
    market_value = models.DecimalField(
        max_digits=16,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Final blended market value (USD).",
    )
    market_value_per_sqft = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Blended price per interior square foot.",
    )
    comparable_value = models.DecimalField(
        max_digits=16,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Weighted comparable sale estimate used in blend.",
    )
    comparable_count = models.IntegerField(default=0)
    comparable_avg_psf = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
    )
    hedonic_value = models.DecimalField(
        max_digits=16,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Regression output before blending.",
    )
    hedonic_r2 = models.FloatField(null=True, blank=True)
    valuation_confidence = models.FloatField(
        null=True,
        blank=True,
        help_text="0-1 score derived from data coverage.",
    )
    methodology = models.CharField(
        max_length=50,
        choices=METHODOLOGY_CHOICES,
        default=METHODOLOGY_HYBRID_V1,
    )
    model_version = models.CharField(max_length=50, default="hybrid-v1.0")
    valued_at = models.DateTimeField()
    payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("town_id", "loc_id")
        indexes = [
            models.Index(fields=["town_id", "loc_id"]),
            models.Index(fields=["valued_at"]),
        ]
        ordering = ["-valued_at", "town_id", "loc_id"]

    def __str__(self) -> str:
        return f"{self.town_id}/{self.loc_id} – {self.market_value or 'n/a'}"


class MassGISParcel(models.Model):
    """
    Precomputed parcel data for all Massachusetts properties.
    Refreshed weekly to enable instant statewide search without file I/O.
    """
    town_id = models.IntegerField(db_index=True)
    loc_id = models.CharField(max_length=200)

    # Address & Location
    site_address = models.CharField(max_length=500, db_index=True, null=True, blank=True)
    site_city = models.CharField(max_length=100, null=True, blank=True)
    site_zip = models.CharField(max_length=20, db_index=True, null=True, blank=True)

    # Owner Information
    owner_name = models.CharField(max_length=500, db_index=True, null=True, blank=True)
    owner_address = models.TextField(null=True, blank=True)
    owner_city = models.CharField(max_length=100, null=True, blank=True)
    owner_state = models.CharField(max_length=2, null=True, blank=True)
    owner_zip = models.CharField(max_length=20, null=True, blank=True)
    absentee = models.BooleanField(default=False, db_index=True)

    # Property Classification
    use_code = models.CharField(max_length=20, null=True, blank=True)
    property_type = models.CharField(max_length=200, db_index=True, null=True, blank=True)
    property_category = models.CharField(max_length=50, db_index=True, null=True, blank=True)
    style = models.CharField(max_length=100, null=True, blank=True)
    zoning = models.CharField(max_length=50, null=True, blank=True)

    # Financial/Assessment (BigInteger for values > 2B)
    total_value = models.BigIntegerField(db_index=True, null=True, blank=True)
    land_value = models.BigIntegerField(null=True, blank=True)
    building_value = models.BigIntegerField(null=True, blank=True)

    # Physical Attributes
    lot_size = models.FloatField(null=True, blank=True)
    lot_units = models.CharField(max_length=20, null=True, blank=True)
    living_area = models.IntegerField(null=True, blank=True)
    units = models.IntegerField(null=True, blank=True)
    bedrooms = models.IntegerField(null=True, blank=True)
    bathrooms = models.FloatField(null=True, blank=True)
    year_built = models.IntegerField(db_index=True, null=True, blank=True)

    # Sale Information
    last_sale_date = models.DateField(db_index=True, null=True, blank=True)
    last_sale_price = models.BigIntegerField(null=True, blank=True)

    # Computed Fields
    equity_percent = models.FloatField(db_index=True, null=True, blank=True)
    years_owned = models.FloatField(db_index=True, null=True, blank=True)

    # Geometry (for map overlays)
    centroid_lon = models.FloatField(db_index=True, null=True, blank=True)
    centroid_lat = models.FloatField(db_index=True, null=True, blank=True)
    geometry = models.JSONField(null=True, blank=True, help_text="GeoJSON polygon for parcel boundary")

    # Metadata
    fiscal_year = models.CharField(max_length=10, null=True, blank=True)
    data_source = models.CharField(max_length=50, default="massgis", null=True, blank=True)
    last_updated = models.DateTimeField(auto_now=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("town_id", "loc_id")
        indexes = [
            models.Index(fields=["town_id", "loc_id"]),
            models.Index(fields=["property_category", "total_value"]),
            models.Index(fields=["absentee", "total_value"]),
            models.Index(fields=["centroid_lon", "centroid_lat"]),
            models.Index(fields=["last_sale_date"]),
        ]
        ordering = ["town_id", "loc_id"]

    def __str__(self):
        return f"{self.town_id}/{self.loc_id} - {self.site_address or 'No Address'}"


class MassGISParcelCache(models.Model):
    """
    Cross-user cache for MassGIS parcel data.
    Reduces load time by storing parsed parcel data for 90 days.
    """
    town_id = models.IntegerField(db_index=True)
    loc_id = models.CharField(max_length=200, db_index=True)

    # Parcel data (stored as JSON for flexibility)
    parcel_data = models.JSONField()

    # Timestamps for cache management
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_accessed = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("town_id", "loc_id")
        indexes = [
            models.Index(fields=["town_id", "loc_id"]),
            models.Index(fields=["last_accessed"]),  # For cleanup queries
        ]
        verbose_name = "MassGIS Parcel Cache"
        verbose_name_plural = "MassGIS Parcel Cache"

    def __str__(self):
        return f"Cache: {self.town_id}/{self.loc_id}"

    @property
    def is_expired(self) -> bool:
        """Check if cache entry is older than 90 days."""
        from django.utils import timezone
        from datetime import timedelta
        expiry_date = timezone.now() - timedelta(days=90)
        return self.last_accessed < expiry_date


class ScheduleCallRequest(models.Model):
    STAGE_NEW = "new"
    STAGE_CONTACTED = "contacted"
    STAGE_APPOINTMENT = "appointment"
    STAGE_LISTED = "listed"
    STAGE_UNDER_CONTRACT = "under_contract"
    STAGE_CLOSED = "closed"

    STAGE_CHOICES = [
        (STAGE_NEW, "New Lead"),
        (STAGE_CONTACTED, "Contacted"),
        (STAGE_APPOINTMENT, "Listing Appointment"),
        (STAGE_LISTED, "Listed"),
        (STAGE_UNDER_CONTRACT, "Under Contract"),
        (STAGE_CLOSED, "Closed/Sold"),
    ]

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="schedule_call_requests",
        null=True,
        blank=True,
    )
    town_id = models.IntegerField(null=True, blank=True)
    loc_id = models.CharField(max_length=200, blank=True)
    property_address = models.CharField(max_length=255, blank=True)
    property_city = models.CharField(max_length=100, blank=True)
    recipient_name = models.CharField(max_length=255, blank=True)
    contact_phone = models.CharField(max_length=50)
    preferred_call_time = models.DateTimeField(blank=True, null=True)
    notes = models.TextField(blank=True)

    # CRM fields
    stage = models.CharField(max_length=20, choices=STAGE_CHOICES, default=STAGE_NEW)
    is_archived = models.BooleanField(default=False)
    archived_at = models.DateTimeField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["town_id", "loc_id"]),
            models.Index(fields=["stage", "is_archived"]),
        ]

    def __str__(self):
        base_label = self.property_address or self.loc_id or "Schedule Call Request"
        return f"{base_label} ({self.contact_phone})"


class GeneratedMailer(models.Model):
    town_id = models.IntegerField()
    loc_id = models.CharField(max_length=200)
    prompt_id = models.CharField(max_length=100, blank=True)
    payload = models.JSONField(default=dict, blank=True)
    html = models.TextField(blank=True)
    ai_generated = models.BooleanField(default=False)
    ai_model = models.CharField(max_length=100, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("town_id", "loc_id")
        ordering = ["-updated_at"]
        indexes = [
            models.Index(fields=["town_id", "loc_id"]),
        ]

    def __str__(self):
        return f"Mailer {self.town_id}-{self.loc_id}"


class MailerTemplate(models.Model):
    SECTOR_RESIDENTIAL = "residential"
    SECTOR_COMMERCIAL = "commercial"
    SECTOR_CHOICES = [
        (SECTOR_RESIDENTIAL, "Residential"),
        (SECTOR_COMMERCIAL, "Commercial"),
    ]

    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="mailer_templates",
    )
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="created_mailer_templates",
    )
    name = models.CharField(max_length=120)
    summary = models.CharField(max_length=255, blank=True)
    sector = models.CharField(
        max_length=20, choices=SECTOR_CHOICES, default=SECTOR_RESIDENTIAL
    )
    letter_body = models.TextField(
        help_text=(
            "Use placeholders like {salutation_name}, {property_address}, or {agent_name} to personalize the letter."
        )
    )
    value_props_title = models.CharField(max_length=120, blank=True)
    value_props = models.JSONField(default=list, blank=True)
    prompt_text = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at", "-created_at"]
        indexes = [
            models.Index(fields=["owner", "is_active"]),
        ]

    def __str__(self) -> str:
        return f"{self.name} ({self.owner.username})"


class AttomData(models.Model):
    saved_list = models.ForeignKey(
        "SavedParcelList",
        on_delete=models.CASCADE,
        related_name="attom_data",
        null=True,
        blank=True,
    )
    town_id = models.IntegerField(null=True, blank=True)
    loc_id = models.CharField(max_length=200, null=True, blank=True)

    # Legacy boolean flags (kept for backward compatibility)
    pre_foreclosure = models.BooleanField(default=False)
    mortgage_default = models.BooleanField(default=False)
    tax_default = models.BooleanField(default=False)

    # Detailed foreclosure information
    foreclosure_recording_date = models.CharField(max_length=50, blank=True, null=True)
    foreclosure_auction_date = models.CharField(max_length=50, blank=True, null=True)
    foreclosure_estimated_value = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    foreclosure_judgment_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    foreclosure_default_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    foreclosure_stage = models.CharField(max_length=100, blank=True, null=True)  # e.g., "Notice of Default", "Lis Pendens", "Auction"
    foreclosure_document_type = models.CharField(max_length=100, blank=True, null=True)

    # Mortgage information
    mortgage_loan_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    mortgage_loan_type = models.CharField(max_length=100, blank=True, null=True)
    mortgage_lender_name = models.CharField(max_length=255, blank=True, null=True)
    mortgage_interest_rate = models.DecimalField(max_digits=6, decimal_places=3, blank=True, null=True)
    mortgage_term_years = models.IntegerField(blank=True, null=True)
    mortgage_recording_date = models.CharField(max_length=50, blank=True, null=True)
    mortgage_due_date = models.CharField(max_length=50, blank=True, null=True)
    mortgage_loan_number = models.CharField(max_length=100, blank=True, null=True)

    # Tax information
    tax_assessment_year = models.IntegerField(blank=True, null=True)
    tax_assessed_value = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    tax_amount_annual = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    tax_delinquent_year = models.IntegerField(blank=True, null=True)

    # Propensity to default score (0-100, higher = more likely to default)
    propensity_to_default_score = models.IntegerField(blank=True, null=True)
    propensity_to_default_decile = models.IntegerField(blank=True, null=True)  # 1-10, where 10 is highest risk

    # Store the full raw JSON response for reference
    raw_response = models.JSONField(default=dict, blank=True)

    # Timestamp fields for caching
    last_updated = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)

    class Meta:
        # Create index for efficient cache lookups across all users
        indexes = [
            models.Index(fields=["town_id", "loc_id"]),
            models.Index(fields=["last_updated"]),
        ]

    def __str__(self):
        return f"ATTOM Data for {self.town_id}-{self.loc_id}"

    @property
    def is_cache_fresh(self, max_age_days=60):
        """Check if the cached data is fresh (less than max_age_days old)."""
        from django.utils import timezone
        from datetime import timedelta

        if not self.last_updated:
            return False

        age = timezone.now() - self.last_updated
        return age < timedelta(days=max_age_days)


class LienRecord(models.Model):
    """
    Tracks liens and releases recorded against parcels/owners.
    Sources: Registry of Deeds, Municipal tax records, UCC filings, etc.
    """
    LIEN_TYPE_CHOICES = [
        ('mortgage', 'Mortgage'),
        ('tax_municipal', 'Municipal Tax Lien'),
        ('tax_federal', 'Federal Tax Lien'),
        ('tax_state', 'State Tax Lien'),
        ('mechanics', 'Mechanics Lien'),
        ('irs', 'IRS Lien'),
        ('ucc', 'UCC Filing (Personal Property)'),
        ('judgment', 'Judgment Lien'),
        ('hoa', 'HOA Lien'),
        ('other', 'Other'),
    ]

    STATUS_CHOICES = [
        ('active', 'Active'),
        ('released', 'Released'),
        ('discharged', 'Discharged'),
        ('satisfied', 'Satisfied'),
        ('partial', 'Partially Released'),
    ]

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="lien_records",
    )
    town_id = models.IntegerField()
    loc_id = models.CharField(max_length=200)

    # Lien Details
    lien_type = models.CharField(max_length=50, choices=LIEN_TYPE_CHOICES)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='active')
    lien_holder = models.CharField(max_length=255, help_text="Name of lien holder/creditor")

    # Financial Information
    amount = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Lien amount in dollars"
    )

    # Recording Information
    recording_date = models.DateField(null=True, blank=True)
    book_number = models.CharField(max_length=50, blank=True, help_text="Registry book number")
    page_number = models.CharField(max_length=50, blank=True, help_text="Registry page number")
    instrument_number = models.CharField(max_length=100, blank=True, help_text="Document/instrument number")

    # Release Information
    release_date = models.DateField(null=True, blank=True)
    release_book_number = models.CharField(max_length=50, blank=True)
    release_page_number = models.CharField(max_length=50, blank=True)
    release_instrument_number = models.CharField(max_length=100, blank=True)

    # Source Information
    source = models.CharField(
        max_length=255,
        blank=True,
        help_text="Where this lien was found (e.g., 'Essex County Registry', 'Town Treasurer PDF')"
    )
    source_url = models.URLField(blank=True, help_text="Link to source document/page")

    # Additional Details
    notes = models.TextField(blank=True)
    attachments = models.JSONField(
        default=list,
        blank=True,
        help_text="List of file paths or URLs to supporting documents"
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-recording_date', '-created_at']
        indexes = [
            models.Index(fields=['town_id', 'loc_id']),
            models.Index(fields=['status']),
            models.Index(fields=['lien_type']),
            models.Index(fields=['recording_date']),
        ]

    def __str__(self):
        return f"{self.get_lien_type_display()} - {self.lien_holder} (${self.amount or 'N/A'})"


class LegalAction(models.Model):
    """
    Tracks legal actions and court cases against property owners.
    Sources: MA Trial Court, PACER, CourtListener, etc.
    """
    ACTION_TYPE_CHOICES = [
        ('foreclosure', 'Foreclosure'),
        ('eviction', 'Eviction/Summary Process'),
        ('civil_judgment', 'Civil Judgment'),
        ('land_court', 'Land Court Case'),
        ('bankruptcy_ch7', 'Bankruptcy Chapter 7'),
        ('bankruptcy_ch11', 'Bankruptcy Chapter 11'),
        ('bankruptcy_ch13', 'Bankruptcy Chapter 13'),
        ('federal_civil', 'Federal Civil Case'),
        ('federal_criminal', 'Federal Criminal Case'),
        ('state_civil', 'State Civil Case'),
        ('state_criminal', 'State Criminal Case'),
        ('probate', 'Probate/Estate Matter'),
        ('other', 'Other'),
    ]

    STATUS_CHOICES = [
        ('filed', 'Filed'),
        ('pending', 'Pending'),
        ('active', 'Active'),
        ('dismissed', 'Dismissed'),
        ('settled', 'Settled'),
        ('judgment', 'Judgment Entered'),
        ('closed', 'Closed'),
        ('appealed', 'Under Appeal'),
    ]

    COURT_CHOICES = [
        # Massachusetts State Courts
        ('ma_housing', 'MA Housing Court'),
        ('ma_district', 'MA District Court'),
        ('ma_superior', 'MA Superior Court'),
        ('ma_land', 'MA Land Court'),
        ('ma_probate', 'MA Probate & Family Court'),
        ('ma_appeals', 'MA Appeals Court'),
        ('ma_sjc', 'MA Supreme Judicial Court'),
        # Federal Courts
        ('federal_district_ma', 'US District Court (MA)'),
        ('federal_bankruptcy_ma', 'US Bankruptcy Court (MA)'),
        ('federal_1st_circuit', 'US Court of Appeals (1st Circuit)'),
        ('federal_supreme', 'US Supreme Court'),
        # Other
        ('other', 'Other Court'),
    ]

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="legal_actions",
    )
    town_id = models.IntegerField(null=True, blank=True)
    loc_id = models.CharField(max_length=200, blank=True)

    # Case Details
    action_type = models.CharField(max_length=50, choices=ACTION_TYPE_CHOICES)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='filed')

    # Court Information
    court = models.CharField(max_length=50, choices=COURT_CHOICES)
    case_number = models.CharField(max_length=100, help_text="Court case/docket number")

    # Parties
    plaintiff = models.CharField(max_length=255, blank=True, help_text="Plaintiff/petitioner name")
    defendant = models.CharField(max_length=255, blank=True, help_text="Defendant/respondent name")

    # Dates
    filing_date = models.DateField(null=True, blank=True)
    hearing_date = models.DateField(null=True, blank=True)
    judgment_date = models.DateField(null=True, blank=True)
    closed_date = models.DateField(null=True, blank=True)

    # Financial Information
    amount_claimed = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Amount claimed/sought"
    )
    judgment_amount = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Judgment amount awarded"
    )

    # Source Information
    source = models.CharField(
        max_length=255,
        blank=True,
        help_text="Where this case was found (e.g., 'PACER', 'MA Trial Court eAccess', 'CourtListener')"
    )
    source_url = models.URLField(blank=True, help_text="Link to case docket/details")
    pacer_case_id = models.CharField(max_length=100, blank=True, help_text="PACER case ID if applicable")

    # Additional Details
    description = models.TextField(blank=True, help_text="Brief description of the case")
    notes = models.TextField(blank=True)
    attachments = models.JSONField(
        default=list,
        blank=True,
        help_text="List of file paths or URLs to court documents"
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-filing_date', '-created_at']
        indexes = [
            models.Index(fields=['town_id', 'loc_id']),
            models.Index(fields=['status']),
            models.Index(fields=['action_type']),
            models.Index(fields=['case_number']),
            models.Index(fields=['filing_date']),
        ]

    def __str__(self):
        return f"{self.get_action_type_display()} - {self.case_number} ({self.get_status_display()})"


class LienSearchAttempt(models.Model):
    """
    Track when we've searched CourtListener for a parcel to avoid duplicate API calls.

    This model records that we've attempted a search for a parcel, even if no liens/actions
    were found. This prevents re-searching the same parcel repeatedly.

    Cache duration: 90 days (1 quarter)
    """
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='lien_search_attempts'
    )
    town_id = models.IntegerField(db_index=True)
    loc_id = models.CharField(max_length=200, db_index=True)

    # When the search was performed
    searched_at = models.DateTimeField(auto_now_add=True, db_index=True)

    # Whether the search found any results
    found_liens = models.BooleanField(default=False)
    found_legal_actions = models.BooleanField(default=False)

    class Meta:
        db_table = 'leads_lien_search_attempt'
        indexes = [
            models.Index(fields=['created_by', 'town_id', 'loc_id']),
            models.Index(fields=['searched_at']),
        ]
        unique_together = [['created_by', 'town_id', 'loc_id']]

    def __str__(self):
        return f"Search for {self.town_id}/{self.loc_id} at {self.searched_at}"


class CorporateEntity(models.Model):
    """
    Stores corporate entity data scraped from MA Secretary of Commonwealth.
    Used to resolve LLC owners when GIS parcels show corporate ownership.

    Sources: MA Secretary of Commonwealth Corporate Database
    Cache: 180 days (LLC officers change infrequently)
    """
    ENTITY_TYPE_CHOICES = [
        ('llc', 'Limited Liability Company'),
        ('corp', 'Corporation'),
        ('lp', 'Limited Partnership'),
        ('gp', 'General Partnership'),
        ('trust', 'Business Trust'),
        ('other', 'Other'),
    ]

    STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('dissolved', 'Dissolved'),
        ('suspended', 'Suspended'),
        ('cancelled', 'Cancelled'),
        ('merged', 'Merged'),
        ('unknown', 'Unknown'),
    ]

    # Entity Identification
    entity_id = models.CharField(
        max_length=50,
        unique=True,
        db_index=True,
        help_text="State-issued entity ID number"
    )
    entity_name = models.CharField(
        max_length=255,
        db_index=True,
        help_text="Legal name of the entity"
    )
    entity_type = models.CharField(
        max_length=20,
        choices=ENTITY_TYPE_CHOICES,
        default='llc'
    )
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default='active',
        db_index=True
    )

    # Principal Information (the actual owner/manager we want)
    principal_name = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Name of managing member, president, or principal officer"
    )
    principal_title = models.CharField(
        max_length=100,
        blank=True,
        null=True,
        help_text="Title (e.g., Managing Member, President, Manager)"
    )

    # Registered Agent
    registered_agent = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Name of registered agent for legal service"
    )

    # Business Contact Info
    business_phone = models.CharField(
        max_length=20,
        blank=True,
        null=True,
        help_text="Primary business phone number"
    )
    business_email = models.EmailField(
        blank=True,
        null=True,
        help_text="Primary business email"
    )
    business_address = models.TextField(
        blank=True,
        null=True,
        help_text="Principal office address"
    )

    # Dates
    formation_date = models.DateField(
        blank=True,
        null=True,
        help_text="Date entity was formed/incorporated"
    )
    last_annual_report = models.DateField(
        blank=True,
        null=True,
        help_text="Date of most recent annual report filing"
    )

    # Data Management
    source_url = models.URLField(
        blank=True,
        null=True,
        help_text="URL of the source record"
    )
    raw_data = models.JSONField(
        default=dict,
        blank=True,
        help_text="Raw scraped data for audit trail"
    )

    created_at = models.DateTimeField(auto_now_add=True)
    last_updated = models.DateTimeField(
        auto_now=True,
        db_index=True,
        help_text="Used for 180-day cache freshness checks"
    )

    class Meta:
        db_table = 'leads_corporate_entity'
        ordering = ['-last_updated', 'entity_name']
        indexes = [
            models.Index(fields=['entity_name']),
            models.Index(fields=['entity_id']),
            models.Index(fields=['status']),
            models.Index(fields=['last_updated']),
            models.Index(fields=['principal_name']),
        ]
        verbose_name = 'Corporate Entity'
        verbose_name_plural = 'Corporate Entities'

    def __str__(self):
        return f"{self.entity_name} ({self.entity_id})"

    @property
    def is_cache_fresh(self, max_age_days=180):
        """Check if cached data is still fresh (default 180 days)."""
        from django.utils import timezone
        from datetime import timedelta

        if not self.last_updated:
            return False

        age = timezone.now() - self.last_updated
        return age < timedelta(days=max_age_days)

    @property
    def display_principal(self):
        """Return formatted principal with title if available."""
        if not self.principal_name:
            return "Unknown"
        if self.principal_title:
            return f"{self.principal_name} ({self.principal_title})"
        return self.principal_name
