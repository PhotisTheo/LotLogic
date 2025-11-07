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
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["town_id", "loc_id"]),
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
