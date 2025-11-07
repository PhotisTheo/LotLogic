import json
from typing import Dict, List, Optional, Tuple

from django import forms
from django.urls import reverse

from .models import Lead, ScheduleCallRequest, LienRecord, LegalAction, MailerTemplate


class LeadForm(forms.ModelForm):
    class Meta:
        model = Lead
        exclude = ("created_by",)


class UploadFileForm(forms.Form):
    file = forms.FileField()


class ParcelSearchForm(forms.Form):
    town_id = forms.CharField(
        label="Town",
        required=False,
        widget=forms.TextInput(attrs={"placeholder": "Start typing a town name (optional)…"}),
    )
    property_category = forms.ChoiceField(label="Category", choices=(), required=False)
    commercial_subtype = forms.ChoiceField(label="Commercial Type", choices=(), required=False)
    property_type = forms.ChoiceField(label="Property Type (MassGIS)", choices=(), required=False)
    address_contains = forms.CharField(
        label="Address contains",
        required=False,
        widget=forms.TextInput(attrs={"placeholder": "Street, number, etc."}),
    )
    equity_min = forms.DecimalField(
        label="Min. Equity %",
        min_value=0,
        max_value=100,
        required=False,
        help_text="Only include parcels where assessed value exceeds last sale price by this percentage.",
    )
    style = forms.CharField(
        label="Style contains",
        required=False,
        widget=forms.TextInput(attrs={"placeholder": "e.g. Colonial, Condo"}),
    )
    absentee = forms.ChoiceField(label="Occupancy", choices=(), required=False)
    limit = forms.IntegerField(
        label="Max results",
        min_value=1,
        max_value=10000,
        required=False,
        initial=1000,
        help_text="Limits how many parcels are returned per search (default 1000).",
    )
    min_price = forms.DecimalField(
        label="Min. Assessed Value",
        min_value=0,
        required=False,
        help_text="Only include parcels with total assessed value at or above this amount.",
    )
    max_price = forms.DecimalField(
        label="Max. Assessed Value",
        min_value=0,
        required=False,
        help_text="Only include parcels with total assessed value at or below this amount.",
    )
    min_years_owned = forms.IntegerField(
        label="Min. Years Owned",
        min_value=0,
        required=False,
        help_text="Filter for properties the current owner has held for at least this many years.",
    )
    proximity_address = forms.CharField(
        label="Center Address",
        required=False,
        help_text="Enter a street address to use as the center point.",
    )
    proximity_radius_miles = forms.DecimalField(
        label="Radius (miles)",
        min_value=0,
        required=False,
        help_text="Find parcels within this distance of the center parcel.",
    )

    def __init__(self, *args, **kwargs):
        from .services import (
            MassGISDataError,
            PARCEL_SEARCH_MAX_RESULTS,
            get_massgis_property_type_choices,
            get_massgis_town_choices,
            preload_massgis_dataset,
        )

        try:
            raw_choices = get_massgis_town_choices(include_placeholder=False)
        except MassGISDataError:
            raw_choices = []

        data = kwargs.get("data")
        town_id_to_label: dict[int, str] = {}
        self.town_options: List[dict[str, object]] = []
        for value, label in raw_choices:
            # All entries are regular town IDs
            try:
                town_id = int(value)
            except (TypeError, ValueError):
                continue
            town_id_to_label[town_id] = label
            self.town_options.append({"id": town_id, "label": label})
        self.town_options_json = json.dumps(self.town_options)

        if data:
            mutable = data.copy()
            raw_value = mutable.get("town_id")
            if raw_value and raw_value.isdigit():
                town_id = int(raw_value)
                label = town_id_to_label.get(town_id)
                if label:
                    mutable["town_id"] = label
            kwargs["data"] = mutable

        super().__init__(*args, **kwargs)

        self.cleaned_town_label: Optional[str] = None
        self.town_datalist_id = "parcel-town-options"
        self.town_datalist = raw_choices
        self._town_lookup: Dict[str, Tuple[int, str]] = {}

        for value, label in raw_choices:
            try:
                town_id = int(value)
            except (TypeError, ValueError):
                continue

            label_normalized = (label or "").strip().lower()
            if label_normalized:
                self._town_lookup.setdefault(label_normalized, (town_id, label))

                base_key = label.split(" (", 1)[0].strip().lower()
                if base_key:
                    self._town_lookup.setdefault(base_key, (town_id, label))

            self._town_lookup.setdefault(str(town_id), (town_id, label))

        def _match_town_identifier(value: Optional[str]) -> Optional[Tuple[int, str]]:
            if not value:
                return None
            variants = []
            text = str(value).strip()
            if not text:
                return None
            variants.append(text)
            variants.append(text.lower())
            if " (" in text:
                base = text.split(" (", 1)[0].strip()
                if base:
                    variants.extend([base, base.lower()])
            if text.isdigit():
                variants.append(text)
            for candidate in variants:
                match = self._town_lookup.get(candidate)
                if match:
                    return match
            return None

        matched = None
        if self.data:
            matched = _match_town_identifier(self.data.get("town_id"))
        if matched is None and kwargs.get("initial"):
            matched = _match_town_identifier(kwargs["initial"].get("town_id"))

        self.selected_town_id: Optional[int] = matched[0] if matched else None

        property_type_choices: List[Tuple[str, str]] = [("any", "Any property type")]
        property_type_options: List[Tuple[str, str]] = []
        if self.selected_town_id is not None:
            try:
                property_type_options = get_massgis_property_type_choices(self.selected_town_id)
            except MassGISDataError:
                property_type_options = []
        if property_type_options:
            property_type_choices.extend(property_type_options)

        submitted_property_type = None
        if self.data:
            submitted_property_type = self.data.get("property_type")
        elif kwargs.get("initial"):
            submitted_property_type = kwargs["initial"].get("property_type")

        submitted_property_type = (submitted_property_type or "").strip()
        if submitted_property_type and submitted_property_type not in {choice[0] for choice in property_type_choices}:
            property_type_choices.append((submitted_property_type, submitted_property_type))

        self.fields["property_type"].choices = property_type_choices

        self.fields["property_category"].choices = [
            ("any", "Any"),
            ("residential", "Residential"),
            ("commercial", "Commercial"),
            ("industrial", "Industrial"),
        ]

        self.fields["commercial_subtype"].choices = [
            ("any", "Any Commercial Type"),
            ("retail", "Retail"),
            ("office", "Office"),
            ("mixed_use", "Mixed Use"),
            ("service", "Service"),
        ]

        self.fields["absentee"].choices = [
            ("any", "Any"),
            ("owner", "Owner Occupied"),
            ("absentee", "Absentee"),
        ]

        if not self.data:
            self.fields["property_category"].initial = "any"
            self.fields["commercial_subtype"].initial = "any"
            self.fields["property_type"].initial = "any"
            self.fields["absentee"].initial = "any"
            self.fields["equity_min"].initial = None
            self.fields["limit"].initial = min(100, PARCEL_SEARCH_MAX_RESULTS)
            self.fields["min_price"].initial = None
            self.fields["max_price"].initial = None
            self.fields["min_years_owned"].initial = None
            self.fields["proximity_address"].initial = ""
            self.fields["proximity_radius_miles"].initial = None
        else:
            limit = self.data.get("limit")
            if limit:
                try:
                    limit_value = int(limit)
                except ValueError:
                    limit_value = None
                if limit_value:
                    self.fields["limit"].initial = min(limit_value, PARCEL_SEARCH_MAX_RESULTS)

        self.fields["town_id"].widget.attrs.setdefault("class", "form-control")
        self.fields["town_id"].widget.attrs.setdefault("list", self.town_datalist_id)
        self.fields["town_id"].widget.attrs.setdefault("autocomplete", "off")
        self.fields["town_id"].widget.attrs.setdefault("spellcheck", "false")
        self.fields["property_category"].widget.attrs.setdefault("class", "form-select")
        self.fields["property_type"].widget.attrs.setdefault("class", "form-select")
        self.fields["absentee"].widget.attrs.setdefault("class", "form-select")
        self.fields["address_contains"].widget.attrs.setdefault("class", "form-control")
        self.fields["style"].widget.attrs.setdefault("class", "form-control")
        self.fields["equity_min"].widget.attrs.setdefault("class", "form-control")
        self.fields["limit"].widget.attrs.setdefault("class", "form-control")
        self.fields["min_price"].widget.attrs.setdefault("class", "form-control")
        self.fields["max_price"].widget.attrs.setdefault("class", "form-control")
        self.fields["min_years_owned"].widget.attrs.setdefault("class", "form-control")
        self.fields["proximity_address"].widget.attrs.setdefault("class", "form-control")
        self.fields["proximity_radius_miles"].widget.attrs.setdefault("class", "form-control")

        initial_property_type = submitted_property_type or ""
        endpoint_template = reverse("property_type_choices", args=[0]).replace("/0/", "/__id__/")
        if "__id__" not in endpoint_template:
            endpoint_template = endpoint_template.replace("0", "__id__", 1)
        preload_template = reverse("town_preload", args=[0]).replace("/0/", "/__id__/")
        if "__id__" not in preload_template:
            preload_template = preload_template.replace("0", "__id__", 1)
        self.fields["property_type"].widget.attrs.setdefault("data-initial", initial_property_type)
        self.fields["property_type"].widget.attrs.setdefault("data-endpoint-template", endpoint_template)
        self.fields["property_type"].widget.attrs.setdefault("data-preload-template", preload_template)

    def clean_town_id(self) -> int:
        raw_value = self.cleaned_data.get("town_id")
        if raw_value is None:
            raise forms.ValidationError("Please choose a town to search.")

        value = str(raw_value).strip()
        if not value:
            raise forms.ValidationError("Please choose a town to search.")

        lookup_key = value.lower()
        match = self._town_lookup.get(lookup_key)

        if match is None:
            simplified = value.split(" (", 1)[0].strip().lower()
            match = self._town_lookup.get(simplified)

        if match is None:
            raise forms.ValidationError("Select a town from the suggestions to continue.")

        town_id, label = match
        self.cleaned_town_label = label
        return town_id


class ScheduleCallRequestForm(forms.ModelForm):
    preferred_call_time = forms.DateTimeField(
        required=False,
        label="Preferred Call Time",
        widget=forms.DateTimeInput(attrs={"type": "datetime-local"}),
        help_text="Optional – choose a time that works best for you.",
    )

    class Meta:
        model = ScheduleCallRequest
        fields = ["contact_phone", "preferred_call_time", "notes"]
        widgets = {
            "contact_phone": forms.TextInput(attrs={"placeholder": "Best phone number"}),
            "notes": forms.Textarea(
                attrs={"rows": 3, "placeholder": "Anything else we should know?"}
            ),
        }
        labels = {
            "contact_phone": "Best phone number",
            "notes": "Notes (optional)",
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            css_class = field.widget.attrs.get("class")
            field.widget.attrs["class"] = f"{css_class} form-control".strip() if css_class else "form-control"

    def clean_contact_phone(self):
        phone = (self.cleaned_data.get("contact_phone") or "").strip()
        if not phone:
            raise forms.ValidationError("Please provide the phone number we should call.")
        return phone


class ParcelListSaveForm(forms.Form):
    name = forms.CharField(
        label="List name",
        max_length=200,
        widget=forms.TextInput(attrs={"class": "form-control", "placeholder": "e.g. Salem Absentee Owners"}),
    )
    town_id = forms.IntegerField(widget=forms.HiddenInput())
    criteria = forms.CharField(widget=forms.HiddenInput())
    loc_ids = forms.CharField(widget=forms.HiddenInput())


class ScheduleCallForm(forms.Form):
    town_id = forms.IntegerField(widget=forms.HiddenInput(), required=False)
    loc_id = forms.CharField(widget=forms.HiddenInput(), required=False)
    recipient_name = forms.CharField(
        label="Your name",
        max_length=255,
        widget=forms.TextInput(
            attrs={
                "class": "form-control",
                "placeholder": "Start with the name on the letter",
            }
        ),
    )
    property_address = forms.CharField(
        label="Property address",
        max_length=255,
        widget=forms.TextInput(
            attrs={
                "class": "form-control",
                "placeholder": "Use the property featured in the mailer",
            }
        ),
    )
    contact_phone = forms.CharField(
        label="Best phone number",
        max_length=50,
        widget=forms.TextInput(
            attrs={
                "class": "form-control",
                "placeholder": "Enter the best number to reach you",
                "autocomplete": "tel",
            }
        ),
    )
    preferred_call_time = forms.DateTimeField(
        label="Preferred call time",
        required=False,
        input_formats=["%Y-%m-%dT%H:%M"],
        widget=forms.DateTimeInput(
            attrs={
                "class": "form-control",
                "type": "datetime-local",
            }
        ),
        help_text="Pick a date and time that fits your schedule.",
    )
    notes = forms.CharField(
        label="Anything we should know?",
        required=False,
        widget=forms.Textarea(
            attrs={
                "class": "form-control",
                "rows": 3,
                "placeholder": "Share context, gate codes, or timing preferences.",
            }
        ),
    )


class LienRecordForm(forms.ModelForm):
    """Form for adding/editing lien records"""

    recording_date = forms.DateField(
        required=False,
        label="Recording Date",
        widget=forms.DateInput(attrs={"type": "date", "class": "form-control"}),
        help_text="Date the lien was recorded"
    )

    release_date = forms.DateField(
        required=False,
        label="Release Date",
        widget=forms.DateInput(attrs={"type": "date", "class": "form-control"}),
        help_text="Date the lien was released/satisfied"
    )

    class Meta:
        model = LienRecord
        fields = [
            'lien_type', 'status', 'lien_holder', 'amount',
            'recording_date', 'book_number', 'page_number', 'instrument_number',
            'release_date', 'release_book_number', 'release_page_number', 'release_instrument_number',
            'source', 'source_url', 'notes'
        ]
        widgets = {
            'lien_type': forms.Select(attrs={'class': 'form-select'}),
            'status': forms.Select(attrs={'class': 'form-select'}),
            'lien_holder': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Name of lien holder/creditor'}),
            'amount': forms.NumberInput(attrs={'class': 'form-control', 'placeholder': '0.00', 'step': '0.01'}),
            'book_number': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Book number'}),
            'page_number': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Page number'}),
            'instrument_number': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Document/instrument number'}),
            'release_book_number': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Release book number'}),
            'release_page_number': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Release page number'}),
            'release_instrument_number': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Release instrument number'}),
            'source': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'e.g., Essex County Registry'}),
            'source_url': forms.URLInput(attrs={'class': 'form-control', 'placeholder': 'https://...'}),
            'notes': forms.Textarea(attrs={'class': 'form-control', 'rows': 3, 'placeholder': 'Additional notes...'}),
        }
        labels = {
            'lien_type': 'Lien Type',
            'status': 'Status',
            'lien_holder': 'Lien Holder/Creditor',
            'amount': 'Amount ($)',
            'book_number': 'Book Number',
            'page_number': 'Page Number',
            'instrument_number': 'Instrument Number',
            'release_date': 'Release Date',
            'release_book_number': 'Release Book Number',
            'release_page_number': 'Release Page Number',
            'release_instrument_number': 'Release Instrument Number',
            'source': 'Source',
            'source_url': 'Source URL',
            'notes': 'Notes',
        }


class LegalActionForm(forms.ModelForm):
    """Form for adding/editing legal action records"""

    filing_date = forms.DateField(
        required=False,
        label="Filing Date",
        widget=forms.DateInput(attrs={"type": "date", "class": "form-control"}),
        help_text="Date the case was filed"
    )

    hearing_date = forms.DateField(
        required=False,
        label="Hearing Date",
        widget=forms.DateInput(attrs={"type": "date", "class": "form-control"}),
        help_text="Date of scheduled hearing"
    )

    judgment_date = forms.DateField(
        required=False,
        label="Judgment Date",
        widget=forms.DateInput(attrs={"type": "date", "class": "form-control"}),
        help_text="Date judgment was entered"
    )

    closed_date = forms.DateField(
        required=False,
        label="Closed Date",
        widget=forms.DateInput(attrs={"type": "date", "class": "form-control"}),
        help_text="Date the case was closed"
    )

    class Meta:
        model = LegalAction
        fields = [
            'action_type', 'status', 'court', 'case_number',
            'plaintiff', 'defendant',
            'filing_date', 'hearing_date', 'judgment_date', 'closed_date',
            'amount_claimed', 'judgment_amount',
            'source', 'source_url', 'pacer_case_id',
            'description', 'notes'
        ]
        widgets = {
            'action_type': forms.Select(attrs={'class': 'form-select'}),
            'status': forms.Select(attrs={'class': 'form-select'}),
            'court': forms.Select(attrs={'class': 'form-select'}),
            'case_number': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Court case/docket number'}),
            'plaintiff': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Plaintiff/petitioner name'}),
            'defendant': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Defendant/respondent name'}),
            'amount_claimed': forms.NumberInput(attrs={'class': 'form-control', 'placeholder': '0.00', 'step': '0.01'}),
            'judgment_amount': forms.NumberInput(attrs={'class': 'form-control', 'placeholder': '0.00', 'step': '0.01'}),
            'source': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'e.g., PACER, MA Trial Court eAccess'}),
            'source_url': forms.URLInput(attrs={'class': 'form-control', 'placeholder': 'https://...'}),
            'pacer_case_id': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'PACER case ID (if applicable)'}),
            'description': forms.Textarea(attrs={'class': 'form-control', 'rows': 2, 'placeholder': 'Brief description of the case'}),
            'notes': forms.Textarea(attrs={'class': 'form-control', 'rows': 3, 'placeholder': 'Additional notes...'}),
        }
        labels = {
            'action_type': 'Action Type',
            'status': 'Status',
            'court': 'Court',
            'case_number': 'Case Number',
            'plaintiff': 'Plaintiff',
            'defendant': 'Defendant',
            'filing_date': 'Filing Date',
            'hearing_date': 'Hearing Date',
            'judgment_date': 'Judgment Date',
            'closed_date': 'Closed Date',
            'amount_claimed': 'Amount Claimed ($)',
            'judgment_amount': 'Judgment Amount ($)',
            'source': 'Source',
            'source_url': 'Source URL',
            'pacer_case_id': 'PACER Case ID',
            'description': 'Description',
            'notes': 'Notes',
        }


class MailerTemplateForm(forms.ModelForm):
    value_props_raw = forms.CharField(
        label="Bullet points",
        required=False,
        widget=forms.Textarea(
            attrs={
                "class": "form-control",
                "rows": 4,
                "placeholder": "One point per line. These render beneath the letter as bullet points.",
            }
        ),
        help_text="Optional list of value props. Enter one item per line.",
    )

    class Meta:
        model = MailerTemplate
        fields = [
            "name",
            "summary",
            "sector",
            "letter_body",
            "value_props_title",
            "prompt_text",
            "is_active",
        ]
        widgets = {
            "name": forms.TextInput(
                attrs={"class": "form-control", "placeholder": "Template name"}
            ),
            "summary": forms.TextInput(
                attrs={
                    "class": "form-control",
                    "placeholder": "Short description shown in template pickers",
                }
            ),
            "sector": forms.Select(attrs={"class": "form-select"}),
            "letter_body": forms.Textarea(
                attrs={
                    "class": "form-control",
                    "rows": 10,
                    "placeholder": (
                        "Write the letter body. Use placeholders like {salutation_name}, "
                        "{property_address}, {agent_name}, {contact_phone}, or {property_descriptor} "
                        "to personalize each mailer."
                    ),
                }
            ),
            "value_props_title": forms.TextInput(
                attrs={
                    "class": "form-control",
                    "placeholder": "Optional heading shown above the bullet list",
                }
            ),
            "prompt_text": forms.Textarea(
                attrs={
                    "class": "form-control",
                    "rows": 4,
                    "placeholder": "Optional guidance for AI-generated variants, tone, or follow-up scripts.",
                }
            ),
            "is_active": forms.CheckboxInput(attrs={"class": "form-check-input"}),
        }
        labels = {
            "letter_body": "Letter body",
            "prompt_text": "AI prompt (optional)",
            "is_active": "Template is active",
        }
        help_texts = {
            "sector": "Sector helps pre-select the right template for residential vs. commercial parcels.",
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        instance = getattr(self, "instance", None)
        if instance and instance.pk:
            value_props = instance.value_props or []
            self.fields["value_props_raw"].initial = "\n".join(value_props)
        self.fields["is_active"].widget.attrs.setdefault("role", "switch")

    def save(self, commit: bool = True):
        instance = super().save(commit=False)
        raw_value_props = self.cleaned_data.get("value_props_raw") or ""
        parsed_value_props = [
            line.strip() for line in str(raw_value_props).splitlines() if line.strip()
        ]
        instance.value_props = parsed_value_props
        if commit:
            instance.save()
        return instance
