from django import forms
from django.contrib.auth.forms import AuthenticationForm, UserCreationForm
from django.contrib.auth.models import User

from .models import TeamInvite, UserProfile
from .plans import PLAN_CATALOG, PLAN_GROUPS, DEFAULT_PLAN_ID, PUBLIC_SIGNUP_PLAN_IDS


class UserSignupForm(UserCreationForm):
    email = forms.EmailField(
        required=True,
        widget=forms.EmailInput(attrs={"class": "form-control", "placeholder": "you@example.com"}),
    )
    account_type = forms.ChoiceField(
        choices=[
            (UserProfile.ACCOUNT_INDIVIDUAL, "Individual"),
            (UserProfile.ACCOUNT_TEAM_LEAD, "Team Lead"),
        ],
        widget=forms.Select(attrs={"class": "form-select"}),
    )

    plan_id = forms.ChoiceField(
        choices=[(DEFAULT_PLAN_ID, "Solo Agent")],
        required=False,
    )
    accept_terms = forms.BooleanField(
        required=True,
        label="I agree to the Lead CRM Terms & Conditions",
    )

    class Meta:
        model = User
        fields = ("username", "email", "account_type", "plan_id")

    def __init__(
        self,
        *args,
        invite: TeamInvite | None = None,
        plan_groups: dict | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.invite = invite
        self.plan_groups = plan_groups or PLAN_GROUPS
        for field in self.fields.values():
            css = field.widget.attrs.get("class", "").strip()
            if isinstance(field.widget, forms.CheckboxInput):
                field.widget.attrs["class"] = f"{css} form-check-input".strip()
            else:
                field.widget.attrs["class"] = f"{css} form-control".strip()
        self.fields["username"].widget.attrs.update(
            {"class": "form-control form-control-lg", "placeholder": "Create a username"}
        )
        self.fields["email"].widget.attrs.update(
            {"class": "form-control form-control-lg", "placeholder": "you@example.com"}
        )
        self.fields["password1"].widget.attrs.update(
            {"class": "form-control", "placeholder": "Create password"}
        )
        self.fields["password2"].widget.attrs.update(
            {"class": "form-control", "placeholder": "Confirm password"}
        )
        if self.invite:
            self.fields["account_type"].initial = UserProfile.ACCOUNT_TEAM_MEMBER
            self.fields["account_type"].widget = forms.HiddenInput()
            self.fields["plan_id"].initial = "team_member_included"
            self.fields["plan_id"].widget = forms.HiddenInput()
            self.fields["plan_id"].required = False
            self.fields["accept_terms"].widget.attrs.setdefault("class", "form-check-input")
        else:
            self.fields["account_type"].initial = UserProfile.ACCOUNT_INDIVIDUAL
            self.fields["account_type"].widget.attrs.setdefault("class", "form-select")
            self.fields["account_type"].help_text = "Team leads can invite their whole team—start with 15 seats and upgrade to 30 anytime."
            self.fields["plan_id"].required = True
            public_plan_ids = [
                plan_id
                for plan_id in PUBLIC_SIGNUP_PLAN_IDS
                if plan_id in PLAN_CATALOG
            ] or [DEFAULT_PLAN_ID]
            self.fields["plan_id"].initial = public_plan_ids[0]
            self.fields["plan_id"].choices = [
                (plan_id, PLAN_CATALOG[plan_id]["label"])
                for plan_id in public_plan_ids
                if plan_id in PLAN_CATALOG
            ]
            self.fields["plan_id"].widget = forms.HiddenInput()
            self.fields["accept_terms"].widget.attrs.setdefault("class", "form-check-input")

    def clean_account_type(self):
        account_type = self.cleaned_data.get("account_type")
        if self.invite:
            return UserProfile.ACCOUNT_TEAM_MEMBER
        if account_type not in {
            UserProfile.ACCOUNT_INDIVIDUAL,
            UserProfile.ACCOUNT_TEAM_LEAD,
        }:
            raise forms.ValidationError("Please choose an account type.")
        return account_type

    def clean_email(self):
        email = (self.cleaned_data.get("email") or "").strip()
        if not email:
            return email
        if User.objects.filter(email__iexact=email).exists():
            raise forms.ValidationError(
                "An account already exists with this email address."
            )
        return email

    def clean_plan_id(self):
        plan_id = self.cleaned_data.get("plan_id")
        if self.invite:
            self.cleaned_data["account_type"] = UserProfile.ACCOUNT_TEAM_MEMBER
            return "team_member_included"
        if not plan_id:
            raise forms.ValidationError("Please choose a billing plan.")
        if plan_id not in PLAN_CATALOG:
            raise forms.ValidationError("Please choose a valid billing plan.")
        if not self.invite and plan_id not in PUBLIC_SIGNUP_PLAN_IDS:
            raise forms.ValidationError("Selected plan is not available right now.")
        plan = PLAN_CATALOG[plan_id]
        plan_account_type = plan.get("account_type") or UserProfile.ACCOUNT_INDIVIDUAL
        allowed = set(self.plan_groups.get(plan_account_type, []))
        if plan_id not in allowed:
            raise forms.ValidationError("Selected plan is not available for this account type.")
        self.cleaned_data["account_type"] = plan_account_type
        return plan_id

    def clean_accept_terms(self):
        accepted = self.cleaned_data.get("accept_terms")
        if not accepted:
            raise forms.ValidationError("You must agree to the Terms & Conditions to continue.")
        return accepted

    def clean(self):
        cleaned = super().clean()
        if self.invite:
            if not self.invite.can_accept():
                raise forms.ValidationError(
                    "This team invite is no longer available. Contact the team lead for a new link."
                )
        return cleaned


class TeamInviteForm(forms.Form):
    email = forms.EmailField(
        widget=forms.EmailInput(attrs={"class": "form-control", "placeholder": "teammate@example.com"})
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            css = field.widget.attrs.get("class", "")
            field.widget.attrs["class"] = f"{css} form-control".strip()


class StyledAuthenticationForm(AuthenticationForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            css = field.widget.attrs.get("class", "")
            field.widget.attrs["class"] = f"{css} form-control".strip()
        self.fields["username"].widget.attrs.setdefault("placeholder", "Username")
        self.fields["password"].widget.attrs.setdefault("placeholder", "Password")


class ProfileUpdateForm(forms.Form):
    first_name = forms.CharField(
        label="First name",
        required=False,
        widget=forms.TextInput(attrs={"class": "form-control", "placeholder": "Your first name"}),
    )
    last_name = forms.CharField(
        label="Last name",
        required=False,
        widget=forms.TextInput(attrs={"class": "form-control", "placeholder": "Your last name"}),
    )
    company_name = forms.CharField(
        label="Company",
        required=False,
        widget=forms.TextInput(attrs={"class": "form-control", "placeholder": "Company or team"}),
    )
    job_title = forms.CharField(
        label="Role",
        required=False,
        widget=forms.TextInput(attrs={"class": "form-control", "placeholder": "Title or role"}),
    )
    work_phone = forms.CharField(
        label="Work phone",
        required=False,
        widget=forms.TextInput(attrs={"class": "form-control", "placeholder": "Work phone"}),
    )
    mobile_phone = forms.CharField(
        label="Mobile",
        required=False,
        widget=forms.TextInput(attrs={"class": "form-control", "placeholder": "Mobile phone"}),
    )
    bio = forms.CharField(
        label="About you",
        required=False,
        widget=forms.Textarea(
            attrs={
                "class": "form-control",
                "rows": 4,
                "placeholder": "Share a short intro for teammates—areas served, specialties, and how you work leads.",
            }
        ),
    )

    def __init__(self, *args, user: User | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.user = user
        profile = getattr(user, "profile", None) if user else None
        if user:
            self.fields["first_name"].initial = user.first_name
            self.fields["last_name"].initial = user.last_name
        if profile:
            self.fields["company_name"].initial = profile.company_name
            self.fields["job_title"].initial = profile.job_title
            self.fields["work_phone"].initial = profile.work_phone
            self.fields["mobile_phone"].initial = profile.mobile_phone
            self.fields["bio"].initial = profile.bio

    def save(self):
        if not self.user:
            return
        cleaned = self.cleaned_data
        user = self.user
        user.first_name = cleaned.get("first_name", "").strip()
        user.last_name = cleaned.get("last_name", "").strip()
        user.save(update_fields=["first_name", "last_name"])

        profile = user.profile
        profile.company_name = cleaned.get("company_name", "").strip()
        profile.job_title = cleaned.get("job_title", "").strip()
        profile.work_phone = cleaned.get("work_phone", "").strip()
        profile.mobile_phone = cleaned.get("mobile_phone", "").strip()
        profile.bio = cleaned.get("bio", "").strip()
        profile.save(
            update_fields=["company_name", "job_title", "work_phone", "mobile_phone", "bio"]
        )
