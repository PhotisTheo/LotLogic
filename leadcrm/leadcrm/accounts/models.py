import uuid
from typing import Optional

from django.conf import settings
from django.contrib.auth.models import User
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver


class UserProfile(models.Model):
    ACCOUNT_INDIVIDUAL = "individual"
    ACCOUNT_TEAM_LEAD = "team_lead"
    ACCOUNT_TEAM_MEMBER = "team_member"

    ACCOUNT_TYPE_CHOICES = [
        (ACCOUNT_INDIVIDUAL, "Individual"),
        (ACCOUNT_TEAM_LEAD, "Team Lead"),
        (ACCOUNT_TEAM_MEMBER, "Team Member"),
    ]

    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name="profile")
    account_type = models.CharField(
        max_length=20, choices=ACCOUNT_TYPE_CHOICES, default=ACCOUNT_INDIVIDUAL
    )
    team_lead = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="team_memberships",
        null=True,
        blank=True,
    )
    plan_id = models.CharField(max_length=50, default="individual_standard")
    plan_amount_cents = models.PositiveIntegerField(default=2500)
    billing_status = models.CharField(max_length=20, default="active")
    payment_intent_id = models.CharField(max_length=100, blank=True, null=True)
    stripe_customer_id = models.CharField(max_length=100, blank=True, null=True)
    stripe_subscription_id = models.CharField(max_length=100, blank=True, null=True)
    company_name = models.CharField(max_length=120, blank=True)
    job_title = models.CharField(max_length=120, blank=True)
    work_phone = models.CharField(max_length=32, blank=True)
    mobile_phone = models.CharField(max_length=32, blank=True)
    bio = models.TextField(blank=True)

    def __str__(self) -> str:
        label = dict(self.ACCOUNT_TYPE_CHOICES).get(self.account_type, self.account_type)
        return f"{self.user.username} ({label})"

    @property
    def plan(self) -> dict:
        from .plans import PLAN_CATALOG, DEFAULT_PLAN_ID

        return PLAN_CATALOG.get(self.plan_id or DEFAULT_PLAN_ID, PLAN_CATALOG[DEFAULT_PLAN_ID])

    @property
    def plan_name(self) -> str:
        return self.plan.get("label", "Individual")

    @property
    def plan_price_display(self) -> str:
        return self.plan.get("price_display", "$0")

    @property
    def team_member_limit(self) -> int:
        seat_limit = self.plan.get("seat_limit")
        if seat_limit is not None:
            return seat_limit
        return 1

    @property
    def workspace_owner(self) -> User:
        if self.account_type == self.ACCOUNT_TEAM_MEMBER and self.team_lead:
            return self.team_lead
        return self.user

    @property
    def remaining_invites(self) -> int:
        if self.account_type != self.ACCOUNT_TEAM_LEAD:
            return 0
        accepted = UserProfile.objects.filter(team_lead=self.user).count()
        pending = TeamInvite.objects.filter(
            team_lead=self.user, accepted_at__isnull=True
        ).count()
        remaining = self.team_member_limit - (accepted + pending)
        return max(0, remaining)


class TeamInvite(models.Model):
    team_lead = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="team_invites",
    )
    email = models.EmailField()
    token = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    accepted_at = models.DateTimeField(null=True, blank=True)
    accepted_user = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="accepted_team_invites",
    )

    class Meta:
        ordering = ["-created_at"]
        unique_together = [("team_lead", "email")]

    def __str__(self) -> str:
        return f"Invite from {self.team_lead.username} to {self.email}"

    @property
    def is_accepted(self) -> bool:
        return self.accepted_at is not None

    @property
    def remaining_slots(self) -> int:
        profile = getattr(self.team_lead, "profile", None)
        if not profile:
            return 0
        return profile.remaining_invites

    def can_accept(self) -> bool:
        if self.is_accepted:
            return False
        profile = getattr(self.team_lead, "profile", None)
        if not profile:
            return False
        accepted = UserProfile.objects.filter(team_lead=self.team_lead).count()
        return accepted < profile.team_member_limit


def get_workspace_owner(user: Optional[User]) -> Optional[User]:
    if not user or not isinstance(user, User):
        return None
    profile = getattr(user, "profile", None)
    if not profile:
        return user
    return profile.workspace_owner


@receiver(post_save, sender=User)
def ensure_user_profile(sender, instance: User, created: bool, **kwargs):
    if created:
        UserProfile.objects.create(user=instance)
    else:
        UserProfile.objects.get_or_create(user=instance)
