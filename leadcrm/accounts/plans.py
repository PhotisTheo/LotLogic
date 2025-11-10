from __future__ import annotations

from typing import Dict, List

from django.conf import settings

ACCOUNT_INDIVIDUAL = "individual"
ACCOUNT_TEAM_LEAD = "team_lead"
ACCOUNT_TEAM_MEMBER = "team_member"


DEFAULT_PLAN_ID = "beta_tester"

PUBLIC_SIGNUP_PLAN_IDS = ("beta_tester",)

PLAN_CATALOG: Dict[str, Dict[str, object]] = {
    "beta_tester": {
        "id": "beta_tester",
        "label": "Beta Tester",
        "subtitle": "Preview the newest Lead CRM updates alongside our team.",
        "price_cents": 0,
        "price_display": "Free during beta",
        "account_type": ACCOUNT_INDIVIDUAL,
        "seat_limit": 1,
        "features": [
            "Unlimited parcel intelligence searches",
            "Mailer builder, saved lists, and CRM tools",
            "Skip tracing available as in-app purchases",
        ],
        "cta": "Join the beta",
        "highlight": True,
        "badge": "Now open",
        "badge_style": "bg-success",
        "public_signup": True,
        "requires_manual_approval": True,
    },
    "individual_standard": {
        "id": "individual_standard",
        "label": "Solo Agent",
        "subtitle": "Best for independent agents growing their pipeline.",
        "price_cents": 2500,
        "price_display": "$25 / month",
        "account_type": ACCOUNT_INDIVIDUAL,
        "seat_limit": 1,
        "features": [
            "Unlimited parcel lookups",
            "On-demand skip tracing",
            "Automated mailer generation",
            "Saved lists & CRM exports",
        ],
        "cta": "Start as a solo agent",
        "highlight": False,
        "badge": "Popular with new agents",
        "badge_style": "bg-info text-dark",
        "stripe_price_id": getattr(settings, "STRIPE_PRICE_INDIVIDUAL_STANDARD", ""),
    },
    "team_standard": {
        "id": "team_standard",
        "label": "Team Workspace",
        "subtitle": "Invite up to 15 teammates and run outreach together.",
        "price_cents": 20000,
        "price_display": "$200 / month",
        "account_type": ACCOUNT_TEAM_LEAD,
        "seat_limit": 15,
        "features": [
            "Shared saved lists & CRM",
            "Team activity tracking",
            "Centralized skip trace billing",
            "Priority support & onboarding",
        ],
        "cta": "Launch your team workspace",
        "highlight": True,
        "badge": "Most popular",
        "badge_style": "bg-primary",
        "upgrade_note": "Need more seats? Upgrade to Growth 30 anytime.",
        "stripe_price_id": getattr(settings, "STRIPE_PRICE_TEAM_STANDARD", ""),
    },
    "team_plus": {
        "id": "team_plus",
        "label": "Growth 30",
        "subtitle": "Scaling teams that need more seats and support.",
        "price_cents": 35000,
        "price_display": "$350 / month",
        "account_type": ACCOUNT_TEAM_LEAD,
        "seat_limit": 30,
        "features": [
            "Everything in Team Workspace",
            "30 user seats included",
            "Dedicated success manager",
            "Quarterly strategy review",
        ],
        "cta": "Upgrade to Growth 30",
        "highlight": False,
        "badge": "Scale-ready",
        "badge_style": "bg-warning text-dark",
        "upgrade_note": "You can start with 15 seats and upgrade later.",
        "stripe_price_id": getattr(settings, "STRIPE_PRICE_TEAM_PLUS", ""),
    },
    "team_member_included": {
        "id": "team_member_included",
        "label": "Team Member",
        "subtitle": "Included with your team lead’s subscription.",
        "price_cents": 0,
        "price_display": "Included",
        "account_type": ACCOUNT_TEAM_MEMBER,
        "seat_limit": 0,
        "features": [
            "Shared team workspace access",
            "Skip trace & mailer tools",
            "Real-time collaboration",
        ],
        "cta": "Join your team",
        "highlight": False,
        "badge": None,
        "badge_style": None,
    },
}


PLAN_GROUPS: Dict[str, List[str]] = {
    ACCOUNT_INDIVIDUAL: ["beta_tester", "individual_standard"],
    ACCOUNT_TEAM_LEAD: ["team_standard", "team_plus"],
    ACCOUNT_TEAM_MEMBER: ["team_member_included"],
}


def get_plan(plan_id: str) -> Dict[str, object]:
    return PLAN_CATALOG.get(plan_id, PLAN_CATALOG[DEFAULT_PLAN_ID])


def plans_for_account_type(account_type: str) -> List[Dict[str, object]]:
    plan_ids = PLAN_GROUPS.get(account_type, [])
    return [PLAN_CATALOG[plan_id] for plan_id in plan_ids if plan_id in PLAN_CATALOG]
