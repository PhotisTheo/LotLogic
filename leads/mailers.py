from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Literal, Optional, Sequence, Tuple

from django.conf import settings
from django.contrib.auth.models import User

from .models import MailerTemplate

logger = logging.getLogger(__name__)


@dataclass
class MailerAgentProfile:
    name: Optional[str]
    title: Optional[str]
    company: Optional[str]
    tagline: Optional[str]


def _safe_currency(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    try:
        return f"${float(value):,.0f}"
    except (ValueError, TypeError):
        return None


def _safe_percentage(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    try:
        return f"{float(value):.1f}%"
    except (ValueError, TypeError):
        return None


def _interpret_bool_flag(value: Optional[object]) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if not normalized:
            return None
        if normalized in {"y", "yes", "true", "1"}:
            return True
        if normalized in {"n", "no", "false", "0"}:
            return False
        return None
    return bool(value)


@dataclass
class MailerFallback:
    lines: List[str]
    value_props: List[str]


class _SafeDefaultDict(dict):
    def __missing__(self, key: str) -> str:
        return ""


MAILER_TEMPLATES: Dict[str, Dict[str, Any]] = {
    "res_neighborhood_intro": {
        "banner_label": "Neighborhood Connection",
        "headline": "Interest in {property_address}",
        "subheadline": "A quick hello for {property_city_friendly}",
        "letter": [
            "Dear {salutation_name},",
            "",
            "{agent_intro_sentence}",
            "I'm reaching out because buyers I represent are focused on {property_descriptor} homes in {property_city_friendly}. {property_address} is the kind of property they ask about first.",
            "Inventory nearby is still tight, so they asked me to connect quietly with a handful of owners before anything goes public.",
            "If you're ever open to a private conversation—now or down the road—I'm glad to share what they're looking for and how they handle timing.",
        ],
        "value_props": [
            "Qualified buyers targeting {focus_area} with flexible timing.",
            "Respectful, one-on-one conversations—no open houses or yard signage.",
            "You stay in control of next steps; we move only when it fits your plans.",
        ],
    },
    "res_equity_gameplan": {
        "banner_label": "Equity Game Plan",
        "headline": "What Your Equity Can Unlock",
        "subheadline": "A concise snapshot for {property_city_friendly}",
        "letter": [
            "Dear {salutation_name},",
            "",
            "{agent_intro_sentence}",
            "I pulled together a quick market pulse for {property_address} because neighbors are asking where values stand heading into this season.",
            "{equity_sentence}",
            "My clients are looking for opportunities to match motivated sellers with buyers who respect a private timeline.",
        ],
        "value_props": [
            "Clear talking points about today's demand for {property_descriptor} homes.",
            "Flexible paths—stay put, refinance, or explore a quiet match.",
            "Trusted buyers ready to accommodate the timing that works for you.",
        ],
    },
    "res_private_sale": {
        "banner_label": "Private Buyer Match",
        "headline": "A Discreet Buyer For {property_address}",
        "subheadline": "Sharing an off-market option for {property_city_friendly}",
        "letter": [
            "Dear {salutation_name},",
            "",
            "{agent_intro_sentence}",
            "I represent a pre-approved buyer searching specifically for a {property_descriptor} in {property_city_friendly}.",
            "They value privacy and are prepared to craft an agreement that fits your timing and terms.",
            "I'm reaching out to see if an informal conversation would be welcome—even if you're just curious about the numbers.",
        ],
        "value_props": [
            "Serious buyer already vetted and ready to move forward.",
            "Flexible occupancy and closing timelines tailored to your plans.",
            "No obligation—just an introductory conversation if it helps.",
        ],
    },
    "com_local_ops": {
        "banner_label": "Local Expansion Alert",
        "headline": "Operators Eye {property_city_friendly}",
        "subheadline": "Checking availability for {property_descriptor} properties like {property_address}",
        "letter": [
            "Dear {salutation_name},",
            "",
            "{agent_intro_sentence}",
            "We support operators who are actively scouting {property_type_focus} space in {property_city_friendly}. {property_address} is exactly the profile they're targeting.",
            "They're prepared to discuss lease, sale, or hybrid structures depending on what aligns with your plans.",
            "If you'd entertain a confidential conversation, I'm happy to outline their criteria and see whether there's a mutual fit.",
        ],
        "value_props": [
            "Operators with funding in place, prepared to move quickly.",
            "Flexible deal structures—purchase, lease, or creative partnerships.",
            "Discreet outreach so you stay in control of timing and visibility.",
        ],
    },
    "com_market_pulse": {
        "banner_label": "Commercial Market Pulse",
        "headline": "Demand Trends Around {property_city_friendly}",
        "subheadline": "A concise check-in for {property_address}",
        "letter": [
            "Dear {salutation_name},",
            "",
            "{agent_intro_sentence}",
            "Regional investors continue to focus on {property_type_focus} assets in {property_city_friendly}, and they’ve asked for a direct line to owners like you.",
            "I'm sharing a short market brief so you can track how deals are getting structured right now.",
            "If you'd like a custom rundown—including recent comps and incentive packages—I'm glad to prepare it.",
        ],
        "value_props": [
            "First-look access to demand for {property_descriptor} properties.",
            "Data-backed guidance on pricing, incentives, and lease terms.",
            "No-pressure planning session tailored to your operational goals.",
        ],
    },
    "com_investor_brief": {
        "banner_label": "Private Capital Brief",
        "headline": "Capital Ready for {property_type_focus} Assets",
        "subheadline": "Exploring strategic moves for {property_city_friendly}",
        "letter": [
            "Dear {salutation_name},",
            "",
            "{agent_intro_sentence}",
            "Our private-capital partners are ready to place funds into standout assets like {property_address}.",
            "They're flexible on structure—outright acquisition, recapitalization, or joint venture—whichever aligns with your objectives.",
            "I'd welcome a confidential conversation to see whether their deployment timeline lines up with yours.",
        ],
        "value_props": [
            "Capital allocated and ready for immediate deployment.",
            "Collaborative approach to match your hold, exit, or recap goals.",
            "Streamlined diligence to keep conversations private and efficient.",
        ],
    },
}


@dataclass
class MailerFallbackContext:
    parcel: Any
    property_address: str
    full_address: Optional[str]
    greeting_name: str
    recipient_full_name: Optional[str]
    contact_phone: str
    text_keyword: str
    text_keyword_upper: str
    agent: MailerAgentProfile
    zillow_url: Optional[str]
    property_facts: List[str]
    equity_value_display: Optional[str]
    equity_percent_display: Optional[str]
    total_value_display: Optional[str]
    zillow_zestimate_value: Optional[float]
    zillow_zestimate_display: Optional[str]


def _safe_text(value: Optional[object]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isupper():
        return text.title()
    return text


def _normalize_city(parcel: Any) -> Optional[str]:
    raw_city = getattr(parcel, "site_city", None) if parcel else None
    return _safe_text(raw_city)


def _build_template_context(ctx: MailerFallbackContext) -> Dict[str, str]:
    city = _normalize_city(ctx.parcel)
    property_city_friendly = city or "your area"
    city_suffix = f" in {city}" if city else ""

    greeting_first = ctx.greeting_name or "there"
    full_name = _safe_text(ctx.recipient_full_name)
    salutation_name = full_name or greeting_first

    property_type = _safe_text(getattr(ctx.parcel, "property_type", None))
    property_category = _safe_text(getattr(ctx.parcel, "property_category", None))
    style = _safe_text(getattr(ctx.parcel, "style", None))

    descriptor_parts: List[str] = []
    if property_type:
        descriptor_parts.append(property_type.lower())
    if style and style.lower() not in (property_type or "").lower():
        descriptor_parts.append(style.lower())
    property_descriptor = " ".join(descriptor_parts).strip() or "property"

    focus_area = city or "this pocket"
    property_type_focus = property_type or property_category or "Local"

    if ctx.equity_value_display and ctx.equity_percent_display:
        equity_sentence = (
            f"Based on recent activity, you likely have {ctx.equity_value_display} in available equity ({ctx.equity_percent_display})."
        )
    else:
        equity_sentence = (
            "Recent activity suggests you've built meaningful equity—even if moving isn't on your radar yet."
        )

    trend_label = (
        property_type or property_category or property_descriptor or "local"
    ).lower()

    agent_name = ctx.agent.name if ctx.agent and ctx.agent.name else ""
    agent_title_line = ctx.agent.title if ctx.agent and ctx.agent.title else ""
    agent_company_line = ctx.agent.company if ctx.agent and ctx.agent.company else ""

    identity_sentence = ""
    if agent_name:
        if agent_title_line and agent_company_line:
            identity_sentence = f"{agent_name}, {agent_title_line} at {agent_company_line}"
        elif agent_title_line:
            identity_sentence = f"{agent_name}, {agent_title_line}"
        elif agent_company_line:
            identity_sentence = f"{agent_name} at {agent_company_line}"
        else:
            identity_sentence = agent_name
    else:
        if agent_title_line and agent_company_line:
            identity_sentence = f"{agent_title_line} at {agent_company_line}"
        elif agent_title_line:
            identity_sentence = agent_title_line
        elif agent_company_line:
            identity_sentence = agent_company_line

    agent_intro_sentence = f"I'm {identity_sentence}." if identity_sentence else ""

    context = {
        "greeting_name": greeting_first,
        "salutation_name": salutation_name,
        "property_address": ctx.property_address or "your property",
        "property_city_friendly": property_city_friendly,
        "city_suffix": city_suffix,
        "contact_phone": ctx.contact_phone or "",
        "text_keyword_upper": ctx.text_keyword_upper or "",
        "agent_name": agent_name,
        "agent_title_line": agent_title_line,
        "agent_company_line": agent_company_line,
        "agent_intro_sentence": agent_intro_sentence,
        "focus_area": focus_area,
        "property_type_focus": property_type_focus,
        "property_descriptor": property_descriptor,
        "trend_label": trend_label,
        "equity_sentence": equity_sentence,
    }

    return context


def _render_template_mailer(template_id: str, ctx: MailerFallbackContext) -> MailerFallback:
    template = MAILER_TEMPLATES.get(template_id)
    if not template:
        raise KeyError(f"Mailer template '{template_id}' not found")

    formatter = _SafeDefaultDict(_build_template_context(ctx))
    letter_lines = [line.format_map(formatter).strip() for line in template["letter"]]
    while letter_lines and not letter_lines[-1]:
        letter_lines.pop()

    value_props: List[str] = []
    for raw_line in template["value_props"]:
        rendered = raw_line.format_map(formatter).strip()
        if rendered and rendered not in value_props:
            value_props.append(rendered)

    return MailerFallback(lines=letter_lines, value_props=value_props)


def get_mailer_template_metadata(template_id: str, ctx: MailerFallbackContext) -> Dict[str, str]:
    template = MAILER_TEMPLATES.get(template_id)
    if not template:
        return {}
    formatter = _SafeDefaultDict(_build_template_context(ctx))
    return {
        "banner_label": template["banner_label"].format_map(formatter).strip(),
        "headline": template["headline"].format_map(formatter).strip(),
        "subheadline": template["subheadline"].format_map(formatter).strip(),
    }


@dataclass(frozen=True)
class MailerScriptOption:
    id: str
    template_id: str
    sector: Literal["residential", "commercial"]
    label: str
    summary: str
    value_props_title: str
    custom_template_id: Optional[int] = None
    prompt_text: Optional[str] = None


MAILER_SCRIPT_OPTIONS: List[MailerScriptOption] = [
    MailerScriptOption(
        id="res_intro",
        template_id="res_neighborhood_intro",
        sector="residential",
        label="Residential · Neighborhood Introduction",
        summary="Warm outreach that lets nearby owners know qualified buyers are waiting quietly.",
        value_props_title="Why neighbors call us",
    ),
    MailerScriptOption(
        id="res_equity",
        template_id="res_equity_gameplan",
        sector="residential",
        label="Residential · Equity Snapshot",
        summary="Shares a concise equity update and invites a pressure-free planning chat.",
        value_props_title="Equity talking points",
    ),
    MailerScriptOption(
        id="res_private",
        template_id="res_private_sale",
        sector="residential",
        label="Residential · Private Buyer Match",
        summary="Introduces a pre-approved buyer who wants a discreet conversation about the property.",
        value_props_title="Why this buyer fits",
    ),
    MailerScriptOption(
        id="com_ops",
        template_id="com_local_ops",
        sector="commercial",
        label="Commercial · Operator Outreach",
        summary="Connects with owners about operators seeking space and flexible deal structures.",
        value_props_title="Operator highlights",
    ),
    MailerScriptOption(
        id="com_pulse",
        template_id="com_market_pulse",
        sector="commercial",
        label="Commercial · Market Pulse",
        summary="Provides a quick market brief and offer to tailor comps or incentive packages.",
        value_props_title="Current demand signals",
    ),
    MailerScriptOption(
        id="com_capital",
        template_id="com_investor_brief",
        sector="commercial",
        label="Commercial · Investor Brief",
        summary="Alerts owners that private capital is ready for acquisitions or recapitalizations.",
        value_props_title="Capital advantages",
    ),
]

_MAILER_SCRIPT_INDEX: Dict[str, MailerScriptOption] = {
    option.id: option for option in MAILER_SCRIPT_OPTIONS
}


def _build_option_from_template(template: MailerTemplate) -> MailerScriptOption:
    sector_label = (
        "Residential"
        if template.sector == MailerTemplate.SECTOR_RESIDENTIAL
        else "Commercial"
    )
    label = f"{sector_label} · {template.name}"
    summary = template.summary or "Custom workspace letter template."
    value_props_title = template.value_props_title or "Highlights"
    return MailerScriptOption(
        id=f"custom_{template.pk}",
        template_id=f"custom:{template.pk}",
        sector=template.sector,
        label=label,
        summary=summary,
        value_props_title=value_props_title,
        custom_template_id=template.pk,
        prompt_text=(template.prompt_text or None),
    )


def _custom_mailer_options_for_owner(owner: Optional[User]) -> List[MailerScriptOption]:
    if owner is None or not getattr(owner, "pk", None):
        return []
    templates = (
        MailerTemplate.objects.filter(owner=owner, is_active=True)
        .order_by("name")
    )
    return [_build_option_from_template(template) for template in templates]


def get_mailer_script_options(owner: Optional[User] = None) -> List[MailerScriptOption]:
    options = MAILER_SCRIPT_OPTIONS.copy()
    options.extend(_custom_mailer_options_for_owner(owner))
    return options


def _parse_custom_option_id(option_id: str) -> Optional[int]:
    if option_id.startswith("custom_"):
        raw_id = option_id.split("custom_", 1)[1]
    elif option_id.startswith("custom:"):
        raw_id = option_id.split("custom:", 1)[1]
    else:
        return None
    try:
        return int(raw_id)
    except (TypeError, ValueError):
        return None


def _get_mailer_template_for_owner(
    template_pk: int, owner: Optional[User]
) -> MailerTemplate:
    queryset = MailerTemplate.objects.filter(pk=template_pk, is_active=True)
    if owner is not None:
        queryset = queryset.filter(owner=owner)
    template = queryset.first()
    if not template:
        raise KeyError(f"Custom mailer template {template_pk} not found.")
    return template


def get_mailer_script_option(
    option_id: str, owner: Optional[User] = None
) -> MailerScriptOption:
    base_option = _MAILER_SCRIPT_INDEX.get(option_id)
    if base_option:
        return base_option

    template_pk = _parse_custom_option_id(option_id)
    if template_pk is None:
        raise KeyError(f"Mailer script '{option_id}' not found.")

    if owner is None:
        raise KeyError(
            f"Mailer script '{option_id}' not available without an owner context."
        )

    template = _get_mailer_template_for_owner(template_pk, owner)
    return _build_option_from_template(template)


def _render_custom_mailer_template(
    template: MailerTemplate, ctx: MailerFallbackContext
) -> MailerFallback:
    formatter = _SafeDefaultDict(_build_template_context(ctx))
    raw_lines = template.letter_body.splitlines() if template.letter_body else []
    letter_lines = [line.format_map(formatter).strip() for line in raw_lines]
    while letter_lines and not letter_lines[-1]:
        letter_lines.pop()

    value_props: List[str] = []
    for raw_line in template.value_props or []:
        rendered = str(raw_line).format_map(formatter).strip()
        if rendered:
            value_props.append(rendered)

    return MailerFallback(lines=letter_lines, value_props=value_props)


def render_mailer_script(
    option_id: str, ctx: MailerFallbackContext, owner: Optional[User] = None
) -> Dict[str, Any]:
    option = get_mailer_script_option(option_id, owner=owner)
    meta: Dict[str, Optional[str]] = {}

    if option.custom_template_id:
        template = _get_mailer_template_for_owner(option.custom_template_id, owner)
        fallback = _render_custom_mailer_template(template, ctx)
        meta = {"banner_label": None, "headline": None, "subheadline": None}
    else:
        fallback = _render_template_mailer(option.template_id, ctx)
        meta = get_mailer_template_metadata(option.template_id, ctx)

    payload = {
        "prompt_id": option.id,
        "prompt_label": option.label,
        "sector": option.sector,
        "summary": option.summary,
        "banner_label": meta.get("banner_label"),
        "headline": meta.get("headline"),
        "subheadline": meta.get("subheadline"),
        "letter_lines": fallback.lines,
        "value_props": fallback.value_props,
        "value_props_title": option.value_props_title,
        "prompt_text": option.prompt_text or "",
    }
    if option.custom_template_id:
        payload["custom_template_id"] = option.custom_template_id
    return payload


def guess_property_sector(parcel: Any) -> Literal["residential", "commercial"]:
    text_sources = [
        getattr(parcel, "property_category", None),
        getattr(parcel, "property_type", None),
        getattr(parcel, "use_description", None),
        getattr(parcel, "zoning", None),
    ]
    normalized = " ".join(
        str(value).lower() for value in text_sources if isinstance(value, str)
    )
    commercial_keywords = {
        "commercial",
        "industrial",
        "retail",
        "office",
        "mixed use",
        "warehouse",
        "business",
        "manufacturing",
    }
    if any(keyword in normalized for keyword in commercial_keywords):
        return "commercial"
    return "residential"


def _collect_property_facts(
    parcel: Any,
    property_address: str,
    full_address: Optional[str],
    *,
    zillow_zestimate_display: Optional[str],
    assessed_value_display: Optional[str],
) -> List[str]:
    facts: List[str] = []
    if property_address:
        facts.append(f"Property address: {property_address}")
    if full_address and full_address != property_address:
        facts.append(f"Mailing address: {full_address}")

    site_city = _safe_text(getattr(parcel, "site_city", None))
    if site_city:
        facts.append(f"Town or city: {site_city}")

    property_category = _safe_text(getattr(parcel, "property_category", None))
    if property_category:
        facts.append(f"Property category: {property_category}")

    property_type = _safe_text(getattr(parcel, "property_type", None))
    if property_type:
        facts.append(f"Property type: {property_type}")

    style = _safe_text(getattr(parcel, "style", None))
    if style:
        facts.append(f"Building style: {style}")

    units = getattr(parcel, "units", None)
    if units:
        facts.append(f"Number of units: {units}")

    if zillow_zestimate_display:
        facts.append(f"Zillow Zestimate: {zillow_zestimate_display}")
    elif assessed_value_display:
        facts.append(f"Assessed value: {assessed_value_display}")

    equity_value_display = _safe_currency(getattr(parcel, "estimated_equity_value", None))
    if equity_value_display:
        facts.append(f"Estimated equity: {equity_value_display}")

    equity_percent_display = _safe_percentage(getattr(parcel, "equity_percent", None))
    if equity_percent_display:
        facts.append(f"Equity percentage: {equity_percent_display}")

    absentee = _interpret_bool_flag(getattr(parcel, "absentee", None))
    if absentee is not None:
        facts.append(f"Absentee owner: {'Yes' if absentee else 'No'}")

    lot_size = getattr(parcel, "lot_size", None)
    if isinstance(lot_size, (int, float)):
        facts.append(f"Lot size: {lot_size:,.2f}")

    use_code = getattr(parcel, "use_code", None)
    if use_code:
        facts.append(f"Use code: {use_code}")

    return facts


def collect_property_facts(
    parcel: Any,
    property_address: str,
    full_address: Optional[str],
    *,
    zillow_zestimate_display: Optional[str],
    assessed_value_display: Optional[str],
) -> List[str]:
    return _collect_property_facts(
        parcel,
        property_address,
        full_address,
        zillow_zestimate_display=zillow_zestimate_display,
        assessed_value_display=assessed_value_display,
    )
