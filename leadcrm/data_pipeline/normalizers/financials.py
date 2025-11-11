"""Financial derivations (monthly payment, propensity proxy stubs)."""

from __future__ import annotations

from typing import Optional, Tuple


def monthly_payment(principal: float, annual_rate: float, term_years: int) -> Optional[float]:
    """Return P&I payment using standard amortization; placeholder handles invalid inputs."""
    if not principal or not annual_rate or not term_years:
        return None
    monthly_rate = annual_rate / 12 / 100
    n = term_years * 12
    if monthly_rate == 0:
        return principal / n
    numerator = monthly_rate * (1 + monthly_rate) ** n
    denominator = (1 + monthly_rate) ** n - 1
    return principal * (numerator / denominator)


def propensity_proxy(*, mortgage_age_years: Optional[float], ltv: Optional[float]) -> Optional[int]:
    """
    Temporary heuristic until we design a fuller model.
    Returns a decile-like score (1-10) where higher indicates more risk.
    """
    if mortgage_age_years is None or ltv is None:
        return None
    score = 0
    if mortgage_age_years > 20:
        score += 3
    elif mortgage_age_years > 10:
        score += 2
    elif mortgage_age_years > 5:
        score += 1
    if ltv > 0.9:
        score += 4
    elif ltv > 0.75:
        score += 3
    elif ltv > 0.6:
        score += 2
    return min(10, max(1, score))
