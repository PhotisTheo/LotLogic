"""Hybrid hedonic + comparable parcel valuation engine."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from statistics import median
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

from .services import (
    _classify_use_code,
    _clean_string,
    _normalize_loc_id,
    _parse_float_value,
    _parse_massgis_date,
)


DEFAULT_LOOKBACK_DAYS = 365
MIN_SALE_PRICE = 100.0
MAX_SALE_PRICE = 25_000_000.0
FEATURE_NAMES = [
    "bias",
    "log_total_value",
    "log_living_area",
    "log_lot_size",
    "year_score",
    "style_psf",
    "category_psf",
]
CATEGORY_CODES = {
    "Residential": 1.0,
    "Commercial": 1.35,
    "Industrial": 1.5,
    "Agricultural": 1.15,
    "Forest": 1.1,
    "Mixed": 1.25,
    "Exempt": 0.85,
    "Other": 1.0,
    "Unknown": 1.0,
}


@dataclass
class CleanedParcelRecord:
    loc_id: str
    total_value: Optional[float]
    land_value: Optional[float]
    building_value: Optional[float]
    lot_size: Optional[float]
    living_area: Optional[float]
    style: Optional[str]
    property_category: str
    zoning: Optional[str]
    year_built: Optional[int]
    sale_price: Optional[float]
    sale_date: Optional[datetime]

    def sale_price_per_sqft(self) -> Optional[float]:
        if self.sale_price and self.living_area and self.living_area > 0:
            return self.sale_price / self.living_area
        return None


@dataclass
class ComparableSummary:
    loc_id: str
    sale_price: float
    sale_date: datetime
    living_area: Optional[float]
    lot_size: Optional[float]
    style: Optional[str]
    psf: Optional[float]
    weight: float
    distance: float

    def as_payload(self) -> Dict[str, object]:
        return {
            "loc_id": self.loc_id,
            "sale_price": round(self.sale_price, 2),
            "sale_date": self.sale_date.date().isoformat(),
            "living_area": self.living_area,
            "lot_size": self.lot_size,
            "style": self.style,
            "psf": round(self.psf, 2) if self.psf is not None else None,
            "weight": round(self.weight, 4),
            "distance": round(self.distance, 4),
        }


@dataclass
class ValuationStats:
    median_total_value: float
    median_living_area: float
    median_lot_size: float
    median_year_built: float
    global_psf: Optional[float]
    category_psf: Dict[str, float]
    style_psf: Dict[str, float]

    def style_price(self, style: Optional[str]) -> Optional[float]:
        if style:
            key = style.lower()
            if key in self.style_psf:
                return self.style_psf[key]
        return self.global_psf

    def category_price(self, category: Optional[str]) -> Optional[float]:
        if category and category in self.category_psf:
            return self.category_psf[category]
        return self.global_psf


@dataclass
class HedonicModel:
    coefficients: Sequence[float]
    r2: float


@dataclass
class ParcelValuationResult:
    loc_id: str
    market_value: Optional[float]
    market_value_per_sqft: Optional[float]
    comparable_value: Optional[float]
    comparable_count: int
    comparable_avg_psf: Optional[float]
    hedonic_value: Optional[float]
    hedonic_r2: Optional[float]
    confidence: Optional[float]
    comps: List[ComparableSummary]
    inputs: Dict[str, Optional[float]]


class ParcelValuationEngine:
    """Computes parcel market values using a hybrid hedonic + comp model."""

    def __init__(
        self,
        *,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        target_comp_count: int = 5,
        regularization: float = 0.35,
    ) -> None:
        self.lookback_days = max(30, lookback_days)
        self.target_comp_count = max(3, target_comp_count)
        self.regularization = max(0.05, regularization)

    def build_clean_records(
        self, raw_records: Iterable[Dict[str, object]]
    ) -> List[CleanedParcelRecord]:
        cleaned: List[CleanedParcelRecord] = []
        for record in raw_records:
            raw_loc = (
                record.get("LOC_ID")
                or record.get("PAR_ID")
                or record.get("PROP_ID")
            )
            loc_id = _normalize_loc_id(raw_loc)
            if not loc_id:
                continue

            total_value = _parse_float_value(
                record.get("MARKET_VALUE") or record.get("TOTAL_VAL") or record.get("TOTAL_VALUE")
            )
            land_value = _parse_float_value(record.get("LAND_VAL") or record.get("LAND_VALUE"))
            building_value = _parse_float_value(record.get("BLDG_VAL") or record.get("BLDG_VALUE"))
            lot_size = _parse_float_value(record.get("LOT_SIZE") or record.get("LAND_SF"))
            living_area = _parse_float_value(record.get("BLD_AREA") or record.get("LIVING_AREA") or record.get("LIV_AREA"))
            style = _clean_string(record.get("STYLE"))
            property_category = _classify_use_code(record.get("USE_CODE") or record.get("LUC"))
            zoning = _clean_string(record.get("ZONING"))
            year_raw = _parse_float_value(record.get("YEAR_BUILT") or record.get("YR_BUILT"))
            year_built = None
            if year_raw:
                year_int = int(year_raw)
                if 1600 <= year_int <= datetime.utcnow().year + 2:
                    year_built = year_int
            sale_price = _parse_float_value(record.get("LS_PRICE") or record.get("SALE_PRICE"))
            sale_date = _parse_massgis_date(record.get("LS_DATE") or record.get("SALE_DATE"))

            cleaned.append(
                CleanedParcelRecord(
                    loc_id=loc_id,
                    total_value=total_value,
                    land_value=land_value,
                    building_value=building_value,
                    lot_size=lot_size,
                    living_area=living_area,
                    style=style,
                    property_category=property_category,
                    zoning=zoning,
                    year_built=year_built,
                    sale_price=sale_price,
                    sale_date=sale_date,
                )
            )

        return cleaned

    def compute(self, records: Sequence[CleanedParcelRecord]) -> Tuple[List[ParcelValuationResult], Optional[HedonicModel], ValuationStats]:
        recent_sales = self._recent_sales(records)
        stats = self._build_stats(recent_sales)
        hedonic_model = self._fit_model(recent_sales, stats)

        valuations: List[ParcelValuationResult] = []
        for record in records:
            comps = self._select_comparables(record, recent_sales)
            comp_value, comp_avg_psf = self._compute_comparable_value(record, comps)
            hedonic_value = self._predict_value(record, stats, hedonic_model)
            final_value, confidence = self._blend_values(
                record,
                comp_value,
                len(comps),
                hedonic_value,
                hedonic_model.r2 if hedonic_model else None,
            )

            market_psf = None
            if final_value and record.living_area and record.living_area > 0:
                market_psf = final_value / record.living_area
            elif comp_avg_psf:
                market_psf = comp_avg_psf

            valuations.append(
                ParcelValuationResult(
                    loc_id=record.loc_id,
                    market_value=final_value,
                    market_value_per_sqft=market_psf,
                    comparable_value=comp_value,
                    comparable_count=len(comps),
                    comparable_avg_psf=comp_avg_psf,
                    hedonic_value=hedonic_value,
                    hedonic_r2=hedonic_model.r2 if hedonic_model else None,
                    confidence=confidence,
                    comps=comps,
                    inputs={
                        "total_value": record.total_value,
                        "living_area": record.living_area,
                        "lot_size": record.lot_size,
                        "year_built": record.year_built,
                    },
                )
            )

        return valuations, hedonic_model, stats

    def _recent_sales(self, records: Sequence[CleanedParcelRecord]) -> List[CleanedParcelRecord]:
        cutoff = datetime.utcnow() - timedelta(days=self.lookback_days)
        sales: List[CleanedParcelRecord] = []
        for record in records:
            if not record.sale_price or not record.sale_date:
                continue
            if record.sale_price < MIN_SALE_PRICE or record.sale_price > MAX_SALE_PRICE:
                continue
            if record.sale_date < cutoff:
                continue
            sales.append(record)
        return sales

    def _build_stats(self, sales: Sequence[CleanedParcelRecord]) -> ValuationStats:
        total_values = [sale.total_value for sale in sales if sale.total_value]
        living_areas = [sale.living_area for sale in sales if sale.living_area]
        lot_sizes = [sale.lot_size for sale in sales if sale.lot_size]
        years = [sale.year_built for sale in sales if sale.year_built]

        psf_values = [sale.sale_price_per_sqft() for sale in sales if sale.sale_price_per_sqft()]
        global_psf = float(median(psf_values)) if psf_values else None

        category_psf: Dict[str, float] = {}
        style_psf: Dict[str, float] = {}

        if sales:
            category_groups: Dict[str, List[float]] = {}
            style_groups: Dict[str, List[float]] = {}
            for sale in sales:
                psf = sale.sale_price_per_sqft()
                if psf is None:
                    continue
                category_groups.setdefault(sale.property_category, []).append(psf)
                if sale.style:
                    style_groups.setdefault(sale.style.lower(), []).append(psf)

            category_psf = {key: float(median(values)) for key, values in category_groups.items() if values}
            style_psf = {key: float(median(values)) for key, values in style_groups.items() if values}

        return ValuationStats(
            median_total_value=float(median(total_values)) if total_values else 0.0,
            median_living_area=float(median(living_areas)) if living_areas else 0.0,
            median_lot_size=float(median(lot_sizes)) if lot_sizes else 0.0,
            median_year_built=float(median(years)) if years else 1980.0,
            global_psf=global_psf,
            category_psf=category_psf,
            style_psf=style_psf,
        )

    def _fit_model(
        self,
        sales: Sequence[CleanedParcelRecord],
        stats: ValuationStats,
    ) -> Optional[HedonicModel]:
        if len(sales) < len(FEATURE_NAMES) + 2:
            return None

        feature_rows: List[List[float]] = []
        prices: List[float] = []
        for sale in sales:
            features = self._build_features(sale, stats)
            if features is None:
                continue
            feature_rows.append(features)
            prices.append(float(sale.sale_price))

        if len(feature_rows) < len(FEATURE_NAMES) + 1:
            return None

        X = np.array(feature_rows, dtype=float)
        y = np.array(prices, dtype=float)

        xtx = X.T @ X
        ridge = self.regularization * np.eye(len(FEATURE_NAMES))
        try:
            coefficients = np.linalg.solve(xtx + ridge, X.T @ y)
        except np.linalg.LinAlgError:
            return None

        predictions = X @ coefficients
        ss_total = float(np.sum((y - y.mean()) ** 2)) if len(y) > 1 else 0.0
        ss_res = float(np.sum((y - predictions) ** 2))
        r2 = 0.0
        if ss_total > 0:
            r2 = max(0.0, min(0.999, 1 - (ss_res / ss_total)))

        return HedonicModel(coefficients=coefficients.tolist(), r2=r2)

    def _build_features(
        self, record: CleanedParcelRecord, stats: ValuationStats
    ) -> Optional[List[float]]:
        total_val = record.total_value or stats.median_total_value or 1.0
        living_area = record.living_area or stats.median_living_area or 1.0
        lot_size = record.lot_size or stats.median_lot_size or 1.0
        year = record.year_built or stats.median_year_built
        style_price = stats.style_price(record.style) or stats.global_psf or 100.0
        category_price = stats.category_price(record.property_category) or stats.global_psf or 100.0

        return [
            1.0,
            math.log1p(total_val),
            math.log1p(living_area),
            math.log1p(lot_size),
            (year - 1950.0) / 100.0,
            math.log1p(style_price),
            math.log1p(category_price),
        ]

    def _predict_value(
        self,
        record: CleanedParcelRecord,
        stats: ValuationStats,
        model: Optional[HedonicModel],
    ) -> Optional[float]:
        if not model:
            return None
        features = self._build_features(record, stats)
        if features is None:
            return None
        return float(sum(weight * coef for weight, coef in zip(features, model.coefficients)))

    def _select_comparables(
        self,
        target: CleanedParcelRecord,
        candidates: Sequence[CleanedParcelRecord],
    ) -> List[ComparableSummary]:
        relevant = [
            record for record in candidates if record.loc_id != target.loc_id and record.property_category == target.property_category
        ] or list(candidates)

        def distance(record: CleanedParcelRecord) -> float:
            area_diff = _relative_gap(record.living_area, target.living_area, 0.5)
            lot_diff = _relative_gap(record.lot_size, target.lot_size, 0.5) * 0.7
            style_penalty = 0.2 if record.style and target.style and record.style != target.style else 0.0
            zoning_penalty = 0.1 if record.zoning and target.zoning and record.zoning != target.zoning else 0.0
            date_penalty = 0.0
            if record.sale_date:
                days_old = (datetime.utcnow() - record.sale_date).days
                date_penalty = min(0.4, days_old / self.lookback_days)
            return area_diff + lot_diff + style_penalty + zoning_penalty + date_penalty

        ranked = sorted(relevant, key=distance)[: self.target_comp_count * 2]
        comps: List[ComparableSummary] = []
        for record in ranked[: self.target_comp_count]:
            psf = record.sale_price_per_sqft()
            dist = distance(record)
            weight = 1.0 / (1.0 + dist)
            comps.append(
                ComparableSummary(
                    loc_id=record.loc_id,
                    sale_price=float(record.sale_price or 0.0),
                    sale_date=record.sale_date or datetime.utcnow(),
                    living_area=record.living_area,
                    lot_size=record.lot_size,
                    style=record.style,
                    psf=psf,
                    weight=weight,
                    distance=dist,
                )
            )

        return comps

    def _compute_comparable_value(
        self,
        target: CleanedParcelRecord,
        comps: Sequence[ComparableSummary],
    ) -> Tuple[Optional[float], Optional[float]]:
        if not comps:
            return None, None

        weighted_prices: List[float] = []
        weighted_psf: List[float] = []
        psf_weights: List[float] = []
        weights: List[float] = []

        for comp in comps:
            if comp.sale_price <= 0:
                continue
            scaled_price = comp.sale_price
            if target.living_area and comp.living_area and comp.living_area > 0:
                ratio = max(0.5, min(1.5, target.living_area / comp.living_area))
                scaled_price *= ratio
            weighted_prices.append(scaled_price * comp.weight)
            weights.append(comp.weight)
            if comp.psf:
                weighted_psf.append(comp.psf * comp.weight)
                psf_weights.append(comp.weight)

        if not weights or sum(weights) == 0:
            return None, None

        comp_value = sum(weighted_prices) / sum(weights)
        comp_avg_psf = None
        if weighted_psf and psf_weights:
            comp_avg_psf = sum(weighted_psf) / sum(psf_weights)

        return comp_value, comp_avg_psf

    def _blend_values(
        self,
        record: CleanedParcelRecord,
        comp_value: Optional[float],
        comp_count: int,
        hedonic_value: Optional[float],
        hedonic_r2: Optional[float],
    ) -> Tuple[Optional[float], Optional[float]]:
        if comp_value is None and hedonic_value is None:
            return None, None
        if comp_value is None:
            return hedonic_value, max(0.25, hedonic_r2 or 0.2)
        if hedonic_value is None:
            return comp_value, min(0.9, 0.4 + 0.1 * comp_count)

        comp_strength = min(1.0, comp_count / self.target_comp_count)
        model_strength = hedonic_r2 or 0.0

        comp_weight = 0.45 + 0.35 * comp_strength
        model_weight = 0.55 - 0.35 * comp_strength
        model_weight *= 0.5 + 0.5 * model_strength

        total = comp_weight + model_weight
        comp_weight /= total
        model_weight /= total

        blended = comp_weight * comp_value + model_weight * hedonic_value
        confidence = min(0.95, 0.5 * comp_strength + 0.3 * model_strength + 0.2 * _coverage_score(record))
        return blended, confidence


def _relative_gap(a: Optional[float], b: Optional[float], cap: float) -> float:
    if not a or not b or a <= 0 or b <= 0:
        return cap
    return min(cap, abs(a - b) / max(a, b))


def _coverage_score(record: CleanedParcelRecord) -> float:
    total = 0
    filled = 0
    for value in (record.total_value, record.living_area, record.lot_size, record.year_built):
        total += 1
        if value:
            filled += 1
    return filled / total if total else 0.0
