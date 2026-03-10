from __future__ import annotations

import re
from typing import Iterable

from .config import DEFAULT_PRICE_RANGE_BY_SUBBUSINESS


def safe_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_phone(raw_value: str) -> str:
    if not raw_value:
        return ""
    return re.sub(r"[^\d+]", "", raw_value)


def normalize_price(price_value) -> str:
    if price_value is None:
        return ""
    value = safe_text(price_value)
    if not value:
        return ""
    lowered = value.lower()
    if "$" in value:
        count = value.count("$")
        return "$" * max(1, min(count, 4))
    if "murah" in lowered:
        return "$"
    if "mahal" in lowered:
        return "$$$"
    rupiah_values = re.findall(r"(?:rp|idr)\s*([0-9][0-9\.\,]{2,})", lowered)
    numeric_values = []
    for raw in rupiah_values:
        digits = re.sub(r"[^\d]", "", raw)
        if digits:
            numeric_values.append(int(digits))
    if not numeric_values:
        return value
    max_price = max(numeric_values)
    if max_price <= 30000:
        return "$"
    if max_price <= 100000:
        return "$$"
    if max_price <= 300000:
        return "$$$"
    return "$$$$"


def infer_price_from_text(page_text: str) -> str:
    if not page_text:
        return ""
    text = page_text.lower()
    dollar_match = re.search(r"\${1,4}", page_text)
    if dollar_match:
        return normalize_price(dollar_match.group(0))
    if any(keyword in text for keyword in ("budget", "murah", "hemat", "terjangkau")):
        return "$"
    if any(keyword in text for keyword in ("premium", "exclusive", "fine dining")):
        return "$$$"
    rupiah_match = re.findall(r"(?:rp|idr)\s*([0-9][0-9\.\,]{2,})", text)
    values = []
    for raw in rupiah_match:
        digits = re.sub(r"[^\d]", "", raw)
        if digits:
            values.append(int(digits))
    if values:
        return normalize_price(f"Rp {max(values)}")
    return ""


def infer_price_by_sub_business(sub_business: str) -> str:
    return DEFAULT_PRICE_RANGE_BY_SUBBUSINESS.get(sub_business, "$$")


def price_bucket_rank(price_value: str) -> int:
    normalized = normalize_price(price_value)
    if normalized == "$":
        return 1
    if normalized == "$$":
        return 2
    if normalized == "$$$":
        return 3
    if normalized == "$$$$":
        return 4
    return 0


def select_better_price_hint(current_price: str, candidate_price: str) -> str:
    current_normalized = normalize_price(current_price)
    candidate_normalized = normalize_price(candidate_price)
    if price_bucket_rank(candidate_normalized) > price_bucket_rank(current_normalized):
        return candidate_normalized
    return current_normalized


def build_address(tags: dict) -> str:
    address_parts = (
        safe_text(tags.get("addr:housenumber")),
        safe_text(tags.get("addr:street")),
        safe_text(tags.get("addr:suburb")),
        safe_text(tags.get("addr:city")),
        safe_text(tags.get("addr:state")),
        safe_text(tags.get("addr:postcode")),
    )
    compact = ", ".join(part for part in address_parts if part)
    return safe_text(tags.get("addr:full")) or compact


def fixed_location_score(tags: dict, lat, lon) -> int:
    score = 0
    if lat is not None and lon is not None:
        score += 2
    if build_address(tags):
        score += 3
    if safe_text(tags.get("contact:phone")) or safe_text(tags.get("phone")):
        score += 1
    if safe_text(tags.get("website")) or safe_text(tags.get("contact:website")):
        score += 1
    if safe_text(tags.get("brand")) or safe_text(tags.get("operator")):
        score += 1
    return score


def get_lat_lon(element: dict):
    lat = element.get("lat")
    lon = element.get("lon")
    if lat is not None and lon is not None:
        return lat, lon
    center = element.get("center", {})
    if center.get("lat") is not None and center.get("lon") is not None:
        return center["lat"], center["lon"]
    return None, None


def google_maps_from_lat_lon(lat, lon) -> str:
    if lat is None or lon is None:
        return ""
    return f"https://www.google.com/maps?q={lat},{lon}"


def chunk_list(items: Iterable, size: int) -> list:
    items = list(items)
    if size <= 0:
        return [items]
    return [items[idx : idx + size] for idx in range(0, len(items), size)]
