from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

OUTPUT_FILE = "cimahi_business_data.csv"
OUTPUT_DIR = "generated"
DEFAULT_MAX_OUTPUT_ROWS = 1000

REQUEST_TIMEOUT = 25
REQUEST_RETRIES = 3
RETRY_SLEEP_SECONDS = 2
OVERPASS_FILTER_CHUNK_SIZE = 4
OVERPASS_REQUEST_PAUSE_SECONDS = 0.5
OVERPASS_REQUEST_RETRIES = 1
DEFAULT_MIN_FIXED_LOCATION_SCORE = 2
SCRAPLING_FULL_CRAWL_DEFAULT = False
SCRAPLING_FULL_CRAWL_MAX_PAGES = 12
PRICE_PAGE_TIMEOUT = 10
MAX_PRICE_FOLLOW_LINKS = 3

CIMAHI_LAT = -6.8722
CIMAHI_LON = 107.5423
SEARCH_RADIUS_M = 8000
STRICT_FIXED_LOCATION_ONLY_DEFAULT = True

OVERPASS_ENDPOINTS = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
)

OUTPUT_COLUMNS = (
    "place_id",
    "business_name",
    "address",
    "google_maps",
    "industry",
    "sub_business",
    "instagram",
    "tiktok",
    "linkedin",
    "website",
    "facebook",
    "email",
    "pic_name",
    "whatsapp_or_phone",
    "price_range",
)

EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_PATTERN = re.compile(r"\+?\d[\d\s().-]{7,}\d")

SOCIAL_DOMAINS = {
    "instagram": "instagram.com",
    "facebook": "facebook.com",
    "tiktok": "tiktok.com",
    "linkedin": "linkedin.com",
}

DEFAULT_PRICE_RANGE_BY_SUBBUSINESS = {
    "Restaurant": "$$",
    "Coffee Shop": "$$",
    "Bakery": "$",
    "Bengkel": "$$",
    "Toko Material": "$$$",
    "Salon": "$$",
    "Laundry": "$",
    "Klinik": "$$",
    "Hotel": "$$$",
    "Minimarket": "$",
    "Apotek": "$$",
    "Bank/ATM": "$",
    "SPBU": "$$",
    "Ritel Umum": "$$",
}

PRICE_LINK_KEYWORDS = (
    "menu",
    "harga",
    "price",
    "pricing",
    "produk",
    "product",
    "catalog",
    "katalog",
    "shop",
    "order",
)


@dataclass(frozen=True)
class SearchTarget:
    industry: str
    sub_business: str
    osm_filters: tuple[tuple[str, str], ...]


SEARCH_TARGETS = (
    SearchTarget("Kuliner", "Restaurant", (("amenity", "restaurant"), ("amenity", "fast_food"))),
    SearchTarget("Kuliner", "Coffee Shop", (("amenity", "cafe"),)),
    SearchTarget("Kuliner", "Bakery", (("shop", "bakery"),)),
    SearchTarget("Otomotif", "Bengkel", (("shop", "car_repair"),)),
    SearchTarget("Konstruksi", "Toko Material", (("shop", "hardware"), ("shop", "doityourself"))),
    SearchTarget("Kecantikan", "Salon", (("shop", "beauty"),)),
    SearchTarget("Jasa", "Laundry", (("shop", "laundry"),)),
    SearchTarget("Kesehatan", "Klinik", (("amenity", "clinic"), ("amenity", "doctors"))),
    SearchTarget("Akomodasi", "Hotel", (("tourism", "hotel"),)),
    SearchTarget("Ritel", "Minimarket", (("shop", "convenience"), ("shop", "supermarket"))),
    SearchTarget("Kesehatan", "Apotek", (("amenity", "pharmacy"), ("shop", "chemist"))),
    SearchTarget("Keuangan", "Bank/ATM", (("amenity", "bank"), ("amenity", "atm"))),
    SearchTarget("Otomotif", "SPBU", (("amenity", "fuel"),)),
    SearchTarget(
        "Ritel",
        "Ritel Umum",
        (
            ("shop", "clothes"),
            ("shop", "shoes"),
            ("shop", "electronics"),
            ("shop", "mobile_phone"),
            ("shop", "cosmetics"),
            ("shop", "kiosk"),
            ("shop", "mall"),
            ("shop", "department_store"),
            ("shop", "books"),
            ("shop", "stationery"),
            ("shop", "gift"),
            ("shop", "furniture"),
            ("shop", "computer"),
            ("shop", "household_linen"),
        ),
    ),
)


@dataclass(frozen=True)
class ProjectFiles:
    base_dir: Path
    output_dir: Path
    output_csv: Path
    env_files: tuple[Path, ...]

    @classmethod
    def discover(cls, entry_file: str | Path) -> "ProjectFiles":
        return cls.from_base_dir(Path(entry_file).resolve().parent)

    @classmethod
    def from_base_dir(cls, base_dir: Path) -> "ProjectFiles":
        output_dir = base_dir / OUTPUT_DIR
        return cls(
            base_dir=base_dir,
            output_dir=output_dir,
            output_csv=output_dir / OUTPUT_FILE,
            env_files=(
                base_dir / ".env",
                base_dir / "venv" / ".env",
                Path(".env"),
                Path("venv") / ".env",
            ),
        )


@dataclass(frozen=True)
class RuntimeOptions:
    max_output_rows: int = DEFAULT_MAX_OUTPUT_ROWS
    min_fixed_location_score: int = DEFAULT_MIN_FIXED_LOCATION_SCORE
    strict_fixed_location_only: bool = STRICT_FIXED_LOCATION_ONLY_DEFAULT
    request_timeout: int = REQUEST_TIMEOUT
    request_retries: int = REQUEST_RETRIES
    retry_sleep_seconds: int = RETRY_SLEEP_SECONDS
    overpass_filter_chunk_size: int = OVERPASS_FILTER_CHUNK_SIZE
    overpass_request_pause_seconds: float = OVERPASS_REQUEST_PAUSE_SECONDS
    overpass_request_retries: int = OVERPASS_REQUEST_RETRIES
    scrapling_full_crawl: bool = SCRAPLING_FULL_CRAWL_DEFAULT
    scrapling_full_crawl_max_pages: int = SCRAPLING_FULL_CRAWL_MAX_PAGES
    price_page_timeout: int = PRICE_PAGE_TIMEOUT
    max_price_follow_links: int = MAX_PRICE_FOLLOW_LINKS
    cimahi_lat: float = CIMAHI_LAT
    cimahi_lon: float = CIMAHI_LON
    search_radius_m: int = SEARCH_RADIUS_M

    @classmethod
    def from_env(cls) -> "RuntimeOptions":
        return cls(
            max_output_rows=_env_positive_int("MAX_OUTPUT_ROWS", DEFAULT_MAX_OUTPUT_ROWS),
            min_fixed_location_score=_env_positive_int(
                "MIN_FIXED_LOCATION_SCORE", DEFAULT_MIN_FIXED_LOCATION_SCORE
            ),
            strict_fixed_location_only=_env_truthy(
                "STRICT_FIXED_LOCATION_ONLY", STRICT_FIXED_LOCATION_ONLY_DEFAULT
            ),
            overpass_filter_chunk_size=_env_positive_int(
                "OVERPASS_FILTER_CHUNK_SIZE", OVERPASS_FILTER_CHUNK_SIZE
            ),
            scrapling_full_crawl=_env_truthy(
                "SCRAPLING_FULL_CRAWL", SCRAPLING_FULL_CRAWL_DEFAULT
            ),
            scrapling_full_crawl_max_pages=_env_positive_int(
                "SCRAPLING_FULL_CRAWL_MAX_PAGES", SCRAPLING_FULL_CRAWL_MAX_PAGES
            ),
        )


def load_env_files(filepaths: tuple[Path, ...]) -> None:
    for filepath in filepaths:
        if not filepath or not filepath.exists():
            continue
        with filepath.open("r", encoding="utf-8") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value


def _env_truthy(var_name: str, default: bool = False) -> bool:
    raw_value = os.getenv(var_name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_positive_int(var_name: str, default_value: int) -> int:
    raw_value = os.getenv(var_name, "").strip()
    if not raw_value:
        return default_value
    try:
        parsed = int(raw_value)
    except ValueError:
        parsed = 0
    if parsed > 0:
        return parsed
    print(f"[WARN] {var_name} tidak valid: '{raw_value}', pakai {default_value}.")
    return default_value
