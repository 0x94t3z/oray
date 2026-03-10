import os
import re
import time
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    from scrapling.fetchers import Fetcher as ScraplingFetcher

    SCRAPLING_AVAILABLE = True
except Exception:
    ScraplingFetcher = None
    SCRAPLING_AVAILABLE = False

try:
    from scrapling.spiders import Response as ScraplingSpiderResponse
    from scrapling.spiders import Spider as ScraplingSpider

    SCRAPLING_SPIDER_AVAILABLE = True
except Exception:
    ScraplingSpider = None
    ScraplingSpiderResponse = None
    SCRAPLING_SPIDER_AVAILABLE = False

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]
OUTPUT_FILE = "cimahi_business_data.csv"
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

# Koordinat pusat Cimahi, Jawa Barat.
CIMAHI_LAT = -6.8722
CIMAHI_LON = 107.5423
SEARCH_RADIUS_M = 8000
STRICT_FIXED_LOCATION_ONLY_DEFAULT = True

SEARCH_TARGETS = [
    {
        "industry": "Kuliner",
        "sub_business": "Restaurant",
        "osm_filters": [('amenity', 'restaurant'), ('amenity', 'fast_food')],
    },
    {"industry": "Kuliner", "sub_business": "Coffee Shop", "osm_filters": [('amenity', 'cafe')]},
    {"industry": "Kuliner", "sub_business": "Bakery", "osm_filters": [('shop', 'bakery')]},
    {"industry": "Otomotif", "sub_business": "Bengkel", "osm_filters": [('shop', 'car_repair')]},
    {
        "industry": "Konstruksi",
        "sub_business": "Toko Material",
        "osm_filters": [('shop', 'hardware'), ('shop', 'doityourself')],
    },
    {"industry": "Kecantikan", "sub_business": "Salon", "osm_filters": [('shop', 'beauty')]},
    {"industry": "Jasa", "sub_business": "Laundry", "osm_filters": [('shop', 'laundry')]},
    {
        "industry": "Kesehatan",
        "sub_business": "Klinik",
        "osm_filters": [('amenity', 'clinic'), ('amenity', 'doctors')],
    },
    {"industry": "Akomodasi", "sub_business": "Hotel", "osm_filters": [('tourism', 'hotel')]},
    {
        "industry": "Ritel",
        "sub_business": "Minimarket",
        "osm_filters": [('shop', 'convenience'), ('shop', 'supermarket')],
    },
    {
        "industry": "Kesehatan",
        "sub_business": "Apotek",
        "osm_filters": [('amenity', 'pharmacy'), ('shop', 'chemist')],
    },
    {
        "industry": "Keuangan",
        "sub_business": "Bank/ATM",
        "osm_filters": [('amenity', 'bank'), ('amenity', 'atm')],
    },
    {
        "industry": "Otomotif",
        "sub_business": "SPBU",
        "osm_filters": [('amenity', 'fuel')],
    },
    {
        "industry": "Ritel",
        "sub_business": "Ritel Umum",
        "osm_filters": [
            ('shop', 'clothes'),
            ('shop', 'shoes'),
            ('shop', 'electronics'),
            ('shop', 'mobile_phone'),
            ('shop', 'cosmetics'),
            ('shop', 'kiosk'),
            ('shop', 'mall'),
            ('shop', 'department_store'),
            ('shop', 'books'),
            ('shop', 'stationery'),
            ('shop', 'gift'),
            ('shop', 'furniture'),
            ('shop', 'computer'),
            ('shop', 'household_linen'),
        ],
    },
]

OUTPUT_COLUMNS = [
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
]

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


def _safe_text(value):
    if value is None:
        return ""
    return str(value).strip()


def env_truthy(var_name, default=False):
    raw_value = os.getenv(var_name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_positive_int(var_name, default_value):
    raw_value = os.getenv(var_name, "").strip()
    if not raw_value:
        return default_value
    try:
        parsed = int(raw_value)
        if parsed > 0:
            return parsed
    except ValueError:
        pass
    print(f"[WARN] {var_name} tidak valid: '{raw_value}', pakai {default_value}.")
    return default_value


def normalize_phone(raw_value):
    if not raw_value:
        return ""
    return re.sub(r"[^\d+]", "", raw_value)


def normalize_price(price_value):
    if price_value is None:
        return ""
    value = _safe_text(price_value)
    if not value:
        return ""
    if "$" in value:
        count = value.count("$")
        return "$" * max(1, min(count, 4))
    if "murah" in value.lower():
        return "$"
    if "mahal" in value.lower():
        return "$$$"
    rupiah_values = re.findall(r"(?:rp|idr)\s*([0-9][0-9\.\,]{2,})", value.lower())
    numeric_values = []
    for raw in rupiah_values:
        digits = re.sub(r"[^\d]", "", raw)
        if digits:
            numeric_values.append(int(digits))
    if numeric_values:
        max_price = max(numeric_values)
        if max_price <= 30000:
            return "$"
        if max_price <= 100000:
            return "$$"
        if max_price <= 300000:
            return "$$$"
        return "$$$$"
    return value


def infer_price_from_text(page_text):
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
    if rupiah_match:
        values = []
        for raw in rupiah_match:
            digits = re.sub(r"[^\d]", "", raw)
            if digits:
                values.append(int(digits))
        if values:
            return normalize_price(f"Rp {max(values)}")

    return ""


def infer_price_by_sub_business(sub_business):
    return DEFAULT_PRICE_RANGE_BY_SUBBUSINESS.get(sub_business, "$$")


def price_bucket_rank(price_value):
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


def select_better_price_hint(current_price, candidate_price):
    current_normalized = normalize_price(current_price)
    candidate_normalized = normalize_price(candidate_price)
    if price_bucket_rank(candidate_normalized) > price_bucket_rank(current_normalized):
        return candidate_normalized
    return current_normalized


def build_address(tags):
    address_parts = [
        _safe_text(tags.get("addr:housenumber")),
        _safe_text(tags.get("addr:street")),
        _safe_text(tags.get("addr:suburb")),
        _safe_text(tags.get("addr:city")),
        _safe_text(tags.get("addr:state")),
        _safe_text(tags.get("addr:postcode")),
    ]
    compact = ", ".join([part for part in address_parts if part])
    return _safe_text(tags.get("addr:full")) or compact


def fixed_location_score(tags, lat, lon):
    score = 0
    if lat is not None and lon is not None:
        score += 2
    if build_address(tags):
        score += 3
    if _safe_text(tags.get("contact:phone")) or _safe_text(tags.get("phone")):
        score += 1
    if _safe_text(tags.get("website")) or _safe_text(tags.get("contact:website")):
        score += 1
    # brand/name operator biasanya lebih valid sebagai entitas usaha tetap.
    if _safe_text(tags.get("brand")) or _safe_text(tags.get("operator")):
        score += 1
    return score


def load_env_files(filepaths):
    for filepath in filepaths:
        if not filepath or not os.path.exists(filepath):
            continue
        with open(filepath, "r", encoding="utf-8") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value


def safe_request_json(session, method, url, context="", retries=REQUEST_RETRIES, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            response = session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (429, 502, 503, 504) and attempt < retries:
                sleep_seconds = RETRY_SLEEP_SECONDS * attempt
                print(
                    f"[WARN] HTTP {status} {context} ({attempt}/{retries}), retry {sleep_seconds}s."
                )
                time.sleep(sleep_seconds)
                continue
            print(f"[WARN] Request gagal {context}: {exc}")
            return {}
        except requests.Timeout:
            if attempt < retries:
                print(f"[WARN] Timeout {context} ({attempt}/{retries}), retry {RETRY_SLEEP_SECONDS}s.")
                time.sleep(RETRY_SLEEP_SECONDS)
            else:
                print(f"[WARN] Timeout {context}.")
        except requests.RequestException as exc:
            print(f"[WARN] Request gagal {context}: {exc}")
            return {}
        except ValueError:
            print(f"[WARN] JSON tidak valid {context}.")
            return {}
    return {}


def overpass_query_for_filters_nearby(filters):
    statements = []
    for key, value in filters:
        statements.append(
            f'node["{key}"="{value}"](around:{SEARCH_RADIUS_M},{CIMAHI_LAT},{CIMAHI_LON});'
        )
        statements.append(
            f'way["{key}"="{value}"](around:{SEARCH_RADIUS_M},{CIMAHI_LAT},{CIMAHI_LON});'
        )
        statements.append(
            f'relation["{key}"="{value}"](around:{SEARCH_RADIUS_M},{CIMAHI_LAT},{CIMAHI_LON});'
        )
    return f"[out:json][timeout:50];({''.join(statements)});out center tags;"


def overpass_query_for_filters_cimahi_area(filters):
    statements = []
    for key, value in filters:
        statements.append(f'node(area.searchArea)["{key}"="{value}"];')
        statements.append(f'way(area.searchArea)["{key}"="{value}"];')
        statements.append(f'relation(area.searchArea)["{key}"="{value}"];')

    # Prioritaskan area administratif level kota/kab.
    return (
        '[out:json][timeout:70];'
        'area["name"="Cimahi"]["boundary"="administrative"]["admin_level"~"6|7|8"]->.searchArea;'
        f"({''.join(statements)});"
        "out center tags;"
    )


def all_unique_filters():
    unique = []
    seen = set()
    for target in SEARCH_TARGETS:
        for filter_pair in target["osm_filters"]:
            if filter_pair in seen:
                continue
            seen.add(filter_pair)
            unique.append(filter_pair)
    return unique


def chunk_list(items, size):
    if size <= 0:
        return [items]
    return [items[idx : idx + size] for idx in range(0, len(items), size)]


def element_key(element):
    osm_type = _safe_text(element.get("type")) or "unknown"
    osm_id = element.get("id")
    if osm_id is not None:
        return f"{osm_type}:{osm_id}"
    tags = element.get("tags", {})
    lat, lon = get_lat_lon(element)
    return f"{osm_type}:{_safe_text(tags.get('name')).lower()}:{lat}:{lon}"


def fetch_overpass_elements_for_filters(session, filters, label):
    primary_query = overpass_query_for_filters_cimahi_area(filters)
    fallback_query = overpass_query_for_filters_nearby(filters)
    query_attempts = [
        ("cimahi-area", primary_query),
        ("nearby-fallback", fallback_query),
    ]
    successful_response_received = False

    for query_name, query in query_attempts:
        for endpoint in OVERPASS_ENDPOINTS:
            payload = safe_request_json(
                session,
                "POST",
                endpoint,
                context=f"Overpass {label} {query_name} via {endpoint}",
                retries=OVERPASS_REQUEST_RETRIES,
                data={"data": query},
            )
            if payload:
                successful_response_received = True
                elements = payload.get("elements") or []
                if elements:
                    print(
                        f"[INFO] Overpass {label} {query_name} via {endpoint} mengembalikan {len(elements)} elemen."
                    )
                    return elements, True
                print(
                    f"[INFO] Overpass {label} {query_name} via {endpoint} kosong, coba endpoint/query lain."
                )
            time.sleep(OVERPASS_REQUEST_PAUSE_SECONDS)
    return [], successful_response_received


def fetch_all_osm_elements(session):
    filters = all_unique_filters()
    chunk_size = OVERPASS_FILTER_CHUNK_SIZE
    chunk_size_raw = os.getenv("OVERPASS_FILTER_CHUNK_SIZE", "").strip()
    if chunk_size_raw:
        try:
            parsed_chunk_size = int(chunk_size_raw)
            if parsed_chunk_size > 0:
                chunk_size = parsed_chunk_size
        except ValueError:
            print(
                f"[WARN] OVERPASS_FILTER_CHUNK_SIZE tidak valid: '{chunk_size_raw}', pakai {OVERPASS_FILTER_CHUNK_SIZE}."
            )

    chunk_size = max(1, chunk_size)
    filter_chunks = chunk_list(filters, chunk_size)
    all_elements = []
    seen = set()
    any_success = False

    for idx, chunk in enumerate(filter_chunks, start=1):
        label = f"chunk-{idx}/{len(filter_chunks)}"
        print(f"[INFO] Overpass query {label} ({len(chunk)} filters)")
        chunk_elements, chunk_ok = fetch_overpass_elements_for_filters(session, chunk, label)
        if chunk_ok:
            any_success = True
        print(f"[INFO] Overpass hasil {label}: {len(chunk_elements)} elemen")
        for element in chunk_elements:
            key = element_key(element)
            if key in seen:
                continue
            seen.add(key)
            all_elements.append(element)

    return all_elements, any_success


def get_target_for_element(tags):
    for target in SEARCH_TARGETS:
        for key, value in target["osm_filters"]:
            if _safe_text(tags.get(key)) == value:
                return target
    return None


def get_lat_lon(element):
    lat = element.get("lat")
    lon = element.get("lon")
    if lat is not None and lon is not None:
        return lat, lon
    center = element.get("center", {})
    if center.get("lat") is not None and center.get("lon") is not None:
        return center["lat"], center["lon"]
    return None, None


def google_maps_from_lat_lon(lat, lon):
    if lat is None or lon is None:
        return ""
    return f"https://www.google.com/maps?q={lat},{lon}"


def extract_social_from_text_and_links(page_text, links):
    result = {
        "instagram": "",
        "tiktok": "",
        "linkedin": "",
        "facebook": "",
        "email": "",
        "whatsapp": "",
        "pic_name": "",
        "price_hint": "",
    }

    email_match = EMAIL_PATTERN.findall(page_text)
    if email_match:
        result["email"] = email_match[0]

    for href in links:
        href_lower = href.lower()
        if not result["email"] and href_lower.startswith("mailto:"):
            candidate = href.split(":", 1)[1].split("?", 1)[0].strip()
            if EMAIL_PATTERN.fullmatch(candidate):
                result["email"] = candidate

        for field_name, domain in SOCIAL_DOMAINS.items():
            if not result[field_name] and domain in href_lower:
                result[field_name] = href

        if "wa.me/" in href_lower or "api.whatsapp.com" in href_lower or "whatsapp" in href_lower:
            number_match = re.search(r"(?:phone=|wa\.me/)(\+?\d+)", href_lower)
            if number_match:
                result["whatsapp"] = normalize_phone(number_match.group(1))
            elif not result["whatsapp"]:
                result["whatsapp"] = href

    if not result["whatsapp"]:
        phone_match = PHONE_PATTERN.search(page_text)
        if phone_match:
            result["whatsapp"] = normalize_phone(phone_match.group(0))

    owner_match = re.search(
        r"(?:owner|founder|contact person|pic)\s*[:\-]\s*([A-Z][A-Za-z .,'-]{2,40})",
        page_text,
        re.IGNORECASE,
    )
    if owner_match:
        result["pic_name"] = owner_match.group(1).strip()

    result["price_hint"] = infer_price_from_text(page_text)
    return result


def html_from_scrapling(url):
    if not SCRAPLING_AVAILABLE:
        return ""
    try:
        page = ScraplingFetcher.get(url)
    except Exception:
        return ""

    return html_from_scrapling_page(page)


def html_from_scrapling_page(page):
    for attr in ("html_content", "html", "source", "content", "text"):
        candidate = getattr(page, attr, None)
        if callable(candidate):
            try:
                candidate = candidate()
            except Exception:
                candidate = None
        if isinstance(candidate, str) and "<html" in candidate.lower():
            return candidate

    fallback = str(page)
    if "<html" in fallback.lower():
        return fallback
    return ""


def extract_text_and_links_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    links = [a.get("href", "").strip() for a in soup.find_all("a", href=True)]
    return text, links


def candidate_price_urls(base_url, links):
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc.lower()
    candidates = []
    seen = set()

    for href in links:
        href = _safe_text(href)
        if not href:
            continue

        absolute_url = urljoin(base_url, href)
        parsed_candidate = urlparse(absolute_url)
        if parsed_candidate.scheme not in {"http", "https"}:
            continue
        if parsed_candidate.netloc.lower() != base_domain:
            continue

        lowered = absolute_url.lower()
        if not any(keyword in lowered for keyword in PRICE_LINK_KEYWORDS):
            continue

        normalized_url = absolute_url.split("#", 1)[0]
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        candidates.append(normalized_url)

    return candidates[:MAX_PRICE_FOLLOW_LINKS]


def extract_social_from_website_crawl_with_spider(website_url):
    result = {
        "instagram": "",
        "tiktok": "",
        "linkedin": "",
        "facebook": "",
        "email": "",
        "whatsapp": "",
        "pic_name": "",
        "price_hint": "",
    }
    if not SCRAPLING_SPIDER_AVAILABLE:
        return result

    normalized_url = website_url
    if normalized_url and not normalized_url.startswith(("http://", "https://")):
        normalized_url = f"https://{normalized_url}"

    parsed = urlparse(normalized_url)
    if not parsed.netloc:
        return result

    domain = parsed.netloc.split(":", 1)[0].lower()
    max_pages = env_positive_int("SCRAPLING_FULL_CRAWL_MAX_PAGES", SCRAPLING_FULL_CRAWL_MAX_PAGES)

    class WebsiteSpider(ScraplingSpider):
        name = "website_enrichment"
        start_urls = [normalized_url]
        allowed_domains = {domain}
        concurrent_requests = 4
        concurrent_requests_per_domain = 2
        download_delay = 0.1

        def __init__(self):
            super().__init__()
            self.pages_collected = 0

        async def parse(self, response: ScraplingSpiderResponse):
            if self.pages_collected >= max_pages:
                return

            self.pages_collected += 1
            current_url = _safe_text(getattr(response, "url", "")) or normalized_url

            html = html_from_scrapling_page(response)
            if html:
                page_text, page_links = extract_text_and_links_from_html(html)
                yield {"text": page_text, "links": page_links}

            if self.pages_collected >= max_pages:
                return

            for href in response.css("a::attr(href)").getall():
                href = _safe_text(href)
                if not href:
                    continue
                next_url = urljoin(current_url, href)
                if not next_url:
                    continue
                yield response.follow(next_url, callback=self.parse)

    try:
        crawl_result = WebsiteSpider().start()
    except Exception:
        return result

    combined_text_parts = []
    combined_links = []
    for item in getattr(crawl_result, "items", []):
        item_text = _safe_text(item.get("text"))
        if item_text:
            combined_text_parts.append(item_text)
        item_links = item.get("links") or []
        combined_links.extend([_safe_text(link) for link in item_links if _safe_text(link)])

    if not combined_text_parts and not combined_links:
        return result

    unique_links = []
    seen_links = set()
    for link in combined_links:
        if link in seen_links:
            continue
        seen_links.add(link)
        unique_links.append(link)

    merged_text = " ".join(combined_text_parts)
    return extract_social_from_text_and_links(merged_text, unique_links)


def extract_social_and_contact_from_website(session, website_url):
    result = {
        "instagram": "",
        "tiktok": "",
        "linkedin": "",
        "facebook": "",
        "email": "",
        "whatsapp": "",
        "pic_name": "",
        "price_hint": "",
    }
    if not website_url:
        return result

    if not website_url.startswith(("http://", "https://")):
        website_url = f"https://{website_url}"

    if env_truthy("SCRAPLING_FULL_CRAWL", SCRAPLING_FULL_CRAWL_DEFAULT):
        crawled = extract_social_from_website_crawl_with_spider(website_url)
        if any(crawled.values()):
            return crawled

    html = html_from_scrapling(website_url)
    if not html:
        try:
            response = session.get(website_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            html = response.text
        except requests.RequestException:
            return result

    text, links = extract_text_and_links_from_html(html)
    result = extract_social_from_text_and_links(text, links)

    if result["price_hint"]:
        return result

    for candidate_url in candidate_price_urls(website_url, links):
        try:
            response = session.get(candidate_url, timeout=PRICE_PAGE_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException:
            continue

        candidate_text, candidate_links = extract_text_and_links_from_html(response.text)
        candidate_result = extract_social_from_text_and_links(candidate_text, candidate_links)

        if not result["instagram"]:
            result["instagram"] = candidate_result["instagram"]
        if not result["tiktok"]:
            result["tiktok"] = candidate_result["tiktok"]
        if not result["linkedin"]:
            result["linkedin"] = candidate_result["linkedin"]
        if not result["facebook"]:
            result["facebook"] = candidate_result["facebook"]
        if not result["email"]:
            result["email"] = candidate_result["email"]
        if not result["whatsapp"]:
            result["whatsapp"] = candidate_result["whatsapp"]
        if not result["pic_name"]:
            result["pic_name"] = candidate_result["pic_name"]
        result["price_hint"] = select_better_price_hint(
            result["price_hint"], candidate_result["price_hint"]
        )

        if result["price_hint"]:
            break

    return result


def social_from_osm_tags(tags):
    result = {
        "instagram": _safe_text(tags.get("contact:instagram")) or _safe_text(tags.get("instagram")),
        "tiktok": _safe_text(tags.get("contact:tiktok")) or _safe_text(tags.get("tiktok")),
        "linkedin": _safe_text(tags.get("contact:linkedin")) or _safe_text(tags.get("linkedin")),
        "facebook": _safe_text(tags.get("contact:facebook")) or _safe_text(tags.get("facebook")),
        "email": _safe_text(tags.get("contact:email")) or _safe_text(tags.get("email")),
        "pic_name": "",
        "whatsapp": _safe_text(tags.get("contact:whatsapp")) or _safe_text(tags.get("whatsapp")),
    }

    # Beberapa tag sosial hanya ditaruh sebagai URL website.
    website = _safe_text(tags.get("website")) or _safe_text(tags.get("contact:website"))
    if website:
        lower = website.lower()
        for field_name, domain in SOCIAL_DOMAINS.items():
            if not result[field_name] and domain in lower:
                result[field_name] = website

    if result["whatsapp"]:
        result["whatsapp"] = normalize_phone(result["whatsapp"])
    return result


def build_row(element, target, website_social):
    tags = element.get("tags", {})
    lat, lon = get_lat_lon(element)
    maps_link = google_maps_from_lat_lon(lat, lon)

    osm_social = social_from_osm_tags(tags)
    merged_social = {
        "instagram": osm_social["instagram"] or website_social["instagram"],
        "tiktok": osm_social["tiktok"] or website_social["tiktok"],
        "linkedin": osm_social["linkedin"] or website_social["linkedin"],
        "facebook": osm_social["facebook"] or website_social["facebook"],
        "email": osm_social["email"] or website_social["email"],
        "pic_name": website_social["pic_name"],
        "whatsapp": osm_social["whatsapp"] or website_social["whatsapp"],
    }

    phone = (
        _safe_text(tags.get("contact:phone"))
        or _safe_text(tags.get("phone"))
        or _safe_text(tags.get("contact:mobile"))
    )
    whatsapp_or_phone = merged_social["whatsapp"] or normalize_phone(phone)

    website = _safe_text(tags.get("website")) or _safe_text(tags.get("contact:website"))
    price = (
        _safe_text(tags.get("price"))
        or _safe_text(tags.get("price:range"))
        or _safe_text(tags.get("charge"))
    )
    price_range = normalize_price(price)
    if not price_range:
        price_range = normalize_price(website_social.get("price_hint", ""))
    if not price_range:
        price_range = infer_price_by_sub_business(target["sub_business"])

    return {
        "place_id": "",
        "business_name": _safe_text(tags.get("name")),
        "address": build_address(tags),
        "google_maps": maps_link,
        "industry": target["industry"],
        "sub_business": target["sub_business"],
        "instagram": merged_social["instagram"],
        "tiktok": merged_social["tiktok"],
        "linkedin": merged_social["linkedin"],
        "website": website,
        "facebook": merged_social["facebook"],
        "email": merged_social["email"],
        "pic_name": merged_social["pic_name"],
        "whatsapp_or_phone": whatsapp_or_phone,
        "price_range": price_range,
    }


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    load_env_files(
        [
            os.path.join(base_dir, ".env"),
            os.path.join(base_dir, "venv", ".env"),
            ".env",
            os.path.join("venv", ".env"),
        ]
    )

    max_output_rows = env_positive_int("MAX_OUTPUT_ROWS", DEFAULT_MAX_OUTPUT_ROWS)
    min_fixed_location_score = env_positive_int(
        "MIN_FIXED_LOCATION_SCORE", DEFAULT_MIN_FIXED_LOCATION_SCORE
    )
    strict_fixed_location_only = env_truthy(
        "STRICT_FIXED_LOCATION_ONLY", STRICT_FIXED_LOCATION_ONLY_DEFAULT
    )

    if SCRAPLING_AVAILABLE:
        if env_truthy("SCRAPLING_FULL_CRAWL", SCRAPLING_FULL_CRAWL_DEFAULT):
            if SCRAPLING_SPIDER_AVAILABLE:
                max_pages = env_positive_int(
                    "SCRAPLING_FULL_CRAWL_MAX_PAGES", SCRAPLING_FULL_CRAWL_MAX_PAGES
                )
                print(
                    f"[INFO] Scrapling Spider aktif: full-crawl enrichment hingga {max_pages} halaman/site."
                )
            else:
                print("[WARN] SCRAPLING_FULL_CRAWL aktif tapi modul spider tidak tersedia.")
                print("[INFO] Scrapling Fetcher tetap dipakai untuk enrichment 1 halaman.")
        else:
            print("[INFO] Scrapling terdeteksi: website enrichment pakai Scrapling.")
    else:
        print("[INFO] Scrapling belum terpasang: fallback ke requests + BeautifulSoup.")

    session = requests.Session()
    rows = []
    seen_keys = set()
    skipped_missing_name = 0
    skipped_low_fixed_score = 0
    skipped_duplicate = 0
    print("[INFO] Scraping OSM: all targets (chunked query)")
    elements, fetch_ok = fetch_all_osm_elements(session)
    print(f"[INFO] Total elemen OSM terkumpul: {len(elements)}")

    for element in elements:
        tags = element.get("tags", {})
        target = get_target_for_element(tags)
        if target is None:
            continue

        business_name = _safe_text(tags.get("name"))
        if not business_name:
            skipped_missing_name += 1
            continue

        lat, lon = get_lat_lon(element)
        fixed_score = fixed_location_score(tags, lat, lon)
        if strict_fixed_location_only and fixed_score < min_fixed_location_score:
            skipped_low_fixed_score += 1
            continue

        dedupe_key = f"{business_name.lower()}|{build_address(tags).lower()}"
        if dedupe_key in seen_keys:
            skipped_duplicate += 1
            continue
        seen_keys.add(dedupe_key)

        website = _safe_text(tags.get("website")) or _safe_text(tags.get("contact:website"))
        website_social = (
            extract_social_and_contact_from_website(session, website)
            if website
            else {
                "instagram": "",
                "tiktok": "",
                "linkedin": "",
                "facebook": "",
                "email": "",
                "whatsapp": "",
                "pic_name": "",
                "price_hint": "",
            }
        )

        row = build_row(element, target, website_social)
        row["_fixed_score"] = fixed_score
        rows.append(row)
        time.sleep(0.1)
        if len(rows) >= max_output_rows * 2:
            break

    df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    if not df.empty:
        df["_fixed_score"] = [row.get("_fixed_score", 0) for row in rows[: len(df)]]
        df.drop_duplicates(subset=["business_name", "address"], inplace=True)
        df.sort_values(by=["_fixed_score", "business_name"], ascending=[False, True], inplace=True)
        if len(df) > 1000:
            print("[WARN] Data lebih dari 1000, disimpan hanya 1000 baris pertama.")
            df = df.head(1000).copy()
        df["place_id"] = range(1, len(df) + 1)
        df.drop(columns=["_fixed_score"], inplace=True, errors="ignore")

    if not fetch_ok:
        print("[ERROR] Semua endpoint Overpass gagal. Update dianggap tidak lengkap.")
        if os.path.exists(OUTPUT_FILE):
            print(f"[WARN] CSV lama dipertahankan: {OUTPUT_FILE}")
        else:
            print("[WARN] Belum ada CSV lama untuk dipertahankan.")
        print("Done scraping!")
        print("Total data:", len(df))
        return

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"[INFO] CSV diperbarui: {OUTPUT_FILE}")
    print(
        "[INFO] Ringkasan filter:"
        f" tanpa nama={skipped_missing_name},"
        f" skor lokasi rendah={skipped_low_fixed_score},"
        f" duplikat={skipped_duplicate}"
    )
    if df.empty:
        print("[INFO] Hasil scraping kosong. CSV disimpan apa adanya.")
        if skipped_low_fixed_score > 0:
            print(
                "[WARN] Banyak data terbuang oleh filter lokasi tetap."
                f" Coba set MIN_FIXED_LOCATION_SCORE={max(1, min_fixed_location_score - 1)}"
                " atau STRICT_FIXED_LOCATION_ONLY=false di .env."
            )
    elif len(df) < 1000:
        print(f"[WARN] Data terbaru hanya {len(df)} baris (target 1000).")
    print("Done scraping!")
    print("Total data:", len(df))


if __name__ == "__main__":
    main()
