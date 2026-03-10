from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .config import EMAIL_PATTERN, PHONE_PATTERN, PRICE_LINK_KEYWORDS, SOCIAL_DOMAINS, RuntimeOptions
from .helpers import infer_price_from_text, normalize_phone, safe_text, select_better_price_hint

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


def empty_website_social() -> dict[str, str]:
    return {
        "instagram": "",
        "tiktok": "",
        "linkedin": "",
        "facebook": "",
        "email": "",
        "whatsapp": "",
        "pic_name": "",
        "price_hint": "",
    }


def log_scrapling_status(options: RuntimeOptions) -> None:
    if SCRAPLING_AVAILABLE:
        if options.scrapling_full_crawl:
            if SCRAPLING_SPIDER_AVAILABLE:
                print(
                    "[INFO] Scrapling Spider aktif: full-crawl enrichment hingga "
                    f"{options.scrapling_full_crawl_max_pages} halaman/site."
                )
            else:
                print("[WARN] SCRAPLING_FULL_CRAWL aktif tapi modul spider tidak tersedia.")
                print("[INFO] Scrapling Fetcher tetap dipakai untuk enrichment 1 halaman.")
        else:
            print("[INFO] Scrapling terdeteksi: website enrichment pakai Scrapling.")
        return
    print("[INFO] Scrapling belum terpasang: fallback ke requests + BeautifulSoup.")


def extract_social_from_text_and_links(page_text: str, links: list[str]) -> dict[str, str]:
    result = empty_website_social()

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


def html_from_scrapling(url: str) -> str:
    if not SCRAPLING_AVAILABLE:
        return ""
    try:
        page = ScraplingFetcher.get(url)
    except Exception:
        return ""
    return html_from_scrapling_page(page)


def html_from_scrapling_page(page) -> str:
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


def extract_text_and_links_from_html(html: str) -> tuple[str, list[str]]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    links = [anchor.get("href", "").strip() for anchor in soup.find_all("a", href=True)]
    return text, links


def candidate_price_urls(base_url: str, links: list[str], options: RuntimeOptions) -> list[str]:
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc.lower()
    candidates = []
    seen = set()

    for href in links:
        href = safe_text(href)
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

    return candidates[: options.max_price_follow_links]


def extract_social_from_website_crawl_with_spider(
    website_url: str, options: RuntimeOptions
) -> dict[str, str]:
    result = empty_website_social()
    if not SCRAPLING_SPIDER_AVAILABLE:
        return result

    normalized_url = website_url
    if normalized_url and not normalized_url.startswith(("http://", "https://")):
        normalized_url = f"https://{normalized_url}"

    parsed = urlparse(normalized_url)
    if not parsed.netloc:
        return result

    domain = parsed.netloc.split(":", 1)[0].lower()

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
            if self.pages_collected >= options.scrapling_full_crawl_max_pages:
                return

            self.pages_collected += 1
            current_url = safe_text(getattr(response, "url", "")) or normalized_url
            html = html_from_scrapling_page(response)
            if html:
                page_text, page_links = extract_text_and_links_from_html(html)
                yield {"text": page_text, "links": page_links}

            if self.pages_collected >= options.scrapling_full_crawl_max_pages:
                return

            for href in response.css("a::attr(href)").getall():
                href = safe_text(href)
                if not href:
                    continue
                next_url = urljoin(current_url, href)
                if next_url:
                    yield response.follow(next_url, callback=self.parse)

    try:
        crawl_result = WebsiteSpider().start()
    except Exception:
        return result

    combined_text_parts = []
    combined_links = []
    for item in getattr(crawl_result, "items", []):
        item_text = safe_text(item.get("text"))
        if item_text:
            combined_text_parts.append(item_text)
        item_links = item.get("links") or []
        combined_links.extend(safe_text(link) for link in item_links if safe_text(link))

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


def extract_social_and_contact_from_website(
    session: requests.Session, website_url: str, options: RuntimeOptions
) -> dict[str, str]:
    result = empty_website_social()
    if not website_url:
        return result

    normalized_url = website_url
    if not normalized_url.startswith(("http://", "https://")):
        normalized_url = f"https://{normalized_url}"

    if options.scrapling_full_crawl:
        crawled = extract_social_from_website_crawl_with_spider(normalized_url, options)
        if any(crawled.values()):
            return crawled

    html = html_from_scrapling(normalized_url)
    if not html:
        try:
            response = session.get(normalized_url, timeout=options.request_timeout)
            response.raise_for_status()
            html = response.text
        except requests.RequestException:
            return result

    text, links = extract_text_and_links_from_html(html)
    result = extract_social_from_text_and_links(text, links)
    if result["price_hint"]:
        return result

    for candidate_url in candidate_price_urls(normalized_url, links, options):
        try:
            response = session.get(candidate_url, timeout=options.price_page_timeout)
            response.raise_for_status()
        except requests.RequestException:
            continue

        candidate_text, candidate_links = extract_text_and_links_from_html(response.text)
        candidate_result = extract_social_from_text_and_links(candidate_text, candidate_links)

        for field in ("instagram", "tiktok", "linkedin", "facebook", "email", "whatsapp", "pic_name"):
            if not result[field]:
                result[field] = candidate_result[field]
        result["price_hint"] = select_better_price_hint(
            result["price_hint"], candidate_result["price_hint"]
        )
        if result["price_hint"]:
            break

    return result
