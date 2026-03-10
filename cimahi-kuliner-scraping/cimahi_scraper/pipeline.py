from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
import requests

from .config import OUTPUT_COLUMNS, ProjectFiles, RuntimeOptions, SEARCH_TARGETS, SOCIAL_DOMAINS, load_env_files
from .helpers import (
    build_address,
    fixed_location_score,
    get_lat_lon,
    google_maps_from_lat_lon,
    infer_price_by_sub_business,
    normalize_phone,
    normalize_price,
    safe_text,
)
from .overpass import fetch_all_osm_elements
from .website import empty_website_social, extract_social_and_contact_from_website, log_scrapling_status


def get_target_for_element(tags: dict):
    for target in SEARCH_TARGETS:
        for key, value in target.osm_filters:
            if safe_text(tags.get(key)) == value:
                return target
    return None


def social_from_osm_tags(tags: dict) -> dict[str, str]:
    result = {
        "instagram": safe_text(tags.get("contact:instagram")) or safe_text(tags.get("instagram")),
        "tiktok": safe_text(tags.get("contact:tiktok")) or safe_text(tags.get("tiktok")),
        "linkedin": safe_text(tags.get("contact:linkedin")) or safe_text(tags.get("linkedin")),
        "facebook": safe_text(tags.get("contact:facebook")) or safe_text(tags.get("facebook")),
        "email": safe_text(tags.get("contact:email")) or safe_text(tags.get("email")),
        "pic_name": "",
        "whatsapp": safe_text(tags.get("contact:whatsapp")) or safe_text(tags.get("whatsapp")),
    }
    website = safe_text(tags.get("website")) or safe_text(tags.get("contact:website"))
    if website:
        lowered = website.lower()
        for field_name, domain in SOCIAL_DOMAINS.items():
            if not result[field_name] and domain in lowered:
                result[field_name] = website
    if result["whatsapp"]:
        result["whatsapp"] = normalize_phone(result["whatsapp"])
    return result


def build_row(element: dict, target, website_social: dict[str, str]) -> dict[str, str]:
    tags = element.get("tags", {})
    lat, lon = get_lat_lon(element)
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
        safe_text(tags.get("contact:phone"))
        or safe_text(tags.get("phone"))
        or safe_text(tags.get("contact:mobile"))
    )
    website = safe_text(tags.get("website")) or safe_text(tags.get("contact:website"))
    price = safe_text(tags.get("price")) or safe_text(tags.get("price:range")) or safe_text(tags.get("charge"))
    price_range = normalize_price(price)
    if not price_range:
        price_range = normalize_price(website_social.get("price_hint", ""))
    if not price_range:
        price_range = infer_price_by_sub_business(target.sub_business)

    return {
        "place_id": "",
        "business_name": safe_text(tags.get("name")),
        "address": build_address(tags),
        "google_maps": google_maps_from_lat_lon(lat, lon),
        "industry": target.industry,
        "sub_business": target.sub_business,
        "instagram": merged_social["instagram"],
        "tiktok": merged_social["tiktok"],
        "linkedin": merged_social["linkedin"],
        "website": website,
        "facebook": merged_social["facebook"],
        "email": merged_social["email"],
        "pic_name": merged_social["pic_name"],
        "whatsapp_or_phone": merged_social["whatsapp"] or normalize_phone(phone),
        "price_range": price_range,
    }


def build_dataframe(rows: list[dict], max_output_rows: int) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    if df.empty:
        return df
    df["_fixed_score"] = [row.get("_fixed_score", 0) for row in rows[: len(df)]]
    df.drop_duplicates(subset=["business_name", "address"], inplace=True)
    df.sort_values(by=["_fixed_score", "business_name"], ascending=[False, True], inplace=True)
    if len(df) > max_output_rows:
        print(f"[WARN] Data lebih dari {max_output_rows}, disimpan hanya {max_output_rows} baris pertama.")
        df = df.head(max_output_rows).copy()
    df["place_id"] = range(1, len(df) + 1)
    df.drop(columns=["_fixed_score"], inplace=True, errors="ignore")
    return df


def main(entry_file: str | Path | None = None, files: ProjectFiles | None = None) -> None:
    project_files = files or ProjectFiles.from_base_dir(Path(__file__).resolve().parent.parent)
    if entry_file is not None:
        project_files = ProjectFiles.discover(entry_file)
    load_env_files(project_files.env_files)
    options = RuntimeOptions.from_env()

    log_scrapling_status(options)

    session = requests.Session()
    rows = []
    seen_keys = set()
    skipped_missing_name = 0
    skipped_low_fixed_score = 0
    skipped_duplicate = 0

    print("[INFO] Scraping OSM: all targets (chunked query)")
    elements, fetch_ok = fetch_all_osm_elements(session, options)
    print(f"[INFO] Total elemen OSM terkumpul: {len(elements)}")

    for element in elements:
        tags = element.get("tags", {})
        target = get_target_for_element(tags)
        if target is None:
            continue

        business_name = safe_text(tags.get("name"))
        if not business_name:
            skipped_missing_name += 1
            continue

        lat, lon = get_lat_lon(element)
        fixed_score = fixed_location_score(tags, lat, lon)
        if options.strict_fixed_location_only and fixed_score < options.min_fixed_location_score:
            skipped_low_fixed_score += 1
            continue

        dedupe_key = f"{business_name.lower()}|{build_address(tags).lower()}"
        if dedupe_key in seen_keys:
            skipped_duplicate += 1
            continue
        seen_keys.add(dedupe_key)

        website = safe_text(tags.get("website")) or safe_text(tags.get("contact:website"))
        website_social = (
            extract_social_and_contact_from_website(session, website, options)
            if website
            else empty_website_social()
        )

        row = build_row(element, target, website_social)
        row["_fixed_score"] = fixed_score
        rows.append(row)
        time.sleep(0.1)
        if len(rows) >= options.max_output_rows * 2:
            break

    df = build_dataframe(rows, options.max_output_rows)
    if not fetch_ok:
        print("[ERROR] Semua endpoint Overpass gagal. Update dianggap tidak lengkap.")
        if project_files.output_csv.exists():
            print(f"[WARN] CSV lama dipertahankan: {project_files.output_csv.name}")
        else:
            print("[WARN] Belum ada CSV lama untuk dipertahankan.")
        print("Done scraping!")
        print("Total data:", len(df))
        return

    df.to_csv(project_files.output_csv, index=False)
    print(f"[INFO] CSV diperbarui: {project_files.output_csv.name}")
    print(
        "[INFO] Ringkasan filter:"
        f" tanpa nama={skipped_missing_name},"
        f" skor lokasi rendah={skipped_low_fixed_score},"
        f" duplikat={skipped_duplicate}"
    )
    if df.empty:
        print("[INFO] Hasil scraping kosong. CSV disimpan apa adanya.")
        if skipped_low_fixed_score > 0:
            suggested_score = max(1, options.min_fixed_location_score - 1)
            print(
                "[WARN] Banyak data terbuang oleh filter lokasi tetap."
                f" Coba set MIN_FIXED_LOCATION_SCORE={suggested_score}"
                " atau STRICT_FIXED_LOCATION_ONLY=false di .env."
            )
    elif len(df) < options.max_output_rows:
        print(f"[WARN] Data terbaru hanya {len(df)} baris (target {options.max_output_rows}).")

    print("Done scraping!")
    print("Total data:", len(df))
