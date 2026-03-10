from __future__ import annotations

import time

import requests

from .config import OVERPASS_ENDPOINTS, SEARCH_TARGETS, RuntimeOptions
from .helpers import chunk_list, get_lat_lon, safe_text


def safe_request_json(session: requests.Session, method: str, url: str, options: RuntimeOptions, context: str = "", retries: int | None = None, **kwargs) -> dict:
    total_retries = retries if retries is not None else options.request_retries
    for attempt in range(1, total_retries + 1):
        try:
            response = session.request(method, url, timeout=options.request_timeout, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (429, 502, 503, 504) and attempt < total_retries:
                sleep_seconds = options.retry_sleep_seconds * attempt
                print(
                    f"[WARN] HTTP {status} {context} ({attempt}/{total_retries}), retry {sleep_seconds}s."
                )
                time.sleep(sleep_seconds)
                continue
            print(f"[WARN] Request gagal {context}: {exc}")
            return {}
        except requests.Timeout:
            if attempt < total_retries:
                print(
                    f"[WARN] Timeout {context} ({attempt}/{total_retries}), retry {options.retry_sleep_seconds}s."
                )
                time.sleep(options.retry_sleep_seconds)
            else:
                print(f"[WARN] Timeout {context}.")
        except requests.RequestException as exc:
            print(f"[WARN] Request gagal {context}: {exc}")
            return {}
        except ValueError:
            print(f"[WARN] JSON tidak valid {context}.")
            return {}
    return {}


def overpass_query_for_filters_nearby(filters: list[tuple[str, str]], options: RuntimeOptions) -> str:
    statements = []
    for key, value in filters:
        statements.append(
            f'node["{key}"="{value}"](around:{options.search_radius_m},{options.cimahi_lat},{options.cimahi_lon});'
        )
        statements.append(
            f'way["{key}"="{value}"](around:{options.search_radius_m},{options.cimahi_lat},{options.cimahi_lon});'
        )
        statements.append(
            f'relation["{key}"="{value}"](around:{options.search_radius_m},{options.cimahi_lat},{options.cimahi_lon});'
        )
    return f"[out:json][timeout:50];({''.join(statements)});out center tags;"


def overpass_query_for_filters_cimahi_area(filters: list[tuple[str, str]]) -> str:
    statements = []
    for key, value in filters:
        statements.append(f'node(area.searchArea)["{key}"="{value}"];')
        statements.append(f'way(area.searchArea)["{key}"="{value}"];')
        statements.append(f'relation(area.searchArea)["{key}"="{value}"];')
    return (
        '[out:json][timeout:70];'
        'area["name"="Cimahi"]["boundary"="administrative"]["admin_level"~"6|7|8"]->.searchArea;'
        f"({''.join(statements)});"
        "out center tags;"
    )


def all_unique_filters() -> list[tuple[str, str]]:
    unique = []
    seen = set()
    for target in SEARCH_TARGETS:
        for filter_pair in target.osm_filters:
            if filter_pair in seen:
                continue
            seen.add(filter_pair)
            unique.append(filter_pair)
    return unique


def element_key(element: dict) -> str:
    osm_type = safe_text(element.get("type")) or "unknown"
    osm_id = element.get("id")
    if osm_id is not None:
        return f"{osm_type}:{osm_id}"
    tags = element.get("tags", {})
    lat, lon = get_lat_lon(element)
    return f"{osm_type}:{safe_text(tags.get('name')).lower()}:{lat}:{lon}"


def fetch_overpass_elements_for_filters(
    session: requests.Session,
    filters: list[tuple[str, str]],
    label: str,
    options: RuntimeOptions,
) -> tuple[list[dict], bool]:
    query_attempts = (
        ("cimahi-area", overpass_query_for_filters_cimahi_area(filters)),
        ("nearby-fallback", overpass_query_for_filters_nearby(filters, options)),
    )
    successful_response_received = False

    for query_name, query in query_attempts:
        for endpoint in OVERPASS_ENDPOINTS:
            payload = safe_request_json(
                session,
                "POST",
                endpoint,
                options,
                context=f"Overpass {label} {query_name} via {endpoint}",
                retries=options.overpass_request_retries,
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
            time.sleep(options.overpass_request_pause_seconds)
    return [], successful_response_received


def fetch_all_osm_elements(
    session: requests.Session, options: RuntimeOptions
) -> tuple[list[dict], bool]:
    filter_chunks = chunk_list(all_unique_filters(), options.overpass_filter_chunk_size)
    all_elements = []
    seen = set()
    any_success = False

    for idx, chunk in enumerate(filter_chunks, start=1):
        label = f"chunk-{idx}/{len(filter_chunks)}"
        print(f"[INFO] Overpass query {label} ({len(chunk)} filters)")
        chunk_elements, chunk_ok = fetch_overpass_elements_for_filters(
            session, chunk, label, options
        )
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
