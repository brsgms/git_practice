#!/usr/bin/env python3
import argparse
import csv
import json
import os
import random
import sys
import time
from typing import Dict, Generator, Iterable, List, Optional

import requests
import backoff

# Yahoo screener endpoint candidates
SCREENER_ENDPOINTS = [
    "https://query1.finance.yahoo.com/v1/finance/screener",
    "https://query2.finance.yahoo.com/v1/finance/screener",
]

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://finance.yahoo.com",
    "Referer": "https://finance.yahoo.com/screener/new",
    "Content-Type": "application/json",
}


def build_payload(region: str, offset: int, size: int, quote_type: str = "EQUITY", exchange: Optional[str] = None) -> Dict:
    return {
        "offset": offset,
        "size": size,
        "sortField": "symbol",
        "sortType": "asc",
        "quoteType": quote_type,
        "query": {
            "operator": "and",
            "operands": [
                {"operator": "eq", "operands": ["region", region]},
                *(([{"operator": "eq", "operands": ["exchange", exchange]}] ) if exchange else []),
            ],
        },
    }


class TooManyRequests(Exception):
    pass


def _should_give_up(e: Exception) -> bool:
    # Give up on 4xx other than 429, or on explicit flags
    if isinstance(e, requests.HTTPError):
        status = e.response.status_code
        return status != 429 and 400 <= status < 500
    return False


@backoff.on_exception(
    backoff.expo,
    (TooManyRequests, requests.Timeout, requests.ConnectionError, requests.HTTPError),
    max_time=120,
    giveup=_should_give_up,
    jitter=backoff.full_jitter,
)
def call_screener(session: requests.Session, endpoint: str, payload: Dict) -> Dict:
    resp = session.post(endpoint, headers=DEFAULT_HEADERS, data=json.dumps(payload), timeout=20)
    # Some edges return text "Too Many Requests" without a 429 code
    if resp.status_code == 429 or (resp.text and "Too Many Requests" in resp.text):
        raise TooManyRequests("429 Too Many Requests from Yahoo")
    resp.raise_for_status()
    return resp.json()


def _parse_cookie_string(cookie_str: str) -> Dict[str, str]:
    parts = [p.strip() for p in cookie_str.split(";") if p.strip()]
    kv: Dict[str, str] = {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip()] = v.strip()
    return kv


def initialize_session(session: requests.Session, cookie_str: Optional[str] = None) -> None:
    # Optionally inject user-supplied cookies (e.g., exported from browser)
    if cookie_str:
        for k, v in _parse_cookie_string(cookie_str).items():
            session.cookies.set(k, v, domain=".yahoo.com")

    # Warm up cookies by visiting Yahoo domains
    warm_headers = DEFAULT_HEADERS.copy()
    warm_headers.setdefault("Accept-Language", "en-US,en;q=0.9")
    for url in (
        "https://fc.yahoo.com",
        "https://finance.yahoo.com/",
        "https://finance.yahoo.com/screener/new",
    ):
        try:
            session.get(url, headers=warm_headers, timeout=15)
        except Exception:
            # Best-effort; continue
            pass

    # Try to obtain a crumb (often not required for screener, but helps initialize session)
    try:
        session.get("https://query1.finance.yahoo.com/v1/test/getcrumb", headers=warm_headers, timeout=15)
    except Exception:
        pass


def iter_screener_quotes(region: str, page_size: int = 250, cookie_str: Optional[str] = None, exchange: Optional[str] = None, quote_type: str = "EQUITY") -> Generator[Dict, None, None]:
    session = requests.Session()
    initialize_session(session, cookie_str=cookie_str)
    # Randomize initial endpoint and rotate on failures
    endpoints = SCREENER_ENDPOINTS.copy()
    random.shuffle(endpoints)

    offset = 0
    seen_symbols = set()
    consecutive_empty = 0

    while True:
        payload = build_payload(region=region, offset=offset, size=page_size, quote_type=quote_type, exchange=exchange)
        last_error: Optional[Exception] = None
        for endpoint in endpoints:
            try:
                data = call_screener(session, endpoint, payload)
                quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])
                if not quotes:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        return
                    break
                consecutive_empty = 0
                for q in quotes:
                    sym = q.get("symbol")
                    if not sym:
                        continue
                    if sym in seen_symbols:
                        continue
                    seen_symbols.add(sym)
                    yield q
                offset += len(quotes)
                # Friendly small pause to reduce rate limiting
                time.sleep(0.4)
                break
            except Exception as e:
                last_error = e
                # rotate endpoints and continue
                time.sleep(0.8)
                continue
        else:
            # If we exhausted endpoints for this page without success, re-raise last error
            if last_error:
                raise last_error


def write_csv(rows: Iterable[Dict], out_path: str) -> None:
    out_fields = ["symbol", "shortName", "longName", "exchange", "exchangeName", "quoteType", "region", "currency"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in out_fields})


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Yahoo Finance symbols via screener API")
    parser.add_argument("--region", default="us", help="Yahoo region code, e.g. us, gb, de, in")
    parser.add_argument("--page-size", type=int, default=250, help="Page size for pagination")
    parser.add_argument("--limit", type=int, default=0, help="Max number of rows to fetch (0 = all)")
    parser.add_argument("--out", default="symbols.csv", help="Output CSV path")
    parser.add_argument("--cookie", default="", help="Optional cookie string 'k=v; k2=v2' from browser")
    parser.add_argument("--exchange", default="", help="Optional exchange code, e.g. NMS, NYQ, NEO, TOR")
    parser.add_argument("--quote-type", default="EQUITY", help="Yahoo quoteType, e.g. EQUITY, ETF, MUTUALFUND")
    args = parser.parse_args()

    count = 0
    def row_iter():
        nonlocal count
        for q in iter_screener_quotes(
            region=args.region,
            page_size=args.page_size,
            cookie_str=(args.cookie or None),
            exchange=(args.exchange or None),
            quote_type=args.quote_type,
        ):
            yield q
            count += 1
            if args.limit and count >= args.limit:
                return

    write_csv(row_iter(), args.out)
    print(f"Wrote {count} rows to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
