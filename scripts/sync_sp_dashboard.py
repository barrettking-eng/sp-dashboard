#!/usr/bin/env python3
"""
sync_sp_dashboard.py
--------------------
Pulls fresh data from PartnerStack and rewrites sp-dashboard.html in-place.

Required env vars:
  PARTNERSTACK_PUBLIC_KEY  — PartnerStack public key (Basic Auth username)
  PARTNERSTACK_SECRET_KEY  — PartnerStack secret key (Basic Auth password)
"""

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone

import requests

# ── Config ─────────────────────────────────────────────────────────────────────

PS_PUBLIC_KEY = os.environ.get("PARTNERSTACK_PUBLIC_KEY", "").strip()
PS_SECRET_KEY = os.environ.get("PARTNERSTACK_SECRET_KEY", "").strip()
if not PS_PUBLIC_KEY or not PS_SECRET_KEY:
    sys.exit("ERROR: PARTNERSTACK_PUBLIC_KEY and PARTNERSTACK_SECRET_KEY must both be set.")

PS_BASE   = "https://api.partnerstack.com/api/v2"
HTML_FILE = os.path.join(os.path.dirname(__file__), "..", "sp-dashboard.html")

# Only include partners who joined on or after this date
START_DATE_MS = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)

# PartnerStack group slugs → tier label
SLUG_TO_TIER = {
    "solutionpartnertier1": "Tier 1",
    "solutionpartnertier2": "Tier 2",
    "solutionpartnertier3": "Tier 3",
}

# ── API helpers ────────────────────────────────────────────────────────────────

def ps_get(path, params=None):
    resp = requests.get(
        f"{PS_BASE}{path}",
        auth=(PS_PUBLIC_KEY, PS_SECRET_KEY),
        params=params or {},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def paginate_partnerships():
    """Yield all partnerships, paginated via cursor."""
    params = {"limit": 250}
    while True:
        data = ps_get("/partnerships", params)["data"]
        items = data.get("items", [])
        yield from items
        if not data.get("has_more") or not items:
            break
        params["starting_after"] = items[-1]["key"]


# ── Data transformation ────────────────────────────────────────────────────────

def partner_name(p):
    """Best display name for a partner."""
    team = (p.get("team") or {}).get("name", "").strip()
    if team:
        return team
    first = (p.get("first_name") or "").strip()
    last  = (p.get("last_name")  or "").strip()
    full  = f"{first} {last}".strip()
    return full or p.get("email", "")


def build_raw(partners):
    """Aggregate into the RAW structure expected by sp-dashboard.html."""
    all_months   = set()
    rows_by_tier = defaultdict(list)

    for p in partners:
        joined_ms = p.get("joined_at") or p.get("created_at") or 0
        if joined_ms < START_DATE_MS:
            continue

        group_slug = (p.get("group") or {}).get("slug", "")
        tier = SLUG_TO_TIER.get(group_slug)
        if not tier:
            continue  # not a Solutions Partner tier group

        joined_date = datetime.fromtimestamp(joined_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        month       = joined_date[:7]
        all_months.add(month)

        stats = p.get("stats") or {}
        rows_by_tier[tier].append({
            "name":             partner_name(p),
            "email":            p.get("email", ""),
            "tier":             tier,
            "joined_date":      joined_date,
            "month":            month,
            "trials":           int(stats.get("CUSTOMER_COUNT", 0)),
            "activations":      int(stats.get("PAID_ACCOUNT_COUNT", 0)),
            "revenue_cents":    int(stats.get("REVENUE", 0)),
            "commission_cents": int(stats.get("COMMISSION_EARNED", 0)),
        })

    # Sort each tier by joined_date descending
    for tier in rows_by_tier:
        rows_by_tier[tier].sort(key=lambda r: r["joined_date"], reverse=True)

    tiers = ["Tier 1", "Tier 2", "Tier 3"]

    summary = {
        tier: {
            "total_joined":      len(rows_by_tier.get(tier, [])),
            "with_trials":       sum(1 for r in rows_by_tier.get(tier, []) if r["trials"] > 0),
            "with_activations":  sum(1 for r in rows_by_tier.get(tier, []) if r["activations"] > 0),
            "total_trials":      sum(r["trials"]      for r in rows_by_tier.get(tier, [])),
            "total_activations": sum(r["activations"] for r in rows_by_tier.get(tier, [])),
        }
        for tier in tiers
    }

    monthly = {}
    for month in sorted(all_months):
        monthly[month] = {}
        for tier in tiers:
            rows = [r for r in rows_by_tier.get(tier, []) if r["month"] == month]
            if rows:
                monthly[month][tier] = {
                    "joined":            len(rows),
                    "with_trials":       sum(1 for r in rows if r["trials"] > 0),
                    "with_activations":  sum(1 for r in rows if r["activations"] > 0),
                    "total_trials":      float(sum(r["trials"]      for r in rows)),
                    "total_activations": float(sum(r["activations"] for r in rows)),
                }

    raw = {
        "summary":  summary,
        "monthly":  monthly,
        "partners": {tier: rows_by_tier.get(tier, []) for tier in tiers},
    }
    return raw, sorted(all_months)


# ── HTML patching ──────────────────────────────────────────────────────────────

def patch_raw(html, raw):
    new_line = "const RAW = " + json.dumps(raw, ensure_ascii=False) + ";"
    html, n = re.subn(r"const RAW = \{.*?\};", new_line, html, flags=re.DOTALL)
    print(f"  RAW replaced ({n} substitution)")
    return html


def patch_months(html, months):
    html, n = re.subn(r"const MONTHS = \[.*?\];", f"const MONTHS = {json.dumps(months)};", html)
    print(f"  MONTHS → {months} ({n} sub)")

    labels = {m: datetime.strptime(m, "%Y-%m").strftime("%b %Y") for m in months}
    html, n = re.subn(r"const MONTH_LABELS_MAP = \{.*?\};",
                      f"const MONTH_LABELS_MAP = {json.dumps(labels)};", html)
    print(f"  MONTH_LABELS_MAP updated ({n} sub)")
    return html


def patch_timestamp(html, now_dt):
    pretty = now_dt.strftime("%b %-d, %Y at %-I:%M %p UTC")
    html, n = re.subn(
        r'(<div class="header-badge"><span class="dot"></span> ).*?(</div>)',
        rf'\1Live · PartnerStack · Updated {pretty}\2',
        html,
    )
    print(f"  Timestamp → {pretty} ({n} sub)")
    return html


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("═" * 60)
    print("  SP Dashboard — PartnerStack Sync")
    print("═" * 60)

    print("\n[1/3] Fetching all partnerships from PartnerStack...")
    all_partners = list(paginate_partnerships())
    print(f"  → {len(all_partners)} total partnerships fetched")

    print("\n[2/3] Filtering & aggregating (Tier 1–3 since Jan 2026)...")
    raw, months = build_raw(all_partners)
    for tier, s in raw["summary"].items():
        if s["total_joined"] > 0:
            print(f"  {tier}: {s['total_joined']} joined · "
                  f"{s['with_trials']} w/trials · "
                  f"{s['with_activations']} activated")

    print(f"\n[3/3] Patching {os.path.basename(HTML_FILE)}...")
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html = f.read()

    html = patch_raw(html, raw)
    html = patch_months(html, months)
    html = patch_timestamp(html, datetime.now(timezone.utc))

    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print("\n" + "═" * 60)
    t1 = raw["summary"].get("Tier 1", {})
    print(f"  Done · {t1.get('total_joined',0)} partners · "
          f"{t1.get('with_trials',0)} w/trials · "
          f"{t1.get('with_activations',0)} activated")
    print("═" * 60)


if __name__ == "__main__":
    main()
