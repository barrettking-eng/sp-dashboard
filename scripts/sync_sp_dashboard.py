#!/usr/bin/env python3
"""
sync_sp_dashboard.py
--------------------
Pulls fresh data from PartnerStack and rewrites sp-dashboard.html in-place.
Runs on schedule via GitHub Actions.

Required env vars:
  PARTNERSTACK_PUBLIC_KEY  — PartnerStack public key (Basic Auth username)
  PARTNERSTACK_SECRET_KEY  — PartnerStack secret key (Basic Auth password)

PartnerStack API v2 uses HTTP Basic Auth: public key as username, secret key as password.
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

PS_BASE = "https://api.partnerstack.com/api/v2"
HTML_FILE = os.path.join(os.path.dirname(__file__), "..", "sp-dashboard.html")

# Partners are bucketed by join month starting from this date
START_DATE = "2026-01-01"

# Tier tag to look for on PartnerStack partner groups
TIER_1_GROUP_KEY = "tier-1"   # adjust to match your actual PartnerStack group key

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


def paginate(path, params=None, key="partnerships"):
    """Yield all items from a paginated PartnerStack endpoint."""
    params = dict(params or {})
    params.setdefault("limit", 250)
    cursor = None
    while True:
        if cursor:
            params["starting_after"] = cursor
        data = ps_get(path, params)
        items = data.get(key) or data.get("data") or []
        yield from items
        if not data.get("has_more"):
            break
        # Use the last item's key as cursor
        last = items[-1] if items else None
        if not last:
            break
        cursor = last.get("key") or last.get("id")


# ── Data fetching ──────────────────────────────────────────────────────────────

def fetch_partners():
    """Fetch all partners joined since START_DATE."""
    print("  Fetching partners from PartnerStack...")
    partners = list(paginate("/partnerships", {"min_created_at": START_DATE}))
    print(f"  → {len(partners)} partners")
    return partners


def fetch_referrals():
    """Fetch all referrals (trials) and their status."""
    print("  Fetching referrals...")
    referrals = list(paginate("/referrals", {}, key="referrals"))
    print(f"  → {len(referrals)} referrals")
    return referrals


def fetch_rewards():
    """Fetch all approved/paid rewards (activations/commissions)."""
    print("  Fetching rewards...")
    rewards = list(paginate("/rewards", {}, key="rewards"))
    print(f"  → {len(rewards)} rewards")
    return rewards


# ── Data transformation ────────────────────────────────────────────────────────

def month_of(iso_date_str):
    """Return 'YYYY-MM' from an ISO date string."""
    if not iso_date_str:
        return None
    return iso_date_str[:7]


def classify_tier(partner):
    """Return the tier label for a partner based on their group."""
    groups = partner.get("groups") or partner.get("partner_groups") or []
    for g in groups:
        key = (g.get("key") or g.get("name") or "").lower()
        if "tier-1" in key or "tier 1" in key:
            return "Tier 1"
        if "tier-2" in key or "tier 2" in key:
            return "Tier 2"
        if "tier-3" in key or "tier 3" in key:
            return "Tier 3"
    return "Tier 1"   # default — adjust if your program has a different default


def build_raw(partners, referrals, rewards):
    """Aggregate partners, referrals, and rewards into the RAW structure."""

    # Index referrals by partner key
    partner_trials = defaultdict(int)
    for r in referrals:
        pkey = r.get("partner_key") or r.get("partnership_key")
        if pkey:
            partner_trials[pkey] += 1

    # Index rewards by partner key
    partner_activations = defaultdict(int)
    partner_revenue     = defaultdict(int)   # cents
    partner_commission  = defaultdict(int)   # cents
    for rw in rewards:
        pkey = rw.get("partner_key") or rw.get("partnership_key")
        if not pkey:
            continue
        status = (rw.get("status") or "").lower()
        if status not in ("approved", "paid"):
            continue
        partner_activations[pkey] += 1
        partner_revenue[pkey]    += int(rw.get("customer_amount") or 0)
        partner_commission[pkey] += int(rw.get("partner_amount")  or 0)

    # Build per-partner rows
    all_months = set()
    rows_by_tier = defaultdict(list)

    for p in partners:
        joined_date = p.get("created_at") or p.get("joined_at") or ""
        if isinstance(joined_date, (int, float)):
            # Unix timestamp
            joined_date = datetime.fromtimestamp(joined_date, tz=timezone.utc).strftime("%Y-%m-%d")
        else:
            joined_date = joined_date[:10]

        if joined_date < START_DATE[:10]:
            continue

        month = month_of(joined_date)
        if not month:
            continue
        all_months.add(month)

        pkey  = p.get("key") or p.get("partner_key") or p.get("id") or ""
        name  = (p.get("company_name") or p.get("name") or "").strip() or \
                (p.get("customer", {}) or {}).get("name", "")
        email = (p.get("email") or (p.get("customer", {}) or {}).get("email", "")).strip()
        tier  = classify_tier(p)

        trials      = partner_trials.get(pkey, 0)
        activations = partner_activations.get(pkey, 0)
        revenue     = partner_revenue.get(pkey, 0)
        commission  = partner_commission.get(pkey, 0)

        rows_by_tier[tier].append({
            "name":            name or email,
            "email":           email,
            "tier":            tier,
            "joined_date":     joined_date,
            "month":           month,
            "trials":          trials,
            "activations":     activations,
            "revenue_cents":   revenue,
            "commission_cents": commission,
        })

    # Sort each tier by joined_date desc
    for tier in rows_by_tier:
        rows_by_tier[tier].sort(key=lambda r: r["joined_date"], reverse=True)

    # Build summary and monthly aggregates
    tiers = ["Tier 1", "Tier 2", "Tier 3"]
    summary = {}
    monthly = {}

    for tier in tiers:
        rows = rows_by_tier.get(tier, [])
        summary[tier] = {
            "total_joined":      len(rows),
            "with_trials":       sum(1 for r in rows if r["trials"] > 0),
            "with_activations":  sum(1 for r in rows if r["activations"] > 0),
            "total_trials":      sum(r["trials"] for r in rows),
            "total_activations": sum(r["activations"] for r in rows),
        }

    for month in sorted(all_months):
        monthly[month] = {}
        for tier in tiers:
            rows = [r for r in rows_by_tier.get(tier, []) if r["month"] == month]
            if rows:
                monthly[month][tier] = {
                    "joined":           len(rows),
                    "with_trials":      sum(1 for r in rows if r["trials"] > 0),
                    "with_activations": sum(1 for r in rows if r["activations"] > 0),
                    "total_trials":     float(sum(r["trials"] for r in rows)),
                    "total_activations": float(sum(r["activations"] for r in rows)),
                }

    raw = {
        "summary": summary,
        "monthly": monthly,
        "partners": {tier: rows_by_tier.get(tier, []) for tier in tiers},
    }

    return raw, sorted(all_months)


# ── HTML patching ──────────────────────────────────────────────────────────────

def patch_raw(html, raw):
    """Replace the const RAW = {...}; line."""
    new_line = "const RAW = " + json.dumps(raw, ensure_ascii=False) + ";"
    html, n = re.subn(
        r"const RAW = \{.*?\};",
        new_line,
        html,
        flags=re.DOTALL,
    )
    print(f"  RAW data replaced ({n} substitution)")
    return html


def patch_months(html, months):
    """Update the MONTHS array and MONTH_LABELS_MAP."""
    months_js = json.dumps(months)
    html, n = re.subn(r"const MONTHS = \[.*?\];", f"const MONTHS = {months_js};", html)
    print(f"  MONTHS updated → {months} ({n} substitution)")

    labels_map = {m: datetime.strptime(m, "%Y-%m").strftime("%b %Y") for m in months}
    labels_js = json.dumps(labels_map)
    html, n = re.subn(r"const MONTH_LABELS_MAP = \{.*?\};", f"const MONTH_LABELS_MAP = {labels_js};", html)
    print(f"  MONTH_LABELS_MAP updated ({n} substitution)")
    return html


def patch_timestamp(html, now_dt):
    """Update the header badge timestamp."""
    pretty = now_dt.strftime("%b %-d, %Y at %-I:%M %p UTC")
    html, n = re.subn(
        r'(<div class="header-badge"><span class="dot"></span> ).*?(</div>)',
        rf'\1Live · PartnerStack · Updated {pretty}\2',
        html,
    )
    print(f"  Timestamp → {pretty} ({n} substitution)")
    return html


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("═" * 60)
    print("  SP Dashboard — PartnerStack Sync")
    print("═" * 60)

    print("\n[1/4] Fetching PartnerStack data...")
    partners  = fetch_partners()
    referrals = fetch_referrals()
    rewards   = fetch_rewards()

    print("\n[2/4] Aggregating data...")
    raw, months = build_raw(partners, referrals, rewards)
    for tier, s in raw["summary"].items():
        if s["total_joined"] > 0:
            print(f"  {tier}: {s['total_joined']} joined, "
                  f"{s['with_trials']} w/trials, "
                  f"{s['with_activations']} activated")

    print(f"\n[3/4] Patching {HTML_FILE}...")
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html = f.read()

    html = patch_raw(html, raw)
    html = patch_months(html, months)
    html = patch_timestamp(html, datetime.now(timezone.utc))

    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print("\n[4/4] Complete.")
    print("═" * 60)
    t1 = raw["summary"].get("Tier 1", {})
    print(f"  {t1.get('total_joined', 0)} partners · "
          f"{t1.get('with_trials', 0)} w/trials · "
          f"{t1.get('with_activations', 0)} activated")
    print("═" * 60)


if __name__ == "__main__":
    main()
