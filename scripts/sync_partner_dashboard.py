#!/usr/bin/env python3
"""
sync_partner_dashboard.py
--------------------------
Pulls fresh data from Close CRM and rewrites chloe-partner-dashboard.html
in-place.  Runs on schedule via GitHub Actions (8 AM / 12 PM / 5 PM ET).

Required env var:
  CLOSE_API_KEY  — your Close API key (stored as a GitHub Actions secret)
"""

import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone

import requests

# ── Config ─────────────────────────────────────────────────────────────────────

CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY", "").strip()
if not CLOSE_API_KEY:
    sys.exit("ERROR: CLOSE_API_KEY environment variable is not set.")

CLOSE_BASE = "https://api.close.com/api/v1"

# Path to the HTML file (script lives in /scripts, HTML is one level up)
HTML_FILE = os.path.join(os.path.dirname(__file__), "..", "chloe-partner-dashboard.html")

# Chloe Implementation Pipeline
PIPELINE_ID = "pipe_0i0v1kKIr7CV4NRPdRnUhH"

# Beta Waitlist Smart View
BETA_WAITLIST_SMART_VIEW_ID = "save_6HkdoVqth3RjDLaP15yXeAbnVwRI0buetTqpe5UexSd"

# Custom Field IDs for partner attribution
CF_AFFILIATE_PARTNER_NAME = "cf_xDjnd1sERjPoK9OKa5qkWjJBJ0cMrFgHpkwkehXI2e4"
CF_AFFILIATE_REFERRAL     = "lcf_cZapR8Iip0LhFN2hvbJHDYwNpOH51Q9ClzHxTasNp1X"
CF_AFFILIATE_GROUP        = "cf_szbmm8WWP2tcIBKyZulm9Q5KW0WIlIww9Wqj7saabWP"

# ── Known partner-attributed lead IDs ─────────────────────────────────────────
# Maps Close lead_id → partner display name.
# Add a new entry here whenever a new partner-referred account gets a Chloe opp.
PARTNER_LEAD_MAP = {
    "lead_8i1NBlFnGvTrgAKEZxH4jtaRiX61PnUYyYhbGirCFn4": "FlowData LLC",      # Biz Advance
    "lead_f0t9rgYG9iLB6Z1MnVWlkirAdKvs3UCtYg72ZdniMWi": "FlowData LLC",      # The Lanam Group
    "lead_nUHlfhcTH3TOSCVua8HDg0Ro0VJA9l4wvSF9YqTy9cj": "Close Accelerate",  # Kingdom Kapital
    "lead_oCwpNrRb2p03e0TuuBeU6cpdETWbvAXohvPFlwCM5IM": "Arya Rashtchian",   # Offerland
}

# Pipeline stage label → internal key
STAGE_MAP = {
    "Waitlist":                  "waitlist",
    "Targeted":                  "targeted",
    "Interested / Qual Unknown": "interested",
    "Interested":                "interested",
    "Qual Unknown":              "interested",
    "Qualified":                 "qualified",
    "Beta Invite":               "betainvite",
    "Beta Invite Sent":          "betainvite",
    "Testing Agent":             "testing",
    "Activated":                 "activated",
}

# Funnel display order: (key, label, css-gradient, extra-suffix-html)
FUNNEL_STAGES = [
    ("waitlist",     "Waitlist",
     "linear-gradient(90deg,#3b82f6,#60a5fa)",
     '<span style="font-size:10px;color:var(--slate-light)">(new inbound)</span>'),
    ("interested",   "Interested / Qual Unknown",
     "linear-gradient(90deg,#3b82f6,#60a5fa)", ""),
    ("targeted",     "Targeted",
     "linear-gradient(90deg,#3b82f6,#60a5fa)",
     '<span style="font-size:10px;color:var(--amber)">★ Partner</span>'),
    ("qualified",    "Qualified",
     "linear-gradient(90deg,#3b82f6,#60a5fa)",
     '<span style="font-size:10px;color:var(--amber)">★ Partner</span>'),
    ("betainvite",   "Beta Invite",
     "linear-gradient(90deg,#7c3aed,#8b5cf6)", ""),
    ("testing",      "Testing Agent",
     "linear-gradient(90deg,#3b82f6,#60a5fa)",
     '<span style="font-size:10px;color:var(--amber)">★★ Partner</span>'),
    ("activated",    "✅ Activated (Won)",
     "linear-gradient(90deg,#10b981,#34d399)", ""),
    ("disqualified", "✗ Lost (all types)",
     "linear-gradient(90deg,#ef4444,#f87171)", ""),
]


# ── Close API helpers ──────────────────────────────────────────────────────────

def close_get(path, params=None):
    resp = requests.get(
        f"{CLOSE_BASE}{path}",
        auth=(CLOSE_API_KEY, ""),
        params=params or {},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_all_opportunities():
    """Page through every opportunity in the Chloe Implementation Pipeline."""
    opps, skip = [], 0
    while True:
        batch = close_get("/opportunity/", {
            "pipeline_id": PIPELINE_ID,
            "_limit":  100,
            "_skip":   skip,
            "_fields": (
                "id,lead_id,lead_name,status_label,status_type,"
                "user_name,note,close_date,date_won,date_lost"
            ),
        })
        opps.extend(batch.get("data", []))
        if not batch.get("has_more"):
            break
        skip += 100
        print(f"  ...{len(opps)} opportunities fetched")
    return opps


def try_discover_new_partner_leads(known_ids):
    """
    Query Close for leads that have Affiliate Partner Name set but aren't in
    PARTNER_LEAD_MAP yet.  Fails gracefully if the query syntax is unsupported.
    Returns {lead_id: partner_name}.
    """
    discovered = {}
    try:
        skip = 0
        while True:
            batch = close_get("/lead/", {
                f"custom.{CF_AFFILIATE_PARTNER_NAME}[has_value]": 1,
                "_limit": 100,
                "_skip":  skip,
                "_fields": f"id,display_name,custom.{CF_AFFILIATE_PARTNER_NAME}",
            })
            for lead in batch.get("data", []):
                lid = lead["id"]
                if lid not in known_ids:
                    partner_name = (lead.get(f"custom.{CF_AFFILIATE_PARTNER_NAME}") or "").strip()
                    if partner_name:
                        discovered[lid] = partner_name
                        print(f"  🆕 New partner lead: {lead.get('display_name')} → {partner_name}")
            if not batch.get("has_more"):
                break
            skip += 100
    except Exception as exc:
        print(f"  (partner auto-discovery skipped: {exc})")
    return discovered


def fetch_beta_waitlist_count():
    try:
        data = close_get("/lead/", {
            "saved_search_id": BETA_WAITLIST_SMART_VIEW_ID,
            "_limit": 1,
            "_fields": "id",
        })
        return data.get("total_results", 0)
    except Exception as exc:
        print(f"  Warning: could not fetch Beta Waitlist count — {exc}")
        return None


# ── Data transformation ────────────────────────────────────────────────────────

def _safe(value, max_len=140):
    s = (value or "").strip()
    s = s.replace("\\", "\\\\").replace("'", "\\'").replace("\r", "").replace("\n", " ")
    return s[:max_len]


def map_opportunity(opp, partner_map):
    status_type  = opp.get("status_type", "active")
    status_label = opp.get("status_label", "")
    lead_id      = opp.get("lead_id", "")

    if status_type == "won":
        stage = "activated"
    elif status_type == "lost":
        stage = "disqualified"
    else:
        stage = STAGE_MAP.get(status_label, "waitlist")

    close_date = opp.get("date_won") or opp.get("close_date") or ""
    return {
        "company": _safe(opp.get("lead_name", "Unknown")),
        "stage":   stage,
        "rep":     _safe(opp.get("user_name", "—")),
        "note":    _safe(opp.get("note") or ""),
        "closeAt": close_date[:10] if close_date else "",
        "partner": _safe(partner_map.get(lead_id, "")),
    }


# ── HTML builders ──────────────────────────────────────────────────────────────

def build_companies_js(companies):
    lines = ["const COMPANIES = ["]
    for c in companies:
        lines.append(
            f"  {{ company:'{c['company']}', stage:'{c['stage']}', "
            f"rep:'{c['rep']}', note:'{c['note']}', closeAt:'{c['closeAt']}', "
            f"partner:'{c['partner']}' }},"
        )
    lines.append("];")
    return "\n".join(lines)


def build_funnel_html(stage_counts, total, partner_counts):
    """Generate the funnel row HTML block (goes between @@SYNC:FUNNEL markers)."""
    rows = []
    for key, label, color, suffix in FUNNEL_STAGES:
        count = stage_counts.get(key, 0)
        pct   = (count / total * 100) if total else 0
        bar_w = max(pct, 0.5) if count > 0 else 0

        pc = partner_counts.get(key, 0)
        shard_html = ""
        if pc > 0:
            sw = max(pc / total * 100, 0.3) if total else 0
            shard_html = (
                f'<div style="position:absolute;top:0;left:{bar_w:.1f}%;'
                f'width:{sw:.1f}%;height:100%;background:var(--amber-light);'
                f'border-radius:0 4px 4px 0"></div>'
            )
            stalled = key == "testing" and pc >= 2
            p_label = f'{pc} partner{"s" if pc > 1 else ""}{"  (⚠️ stalled)" if stalled else ""}'
            p_color = "color:#d97706"
        else:
            p_label = "—"
            p_color = "color:var(--slate-light)"

        pos_style  = "position:relative;" if pc > 0 else ""
        cnt_style  = "font-size:10px;" if count < 50 else ""

        rows.append(
            f'\n          <div class="funnel-row">'
            f'\n            <div class="funnel-label">{label} {suffix}</div>'
            f'\n            <div class="funnel-bar-wrap" style="{pos_style}">'
            f'\n              <div class="funnel-bar" style="width:{bar_w:.1f}%;background:{color}">'
            f'\n                <span class="funnel-count" style="{cnt_style}">{count}</span>'
            f'\n              </div>'
            f'\n              {shard_html}'
            f'\n            </div>'
            f'\n            <div class="funnel-pct">{pct:.0f}%</div>'
            f'\n            <div class="funnel-partner-count" style="font-size:10px;{p_color}">{p_label}</div>'
            f'\n          </div>'
        )
    return "".join(rows)


# ── HTML patching ──────────────────────────────────────────────────────────────

def patch_companies(html, companies):
    block = (
        "// @@SYNC:COMPANIES_START\n"
        + build_companies_js(companies)
        + "\n// @@SYNC:COMPANIES_END"
    )
    html, n = re.subn(
        r"// @@SYNC:COMPANIES_START\n.*?// @@SYNC:COMPANIES_END",
        block,
        html,
        flags=re.DOTALL,
    )
    print(f"  COMPANIES → {len(companies)} rows ({n} substitution)")
    return html


def patch_funnel(html, funnel_html):
    block = "<!-- @@SYNC:FUNNEL_START -->" + funnel_html + "\n          <!-- @@SYNC:FUNNEL_END -->"
    html, n = re.subn(
        r"<!-- @@SYNC:FUNNEL_START -->.*?<!-- @@SYNC:FUNNEL_END -->",
        block,
        html,
        flags=re.DOTALL,
    )
    print(f"  Funnel rows replaced ({n} substitution)")
    return html


def patch_id_text(html, elem_id, new_text):
    """Replace inner text of the first element with the given id."""
    html, n = re.subn(
        rf'(id="{elem_id}")[^>]*>[^<]*',
        rf'\1>{new_text}',
        html,
    )
    if n == 0:
        print(f"  WARNING: id=\"{elem_id}\" not found in HTML")
    return html


def patch_timestamp(html, now_str):
    html, n = re.subn(
        r'(id="sync-timestamp")[^>]*>[^<]*',
        rf'\1>Updated {now_str}',
        html,
    )
    print(f"  Timestamp → {now_str} ({n} replacement)")
    return html


def patch_footer_date(html, date_str):
    html, _ = re.subn(r"(Generated )[A-Za-z]+ \d+, \d{4}", rf"\1{date_str}", html)
    return html


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("═" * 60)
    print("  Chloe Partner Dashboard — Close CRM Sync")
    print("═" * 60)

    # 1. Partner lead map
    print("\n[1/5] Building partner lead map...")
    partner_map = dict(PARTNER_LEAD_MAP)
    discovered  = try_discover_new_partner_leads(set(partner_map))
    partner_map.update(discovered)
    print(f"  Tracking {len(partner_map)} partner-attributed leads")

    # 2. Fetch opportunities
    print("\n[2/5] Fetching Chloe pipeline opportunities...")
    opps = fetch_all_opportunities()
    print(f"  Total: {len(opps)} opportunities")

    # 3. Map
    companies = [map_opportunity(o, partner_map) for o in opps]
    stage_counts   = Counter(c["stage"]  for c in companies)
    partner_counts = Counter(c["stage"]  for c in companies if c["partner"])
    partner_opps   = [c for c in companies if c["partner"]]
    unique_partners = len({c["partner"] for c in partner_opps})

    print("  Stage breakdown:")
    for stage, cnt in sorted(stage_counts.items(), key=lambda x: -x[1]):
        print(f"    {stage:<22} {cnt}")
    print(f"  Partner-attributed: {len(partner_opps)} opps from {unique_partners} partners")
    for p in partner_opps:
        print(f"    {p['company']:<30} {p['partner']:<25} [{p['stage']}]")

    # 4. Beta waitlist
    print("\n[3/5] Fetching Beta Waitlist count...")
    waitlist_count = fetch_beta_waitlist_count()
    if waitlist_count is not None:
        print(f"  Total: {waitlist_count} leads")

    # 5. Patch HTML
    total = len(companies)
    funnel_html = build_funnel_html(stage_counts, total, partner_counts)

    print(f"\n[4/5] Patching {HTML_FILE}...")
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html = f.read()

    html = patch_companies(html, companies)
    html = patch_funnel(html, funnel_html)

    # Header badges
    html = patch_id_text(html, "badge-total-opps",    f"{total} Chloe Opps")
    html = patch_id_text(html, "badge-partner-count", f"{unique_partners} Active Partners")
    if waitlist_count is not None:
        html = patch_id_text(html, "badge-beta-waitlist", f"{waitlist_count} Beta Waitlist")
        html = patch_id_text(html, "stat-beta-waitlist",  str(waitlist_count))

    # Stat cards
    html = patch_id_text(html, "stat-partner-deals",   str(len(partner_opps)))
    html = patch_id_text(html, "stat-active-partners", str(unique_partners))

    # Timestamps
    now_dt   = datetime.now(timezone.utc)
    now_str  = now_dt.strftime("%b %-d, %Y at %-I:%M %p UTC")
    date_str = now_dt.strftime("%b %-d, %Y")
    html = patch_timestamp(html, now_str)
    html = patch_footer_date(html, date_str)

    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n[5/5] Complete.")
    print("═" * 60)
    print(f"  {total} opps · {len(partner_opps)} partner deals · "
          f"{waitlist_count or '?'} waitlist · {unique_partners} partners")
    print("═" * 60)


if __name__ == "__main__":
    main()
