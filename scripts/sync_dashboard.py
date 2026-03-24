#!/usr/bin/env python3
"""
sync_dashboard.py
-----------------
Pulls fresh data from Close CRM and rewrites chloe-alpha-dashboard.html
in-place. Runs on schedule via GitHub Actions (8 AM / 12 PM / 5 PM ET).

Required env var:
  CLOSE_API_KEY  — your Close API key (stored as a GitHub Actions secret)
"""

import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone

import requests

# ── Config ────────────────────────────────────────────────────────────────────

CLOSE_API_KEY = os.environ.get('CLOSE_API_KEY', '').strip()
if not CLOSE_API_KEY:
    sys.exit('ERROR: CLOSE_API_KEY environment variable is not set.')

CLOSE_BASE = 'https://api.close.com/api/v1'

# Path to the HTML file (script lives in /scripts, HTML is one level up)
HTML_FILE = os.path.join(os.path.dirname(__file__), '..', 'chloe-alpha-dashboard.html')

# Smart View ID for the Beta Waitlist leads
BETA_WAITLIST_SMART_VIEW_ID = 'save_6HkdoVqth3RjDLaP15yXeAbnVwRI0buetTqpe5UexSd'

# Map Close status labels → the JS stage keys used in the dashboard
STAGE_MAP = {
    'Waitlist':         'waitlist',
    'Targeted':         'targeted',
    'Interested':       'interested',
    'Qualified':        'qualified',
    'Beta Invite':      'betainvite',
    'Beta Invite Sent': 'betainvite',
    'Testing Agent':    'testing',
    'Activated':        'activated',
}


# ── Close API helpers ─────────────────────────────────────────────────────────

def close_get(path, params=None):
    resp = requests.get(
        f'{CLOSE_BASE}{path}',
        auth=(CLOSE_API_KEY, ''),
        params=params or {},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def find_chloe_pipeline_id():
    """Return the pipeline ID whose name contains 'chloe' (case-insensitive)."""
    data = close_get('/pipeline/')
    for pipeline in data.get('data', []):
        if 'chloe' in pipeline.get('name', '').lower():
            print(f'  Pipeline: {pipeline["name"]} → {pipeline["id"]}')
            return pipeline['id']
    raise ValueError(
        'No pipeline with "chloe" in the name found. '
        'Check that the API key has access to the correct organization.'
    )


def fetch_pipeline_statuses(pipeline_id):
    """Return all status objects for the given pipeline."""
    data = close_get(f'/pipeline/{pipeline_id}/')
    statuses = data.get('statuses', [])
    print(f'  Pipeline has {len(statuses)} statuses')
    return statuses


def fetch_all_opportunities(pipeline_id):
    """Fetch opportunities per status so we stay scoped to the Chloe pipeline.

    The /opportunity/ endpoint does not support filtering by pipeline_id
    directly — querying by status_id (which is pipeline-scoped) is the
    correct approach.
    """
    statuses = fetch_pipeline_statuses(pipeline_id)
    fields = 'id,lead_name,status_label,status_type,user_name,note,close_date,date_won,date_lost'

    all_opps = []
    for status in statuses:
        skip = 0
        while True:
            batch = close_get('/opportunity/', {
                'status_id': status['id'],
                '_limit':    100,
                '_skip':     skip,
                '_fields':   fields,
            })
            all_opps.extend(batch.get('data', []))
            if not batch.get('has_more'):
                break
            skip += 100
        print(f'  {status["label"]:<25} {len([o for o in all_opps if o.get("status_label") == status["label"]])}')

    return all_opps


def fetch_beta_waitlist_count():
    """Return the total number of leads in the Beta Waitlist Smart View."""
    try:
        data = close_get('/lead/', {
            'saved_search_id': BETA_WAITLIST_SMART_VIEW_ID,
            '_limit':  1,
            '_fields': 'id',
        })
        return data.get('total_results', 0)
    except Exception as exc:
        print(f'  Warning: could not fetch Beta Waitlist count — {exc}')
        return None


# ── Data transformation ───────────────────────────────────────────────────────

def _safe_str(value, max_len=140):
    """Sanitise a value for embedding in a JS single-quoted string."""
    s = (value or '').strip()
    s = s.replace('\\', '\\\\')
    s = s.replace("'",  "\\'")
    s = s.replace('\r', '')
    s = s.replace('\n', ' ')
    return s[:max_len]


def map_opportunity(opp):
    status_type  = opp.get('status_type', 'active')   # 'active' | 'won' | 'lost'
    status_label = opp.get('status_label', '')

    if status_type == 'won':
        stage = 'activated'
    elif status_type == 'lost':
        stage = 'disqualified'
    else:
        stage = STAGE_MAP.get(status_label, 'waitlist')

    close_date = (
        opp.get('date_won')
        or opp.get('close_date')
        or ''
    )

    return {
        'company': _safe_str(opp.get('lead_name', 'Unknown')),
        'stage':   stage,
        'rep':     _safe_str(opp.get('user_name', '—')),
        'note':    _safe_str(opp.get('note') or ''),
        'closeAt': close_date[:10] if close_date else '',
    }


def build_companies_js(companies):
    """Render the COMPANIES array as a JavaScript literal."""
    lines = ['const COMPANIES = [']
    for c in companies:
        lines.append(
            f"  {{ company:'{c['company']}', stage:'{c['stage']}', "
            f"rep:'{c['rep']}', note:'{c['note']}', closeAt:'{c['closeAt']}' }},"
        )
    lines.append('];')
    return '\n'.join(lines)


# ── HTML patching ─────────────────────────────────────────────────────────────

def patch_companies(html, companies):
    """Replace the COMPANIES array between @@SYNC markers."""
    new_block = (
        '// @@SYNC:COMPANIES_START\n'
        + build_companies_js(companies)
        + '\n// @@SYNC:COMPANIES_END'
    )
    html, n = re.subn(
        r'// @@SYNC:COMPANIES_START\n.*?// @@SYNC:COMPANIES_END',
        new_block,
        html,
        flags=re.DOTALL,
    )
    print(f'  COMPANIES array replaced ({n} substitution(s), {len(companies)} rows)')
    return html


def patch_beta_waitlist_kpi(html, count):
    """Update the Beta Waitlist KPI card value."""
    html, n = re.subn(
        r'(<div class="kpi-value" id="kpi-betawaitlist">)\d+(</div>)',
        rf'\g<1>{count}\2',
        html,
    )
    print(f'  Beta Waitlist KPI → {count} ({n} replacement(s))')
    return html


def patch_timestamp(html, now_str):
    """Replace the @@SYNC:TIMESTAMP line with the actual sync time."""
    html, n = re.subn(
        r"document\.getElementById\('asOfBadge'\)\.textContent\s*=\s*'[^']*';[^\n]*",
        f"document.getElementById('asOfBadge').textContent = 'Last synced {now_str}'; // @@SYNC:TIMESTAMP",
        html,
    )
    print(f'  Timestamp → {now_str} ({n} replacement(s))')
    return html


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print('═' * 56)
    print('  Chloe Alpha Dashboard — Close CRM Sync')
    print('═' * 56)

    # 1. Discover pipeline
    print('\n[1/4] Finding Chloe pipeline...')
    pipeline_id = find_chloe_pipeline_id()

    # 2. Fetch opportunities
    print('\n[2/4] Fetching opportunities...')
    opps = fetch_all_opportunities(pipeline_id)
    companies = [map_opportunity(o) for o in opps]
    stage_counts = Counter(c['stage'] for c in companies)
    print(f'  Total: {len(companies)} opportunities')
    for stage, count in sorted(stage_counts.items(), key=lambda x: -x[1]):
        print(f'    {stage:<20} {count}')

    # 3. Fetch Beta Waitlist count
    print('\n[3/4] Fetching Beta Waitlist count...')
    waitlist_count = fetch_beta_waitlist_count()
    if waitlist_count is not None:
        print(f'  Total: {waitlist_count} leads')

    # 4. Patch HTML
    print(f'\n[4/4] Patching {HTML_FILE}...')
    with open(HTML_FILE, 'r', encoding='utf-8') as f:
        html = f.read()

    html = patch_companies(html, companies)

    if waitlist_count is not None:
        html = patch_beta_waitlist_kpi(html, waitlist_count)

    now_str = datetime.now(timezone.utc).strftime('%b %-d, %Y at %-I:%M %p UTC')
    html = patch_timestamp(html, now_str)

    with open(HTML_FILE, 'w', encoding='utf-8') as f:
        f.write(html)

    print('\n' + '═' * 56)
    print('  Sync complete.')
    print('═' * 56)


if __name__ == '__main__':
    main()
