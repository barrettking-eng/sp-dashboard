#!/usr/bin/env python3
"""
One-shot script: updates Chloe pipeline opportunity stages based on
Amplitude vs. Close discrepancy analysis (2026-03-24).

Run with:
  CLOSE_API_KEY=your_key python scripts/fix_stages.py
"""

import os, sys
import requests

CLOSE_API_KEY = os.environ.get('CLOSE_API_KEY', '').strip()
if not CLOSE_API_KEY:
    sys.exit('ERROR: CLOSE_API_KEY environment variable is not set.')

BASE_URL = 'https://api.close.com/api/v1'

def close_patch(path, payload):
    url = f'{BASE_URL}{path}'
    r = requests.put(url, json=payload, auth=(CLOSE_API_KEY, ''))
    if r.status_code not in (200, 201):
        print(f'  ✗ PATCH {path} → {r.status_code}: {r.text[:200]}')
        return None
    return r.json()

def update_opp(opp_id, status_id, label, company, from_stage):
    result = close_patch(f'/opportunity/{opp_id}/', {'status_id': status_id})
    if result:
        print(f'  ✓ {company}: {from_stage} → {label}')
    return result

# ── Status IDs (Chloe Implementation pipeline) ────────────────────────────────
ACTIVATED    = 'stat_dEla0A9nVnmzV5myt6v3cRZAKDZGWAboXMksFeFcC3K'  # won
TESTING      = 'stat_ZlRh04cyFLBWRt97pEpWmWELwnYJR2Q2Tgl1jjogrzZ'  # active

# ── Stage updates ─────────────────────────────────────────────────────────────
print('\n▶ Moving Beta Invite → Activated (setup complete + active call volume)')
to_activated = [
    ('oppo_QYDHUT42ErFYiXddERw7AMeMzgO83fkoouQJL9Aqi0Y', 'Golden Group Commercial Real Estate', 'Beta Invite'),
    ('oppo_Il9Y0TUZY72cV1Wr0WuOCIDvUiYxTd1uMhJ5nizpgOj', 'Every1Drives',                       'Beta Invite'),
    ('oppo_EdwIM28WALpKHsmrmClxx838io6Km7xsOM9T8V8GwCX',  'Achieve Greatness',                  'Beta Invite'),
    ('oppo_4qsdQ4sWfEW5YKVHYyK3YgWVafxwIusphCQh06pOFyq',  'Direct Finance Group',               'Beta Invite'),
    ('oppo_UlUEMfKdcKiSGXSvrAaPIKkT2s8tU6aop3yCIAm6UCE',  'Good Results Home Buyers',           'Beta Invite'),
    ('oppo_xZHf9Fg2BvvBaC70LI14katSQGFaYWzvReSB7KwQxv9',  'Fundolo',                            'Beta Invite'),
    ('oppo_iGCecGxJHvVtRfaky8VvNFsuMt0Uq3YlvIIuSjqMSaD',  'Small Biz Heroes',                  'Beta Invite'),
    ('oppo_d4Z2ZCgt4aHd8AApSfWUX6f2pP6jBE0m9c024CKVv6l',  'Eazy Grease',                        'Testing Agent'),
    ('oppo_4mXGpAvyh5HzmfvOm2JUtanV3CHew58qoxcdZRn9XQb',  'Radix Financial Group',              'Testing Agent'),
]
for opp_id, company, from_stage in to_activated:
    update_opp(opp_id, ACTIVATED, 'Activated', company, from_stage)

print('\n▶ Moving Beta Invite → Testing Agent (active calls, setup not yet complete)')
to_testing = [
    ('oppo_MZfPtQBcXdUObMLKqCOP2FS6ol0PzpSq62AT55oDT4j', 'Zoda Limited',    'Beta Invite'),
    ('oppo_yNTVSmf1uxCkz8KUzzPkORrfLuZgWQ6kqJgoMjpC5zP', 'Dolezal Wealth',  'Beta Invite'),
]
for opp_id, company, from_stage in to_testing:
    update_opp(opp_id, TESTING, 'Testing Agent', company, from_stage)

print('\nDone.\n')
print('Next step: trigger a dashboard sync (or run sync_dashboard.py locally) to')
print('reflect the updated stages in both HTML dashboards.')
