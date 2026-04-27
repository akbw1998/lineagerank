"""Smoke test — replicates rfp-writer Go HTTP pattern exactly using requests.

Headers sent (matching client.go newAPIRequest):
  x-api-key: <oauth_token>          (from ~/.claude/.credentials.json)
  anthropic-version: 2023-06-01
  content-type: application/json
  x-litellm-api-key: Bearer <key>   (from ANTHROPIC_CUSTOM_HEADERS)

Run outside an active Claude Code conversation:
    /opt/venv/bin/python3 tools/smoke_test_llm.py
"""
from __future__ import annotations
import json
import os
import sys
from pathlib import Path

import requests


MODEL  = os.environ.get("SMOKE_MODEL", "claude-sonnet-4-5")
PROMPT = "Reply with exactly the word: SUCCESS"


def oauth_token() -> str:
    data = json.loads((Path.home() / ".claude" / ".credentials.json").read_text())
    token = data["claudeAiOauth"]["accessToken"]
    if not token:
        raise ValueError("no accessToken in credentials file")
    return token


def main() -> int:
    base_url = os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com").rstrip("/")
    url = f"{base_url}/v1/messages"

    # Auth: OAuth token first, fall back to ANTHROPIC_API_KEY (rfp-writer pattern)
    try:
        token = oauth_token()
        print(f"auth           : OAuth token ({token[:20]}…)")
    except Exception:
        token = os.environ.get("ANTHROPIC_API_KEY", "")
        if not token:
            print("ERROR: no OAuth token and ANTHROPIC_API_KEY not set")
            return 2
        print(f"auth           : ANTHROPIC_API_KEY ({token[:20]}…)")

    # Build headers exactly as rfp-writer does
    headers = {
        "x-api-key":         token,
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json",
    }

    # Apply ANTHROPIC_CUSTOM_HEADERS on top (same loop as Go client.go)
    for line in os.environ.get("ANTHROPIC_CUSTOM_HEADERS", "").split("\n"):
        line = line.strip()
        if not line:
            continue
        if ":" in line:
            k, _, v = line.partition(":")
            headers[k.strip()] = v.strip()

    body = {
        "model":      MODEL,
        "max_tokens": 32,
        "messages":   [{"role": "user", "content": PROMPT}],
    }

    print(f"url            : {url}")
    print(f"model          : {MODEL}")
    print(f"headers sent   : {list(headers.keys())}")
    print()
    print("Making single API call …")

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=30)
        print(f"HTTP status    : {resp.status_code}")

        if resp.status_code == 200:
            data = resp.json()
            text = data["content"][0]["text"].strip()
            print(f"\n✓  Succeeded: {text!r}")
            print("\nAuth is working. Safe to run the full suite.")
            return 0

        data = resp.json()
        print(f"\n✗  Failed: {json.dumps(data)[:400]}")

        if resp.status_code == 429:
            print("\nDIAGNOSIS: Rate limit — auth is correct, quota exhausted.")
            return 1
        elif resp.status_code == 401:
            print("\nDIAGNOSIS: Auth error — credentials or headers wrong.")
            return 2
        else:
            print(f"\nDIAGNOSIS: Unexpected {resp.status_code}")
            return 3

    except requests.Timeout:
        print("✗  Timed out after 30s")
        return 3
    except Exception as exc:
        print(f"✗  {exc}")
        return 3


if __name__ == "__main__":
    sys.exit(main())
