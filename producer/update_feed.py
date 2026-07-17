#!/usr/bin/env python3
"""Pars feed producer.

Pulls the latest results + upcoming matches for the FIFA World Cup from the
free TheSportsDB v1 API and writes them into data/worldcup.json in the Pars
schema. If the feed content actually changed, it commits and pushes.

Design rules:
- Single official source: TheSportsDB v1 (free test key "123"). No scraping.
- Fail loud: on any network / parse error the script raises and exits non-zero
  WITHOUT touching the existing JSON (no partial / corrupt writes).
- Idempotent commits: only commit+push when the meaningful feed content
  (league / next / matches) differs from what's already on disk.
"""

import json
import os
import subprocess
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
API_KEY = "123"                       # TheSportsDB free test key
LEAGUE_ID = "4429"                    # FIFA World Cup
LEAGUE_TITLE = "Dünya Kupası 2026"    # widget title (feed "league" field)
BASE = f"https://www.thesportsdb.com/api/v1/json/{API_KEY}"
TIMEOUT = 20                          # seconds per request
MAX_PAST = 8                          # most recent finished matches to keep
MAX_NEXT = 4                          # upcoming matches to keep
TR = timezone(timedelta(hours=3))     # Turkey time (UTC+3, no DST)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FEED_PATH = os.path.join(REPO, "data", "worldcup.json")

TR_MONTHS = ["", "Oca", "Şub", "Mar", "Nis", "May", "Haz",
             "Tem", "Ağu", "Eyl", "Eki", "Kas", "Ara"]
TR_DAYS = ["Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz"]

# Round labels keyed by intRound (TheSportsDB soccer coding, best-effort).
ROUND_TR = {
    1: "Grup", 2: "Grup", 3: "Grup",
    125: "Final",
    150: "Yarı Final",
    160: "Çeyrek Final",
    170: "Son 16",
    180: "Son 32",
    200: "Son 16",
}

# Nation name -> Turkish. Falls back to the API's English name if missing.
TR_COUNTRIES = {
    "Argentina": "Arjantin", "Australia": "Avustralya", "Austria": "Avusturya",
    "Belgium": "Belçika", "Bosnia-Herzegovina": "Bosna-Hersek", "Brazil": "Brezilya",
    "Cameroon": "Kamerun", "Canada": "Kanada", "Cape Verde": "Yeşil Burun",
    "Chile": "Şili", "Colombia": "Kolombiya", "Costa Rica": "Kosta Rika",
    "Croatia": "Hırvatistan", "Curaçao": "Curaçao", "Czech Republic": "Çekya",
    "Denmark": "Danimarka", "Ecuador": "Ekvador", "Egypt": "Mısır",
    "England": "İngiltere", "France": "Fransa", "Germany": "Almanya",
    "Ghana": "Gana", "Greece": "Yunanistan", "Haiti": "Haiti",
    "Honduras": "Honduras", "Iceland": "İzlanda", "Iran": "İran",
    "Italy": "İtalya", "Ivory Coast": "Fildişi Sahili", "Jamaica": "Jamaika",
    "Japan": "Japonya", "Mexico": "Meksika", "Morocco": "Fas",
    "Netherlands": "Hollanda", "New Zealand": "Yeni Zelanda", "Nigeria": "Nijerya",
    "Norway": "Norveç", "Panama": "Panama", "Paraguay": "Paraguay",
    "Peru": "Peru", "Poland": "Polonya", "Portugal": "Portekiz",
    "Qatar": "Katar", "Saudi Arabia": "Suudi Arabistan", "Scotland": "İskoçya",
    "Senegal": "Senegal", "Serbia": "Sırbistan", "Slovenia": "Slovenya",
    "South Africa": "Güney Afrika", "South Korea": "Güney Kore", "Spain": "İspanya",
    "Sweden": "İsveç", "Switzerland": "İsviçre", "Tunisia": "Tunus",
    "Turkey": "Türkiye", "Türkiye": "Türkiye", "Ukraine": "Ukrayna",
    "United States": "ABD", "USA": "ABD", "Uruguay": "Uruguay", "Wales": "Galler",
}

# Status buckets
FINISHED = {"FT", "AET", "PEN", "Match Finished", "FT_PEN"}
NOT_STARTED = {"NS", "", None, "Not Started", "TBD"}


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def fetch(path):
    """GET a TheSportsDB endpoint, return parsed JSON. Raise on any problem."""
    url = f"{BASE}/{path}"
    req = urllib.request.Request(url, headers={"User-Agent": "pars-feed/1.0"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status} for {url}")
        raw = r.read().decode("utf-8")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Non-JSON response from {url}: {raw[:120]!r}") from e


def tr_team(name):
    return TR_COUNTRIES.get((name or "").strip(), name or "?")


def event_dt(e):
    """Return a timezone-aware datetime (Turkey time) for the event, or None."""
    ts = e.get("strTimestamp")
    if ts:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(TR)
        except ValueError:
            pass
    date, t = e.get("dateEvent"), e.get("strTime") or "00:00:00"
    if date:
        try:
            dt = datetime.fromisoformat(f"{date}T{t[:8]}").replace(tzinfo=timezone.utc)
            return dt.astimezone(TR)
        except ValueError:
            return None
    return None


def round_label(e):
    try:
        ir = int(e.get("intRound"))
    except (TypeError, ValueError):
        ir = None
    sr = (e.get("strRound") or "").strip()
    if ir in ROUND_TR:
        return ROUND_TR[ir]
    if sr:
        return sr
    if ir is not None:
        return "Grup" if ir < 100 else f"Tur {ir}"
    return "Maç"


def is_live(status):
    return status not in FINISHED and status not in NOT_STARTED


def transform(e):
    """One API event -> our {home, away, score, info} dict (+ sort key)."""
    status = e.get("strStatus")
    hs, as_ = e.get("intHomeScore"), e.get("intAwayScore")
    played = hs is not None and as_ is not None
    dt = event_dt(e)
    rnd = round_label(e)

    if played:
        score = f"{hs} - {as_}"
    else:
        score = "–"

    if is_live(status):
        prog = (e.get("strProgress") or "").strip()
        info = f"{rnd} · {prog}" if prog else f"{rnd} · CANLI"
    elif played:
        info = f"{rnd} · {dt.day} {TR_MONTHS[dt.month]}" if dt else rnd
    else:  # upcoming
        info = (f"{rnd} · {dt.day} {TR_MONTHS[dt.month]} {dt:%H:%M}"
                if dt else rnd)

    return {
        "_dt": dt,
        "_live": is_live(status),
        "_played": played,
        "_round": rnd,
        "home": tr_team(e.get("strHomeTeam")),
        "away": tr_team(e.get("strAwayTeam")),
        "score": score,
        "info": info,
    }


def build_feed():
    past = (fetch(f"eventspastleague.php?id={LEAGUE_ID}").get("events") or [])
    nxt = (fetch(f"eventsnextleague.php?id={LEAGUE_ID}").get("events") or [])

    seen, rows = set(), []
    for e in past + nxt:
        eid = e.get("idEvent")
        if eid in seen:
            continue
        seen.add(eid)
        rows.append(transform(e))

    far = datetime(2100, 1, 1, tzinfo=TR)
    rows.sort(key=lambda r: r["_dt"] or far)

    played = [r for r in rows if r["_played"] and not r["_live"]]
    live = [r for r in rows if r["_live"]]
    upcoming = [r for r in rows if not r["_played"] and not r["_live"]]

    kept = played[-MAX_PAST:] + live + upcoming[:MAX_NEXT]

    # "next": the current focus match — live first, else next upcoming, else last result
    if live:
        f = live[0]
        nxt_text = f"{f['home']}-{f['away']} · {f['info'].split(' · ',1)[-1]}"
    elif upcoming:
        f = upcoming[0]
        dt = f["_dt"]
        when = (f"{TR_DAYS[dt.weekday()]} {dt:%H:%M}" if dt else "")
        nxt_text = f"{f['_round']} · {when}".strip(" ·")
    elif played:
        f = played[-1]
        nxt_text = f"{f['home']} {f['score']} {f['away']}"
    else:
        nxt_text = ""

    matches = [{"home": r["home"], "away": r["away"], "score": r["score"],
                "info": r["info"]} for r in kept]

    return {
        "league": LEAGUE_TITLE,
        "updated": datetime.now(TR).strftime("%Y-%m-%d"),
        "next": nxt_text,
        "matches": matches,
    }


def meaningful(feed):
    """The parts we diff on (everything except the volatile 'updated' date)."""
    return {k: feed[k] for k in ("league", "next", "matches")}


def git(*args):
    return subprocess.run(["git", "-C", REPO, *args],
                          check=True, capture_output=True, text=True)


def main():
    feed = build_feed()
    if not feed["matches"]:
        raise RuntimeError("API returned zero usable matches; refusing to write.")

    old = None
    if os.path.exists(FEED_PATH):
        try:
            old = json.load(open(FEED_PATH, encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            old = None

    if old is not None and meaningful(old) == meaningful(feed):
        print("No change; feed already up to date.")
        return 0

    # atomic write (temp + rename) so a crash can't leave a half file
    tmp = FEED_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, FEED_PATH)
    print(f"Feed updated: {len(feed['matches'])} matches, next='{feed['next']}'")

    git("add", "data/worldcup.json")
    # Guard: if the staged content matches HEAD there is nothing to commit
    # (e.g. the on-disk file drifted but regenerates to the committed state).
    if subprocess.run(["git", "-C", REPO, "diff", "--cached", "--quiet",
                       "--", "data/worldcup.json"]).returncode == 0:
        print("Regenerated feed already matches HEAD; nothing to commit.")
        return 0
    stamp = datetime.now(TR).strftime("%Y-%m-%d %H:%M")
    git("commit", "-m", f"Auto feed update {stamp}")
    git("push", "origin", "HEAD")
    print(f"Committed + pushed at {stamp}.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # fail loud, leave existing JSON untouched
        print(f"update_feed failed: {exc}", file=sys.stderr)
        sys.exit(1)
