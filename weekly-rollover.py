#!/usr/bin/env python3
"""
weekly-rollover.py — VPINHUB Golden Tee weekly challenge rollover.

Runs the scraper for fresh data, then:
  - Archives this week's results from the leaderboard
  - Picks a new random GT 2019 course
  - Posts to Discord via webhook
  - Updates weekly_challenge.json

Environment variables:
  DISCORD_WEBHOOK_URL  — Discord webhook URL (required for posting)
  DRY_RUN              — Set to 'true' to preview without saving/posting
"""

import json
import os
import random
import re
import sys
from datetime import datetime, timedelta, timezone
import urllib.request
import urllib.error

LEADERBOARD_FILE = "golden_tee_leaderboard.json"
WEEKLY_FILE = "weekly_challenge.json"
COURSES_FILE = "GT_2019_Courses.txt"
COURSE_DATA_FILE = "course_data.json"
LEADERBOARD_URL = "https://vpinhub.github.io/gt/#weekly"

PLACE_EMOJI = {1: ":first_place:", 2: ":second_place:", 3: ":third_place:"}

GENERIC_INTROS = [
    "Attention GT Ballers! This week we're heading to {course} — study the layout, pick your lines, and post your best round before the deadline!",
    "Attention GT Ballers! The next challenge is set: {course}. Step up, dial in your game, and show everyone what you've got!",
    "Attention GT Ballers! This week's course is {course}. Time to grip it and rip it — good luck out there!",
    "Attention GT Ballers! We're taking on {course} this week. Plan your round wisely and make every shot count!",
    "Attention GT Ballers! {course} is on the board for this week's challenge. Lock in your best score before Friday!",
    "Attention GT Ballers! This week we tackle {course}. Manage the course, avoid the trouble spots, and post that score!",
    "Attention GT Ballers! It's time to hit the links at {course}. Bring your A-game and chase that top spot on the board!",
]

GENERIC_TIPS = [
    "Keep a close eye on the wind and manage your distances carefully on your approach shots to stay out of trouble and secure those birdies!",
    "Play smart off the tee and leave yourself with uphill putts for easier scoring opportunities.",
    "Course management is key — avoid the hazards and let your short game do the work!",
    "Focus on keeping the ball in the fairway and attack pins from the correct angle for easy birdies.",
    "Stay patient and trust your swing. A bogey is just a bump — bounce back and keep scoring!",
    "Use the terrain to your advantage and always play to your strengths. Eagle opportunities await!",
    "Short-game precision wins championships here. Get up-and-down and keep the momentum going!",
    "Read the greens carefully — slope and speed are everything when it comes to making clutch putts.",
    "Pick your battles wisely. Sometimes laying up is smarter than going for the hero shot.",
    "Accuracy off the tee beats distance every time on this layout. Keep it in play and attack from the fairway.",
]


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_courses(path):
    courses = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            course = re.sub(r"^\d+[\t\s]*", "", line).strip()
            if course:
                courses.append(course)
    return courses


def parse_date(date_str):
    """Parse TeknoParrot date strings into UTC datetime.

    Handles two formats:
      DD-MM-YYYY [HH:MM:SS [AM/PM]]   (day first, dash-separated)
      MM/DD/YYYY [HH:MM:SS [AM/PM]]   (month first, slash-separated)
    """
    if not date_str:
        return None
    parts = date_str.strip().split()
    if not parts:
        return None
    date_part = parts[0]
    time_part = parts[1] if len(parts) > 1 else None
    ampm = parts[2].upper() if len(parts) > 2 else None

    try:
        if "-" in date_part:
            d, m, y = date_part.split("-")
            day, month, year = int(d), int(m), int(y)
        else:
            components = date_part.split("/")
            month, day, year = int(components[0]), int(components[1]), int(components[2])
    except (ValueError, IndexError):
        return None

    if year < 100:
        year += 2000

    hours, minutes, seconds = 0, 0, 0
    if time_part:
        try:
            tc = time_part.split(":")
            hours, minutes = int(tc[0]), int(tc[1])
            seconds = int(tc[2]) if len(tc) > 2 else 0
            if ampm == "PM" and hours < 12:
                hours += 12
            elif ampm == "AM" and hours == 12:
                hours = 0
        except (ValueError, IndexError):
            pass

    try:
        return datetime(year, month, day, hours, minutes, seconds, tzinfo=timezone.utc)
    except ValueError:
        return None


def ordinal(n):
    if 11 <= n % 100 <= 13:
        return f"{n}th"
    suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def format_deadline(utc_dt):
    """Format a UTC datetime (which is Friday at 16:00 UTC = 12pm EDT) as display text."""
    day_name = utc_dt.strftime("%A")
    month_name = utc_dt.strftime("%B")
    return f"{day_name}, {month_name} {ordinal(utc_dt.day)} @ 12:00 PM ET"


def next_friday_noon_utc(from_utc=None):
    """Return the next Friday at 16:00 UTC (= 12:00 PM EDT, UTC-4)."""
    if from_utc is None:
        from_utc = datetime.now(timezone.utc)
    # weekday(): Monday=0 ... Friday=4
    days_until_friday = (4 - from_utc.weekday()) % 7
    candidate = (from_utc + timedelta(days=days_until_friday)).replace(
        hour=16, minute=0, second=0, microsecond=0
    )
    if candidate <= from_utc:
        candidate += timedelta(days=7)
    return candidate


def get_results_for_week(leaderboard, course, game_year, week_start_iso, week_end_iso):
    """Return per-player best scores for the current weekly challenge."""
    week_start = datetime.fromisoformat(week_start_iso).replace(tzinfo=timezone.utc)
    week_end = datetime.fromisoformat(week_end_iso).replace(tzinfo=timezone.utc)

    player_best = {}
    for entry in leaderboard:
        if not entry.get("course") or entry["course"] != course:
            continue
        if game_year not in entry.get("game", ""):
            continue
        entry_date = parse_date(entry.get("date", ""))
        if not entry_date or not (week_start <= entry_date <= week_end):
            continue
        player = entry.get("username") or entry.get("query_user_id") or "Unknown"
        try:
            score = int(entry.get("total_score", 999))
        except (ValueError, TypeError):
            continue
        if player not in player_best or score < player_best[player]["score_int"]:
            player_best[player] = {
                "player": player,
                "score_vs_par": entry.get("score_vs_par", "E"),
                "total_score": entry.get("total_score", "N/A"),
                "score_int": score,
                "date_raw": entry.get("date", ""),
            }

    results = sorted(player_best.values(), key=lambda x: x["score_int"])
    for i, r in enumerate(results):
        r["rank"] = i + 1
        d = parse_date(r["date_raw"])
        r["date_display"] = f"{d.strftime('%b')} {d.day}" if d else "N/A"
    return results


def build_discord_message(current, results, new_course, new_deadline_text, game_year,
                          course_data=None, new_season_num=0, new_season_week=0, weeks_per_season=6):
    week_start_dt = datetime.fromisoformat(current["start"])
    week_date_str = f"{week_start_dt.strftime('%B')} {week_start_dt.day}, {week_start_dt.year}"
    course = current["course"]

    lines = []
    lines.append(f":trophy: FINAL RESULTS: {course.upper()} ({week_date_str}) :trophy:")
    lines.append("The week is up, the dust has settled on the fairways, and the final scores are officially locked in.")
    lines.append("")

    if not results:
        lines.append(f"No scores were submitted for **{course}** this week. We'll catch them next time!")
    else:
        count = len(results)
        if count == 1:
            lines.append(f"One brave player took on the challenging terrain of {course} this week. Here is your final standing:")
        else:
            lines.append(
                f"{count} players braved the challenging terrain of {course} this week, "
                "and we have a clear champion! Here are your final standings:"
            )
        lines.append("")

        for r in results[:3]:
            emoji = PLACE_EMOJI.get(r["rank"], f"**#{r['rank']}**")
            crown = " :crown:" if r["rank"] == 1 else ""
            lines.append(
                f"{emoji} {ordinal(r['rank'])} Place: {r['player']} | "
                f"{r['score_vs_par']} (Total: {r['total_score']}){crown}"
            )

        lines.append("")
        winner = results[0]["player"]
        lines.append(
            f"Congratulations to @{winner} for a spectacular performance "
            f"and for conquering {course} this week!"
        )
        lines.append("")
        lines.append("Final Leaderboard:")
        for r in results:
            lines.append(
                f"#{r['rank']} {r['player']} | {r['score_vs_par']} "
                f"(Total: {r['total_score']}) {r['date_display']} @{r['player']}"
            )

    lines.append("")
    cd = (course_data or {}).get(new_course, {})
    course_intro = cd.get("intro", "").strip()
    course_tip = cd.get("tip", "").strip()

    lines.append(f":deciduous_tree: NEW WEEKLY CHALLENGE: {new_course.upper()} :man_golfing:")
    lines.append(course_intro if course_intro else random.choice(GENERIC_INTROS).format(course=new_course))
    lines.append("")
    lines.append(f":golf: Course:   {new_course}")
    lines.append(f":video_game: Game:     Golden Tee Unplugged {game_year}")
    if new_season_num > 0:
        lines.append(f":calendar: Season:   Season {new_season_num} · Week {new_season_week} of {weeks_per_season}")
    lines.append(f":alarm_clock: Deadline: {new_deadline_text}")
    lines.append("")
    lines.append(f"PRO-TIP: {course_tip if course_tip else random.choice(GENERIC_TIPS)}")
    lines.append("")
    lines.append(":bar_chart: View the Live Leaderboard:")
    lines.append(LEADERBOARD_URL)

    return "\n".join(lines)


def post_to_discord(message, webhook_url):
    payload = json.dumps({"content": message}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as response:
            print(f"Discord: {response.status} {response.reason}")
    except urllib.error.HTTPError as e:
        print(f"Discord HTTP error {e.code}: {e.read().decode()}", file=sys.stderr)
    except urllib.error.URLError as e:
        print(f"Discord URL error: {e}", file=sys.stderr)


def pick_new_course(all_courses, used_courses):
    available = [c for c in all_courses if c not in used_courses]
    if not available:
        # All courses used — reset, keeping the last 3 to avoid immediate repeats
        recent = set(used_courses[-3:]) if len(used_courses) >= 3 else set(used_courses)
        available = [c for c in all_courses if c not in recent]
    return random.choice(available)


def update_standings(standings, results):
    for r in results:
        player = r["player"]
        if player not in standings:
            standings[player] = {"wins": 0, "top3": 0, "played": 0}
        standings[player]["played"] += 1
        if r["rank"] == 1:
            standings[player]["wins"] += 1
        if r["rank"] <= 3:
            standings[player]["top3"] += 1
    return standings


def load_course_data():
    try:
        with open(COURSE_DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Strip the meta key
        return {k: v for k, v in data.items() if not k.startswith("_")}
    except FileNotFoundError:
        return {}


def main():
    dry_run = os.environ.get("DRY_RUN", "false").lower() == "true"
    if dry_run:
        print("=== DRY RUN MODE — no files will be saved, no Discord post ===")

    weekly = load_json(WEEKLY_FILE)
    leaderboard = load_json(LEADERBOARD_FILE)
    all_courses = load_courses(COURSES_FILE)
    course_data = load_course_data()

    current = weekly["current"]
    course = current["course"]
    game_year = current["game_year"]
    week_start = current["start"]
    week_end = current["end"]

    print(f"Rolling over: {course} ({game_year})")
    print(f"Week: {week_start} — {week_end}")

    results = get_results_for_week(leaderboard, course, game_year, week_start, week_end)
    print(f"Found {len(results)} unique player score(s)")
    for r in results:
        print(f"  #{r['rank']} {r['player']}  {r['score_vs_par']} ({r['total_score']})")

    # Archive to history
    season_config = weekly.get("season_config", {})
    weeks_per_season = season_config.get("weeks_per_season", 6)
    first_season_start = season_config.get("first_season_start", "")
    current_season = weekly.get("current_season", {"number": 0, "week": 0})
    season_num = current_season.get("number", 0)
    season_week = current_season.get("week", 0)

    history_entry = {
        "week_start": week_start[:10],
        "course": course,
        "game": current.get("game", f"Golden Tee Unplugged {game_year}"),
        "season": season_num,
        "season_week": season_week,
        "start": week_start,
        "end": week_end,
        "results": [
            {
                "rank": r["rank"],
                "player": r["player"],
                "score_vs_par": r["score_vs_par"],
                "total_score": r["total_score"],
                "date": r.get("date_display", ""),
            }
            for r in results
        ],
        "winner": results[0]["player"] if results else None,
    }
    weekly.setdefault("history", []).insert(0, history_entry)

    # Update all-time standings
    weekly["standings"] = update_standings(weekly.get("standings", {}), results)

    # Update season standings and advance season
    now_utc = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")

    if season_num == 0:
        # Check if first season should begin with this rollover
        if first_season_start and today_str >= first_season_start:
            new_season_num = 1
            new_season_week = 1
            weekly["season_standings"] = update_standings({}, results)
            print(f"Season 1 begins!")
        else:
            new_season_num = 0
            new_season_week = 0
            print("Pre-season rollover — season has not started yet.")
    else:
        # We're mid-season — update season standings first
        weekly["season_standings"] = update_standings(
            weekly.get("season_standings", {}), results
        )
        if season_week >= weeks_per_season:
            # Season complete — archive it
            season_standings = weekly["season_standings"]
            champion = None
            if season_standings:
                champion = max(
                    season_standings.items(),
                    key=lambda x: (x[1]["wins"], x[1]["top3"])
                )[0]
            completed = {
                "number": season_num,
                "weeks": season_week,
                "champion": champion,
                "standings": season_standings,
            }
            weekly.setdefault("seasons", []).append(completed)
            new_season_num = season_num + 1
            new_season_week = 1
            weekly["season_standings"] = {}
            print(f"Season {season_num} complete! Champion: {champion}. Starting Season {new_season_num}.")
        else:
            new_season_num = season_num
            new_season_week = season_week + 1
            print(f"Season {season_num} Week {new_season_week} of {weeks_per_season}.")

    weekly["current_season"] = {"number": new_season_num, "week": new_season_week}

    # Pick new course (respect a one-time forced override if set)
    used = weekly.get("used_courses_2019", [])
    forced = weekly.pop("forced_next_course", None)
    new_course = forced if forced else pick_new_course(all_courses, used)
    used.append(new_course)
    weekly["used_courses_2019"] = used

    # Calculate new week window (now → next Friday 12pm ET)
    new_end_utc = next_friday_noon_utc(now_utc)
    new_start_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%S")
    new_end_iso = new_end_utc.strftime("%Y-%m-%dT%H:%M:%S")
    new_deadline_text = format_deadline(new_end_utc)

    weekly["current"] = {
        "course": new_course,
        "game": f"Golden Tee Unplugged {game_year}",
        "game_year": game_year,
        "season": new_season_num,
        "season_week": new_season_week,
        "start": new_start_iso,
        "end": new_end_iso,
        "deadline_text": new_deadline_text,
    }

    discord_message = build_discord_message(
        current, results, new_course, new_deadline_text, game_year,
        course_data, new_season_num, new_season_week, weeks_per_season
    )
    print("\n=== DISCORD MESSAGE PREVIEW ===")
    print(discord_message)
    print("===============================\n")

    # Always write the preview file so admin.html (and the Actions log) can show it
    with open("discord_preview.txt", "w", encoding="utf-8") as f:
        f.write(discord_message)
    print("Saved discord_preview.txt")

    if dry_run:
        print("[DRY RUN] Skipped saving weekly_challenge.json and Discord post.")
        return

    save_json(WEEKLY_FILE, weekly)
    print(f"Saved {WEEKLY_FILE}  →  new course: {new_course} | deadline: {new_deadline_text}")

    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    if webhook_url:
        post_to_discord(discord_message, webhook_url)
    else:
        print("DISCORD_WEBHOOK_URL not set — skipping Discord post (copy from admin.html or discord_preview.txt)")


if __name__ == "__main__":
    main()
