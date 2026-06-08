import requests
from bs4 import BeautifulSoup
import json
import csv
from datetime import datetime
import re
import time
import os
import sys

class TeknoParrotScraper:
    def __init__(self, user_ids=None):
        self.user_ids = []
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        })

        if user_ids:
            if isinstance(user_ids, list):
                self.user_ids = user_ids
            elif isinstance(user_ids, str):
                if os.path.isfile(user_ids):
                    self.user_ids = self.load_users_from_file(user_ids)
                else:
                    self.user_ids = [user_ids]

    def load_users_from_file(self, filepath):
        users = []
        if filepath.endswith('.json'):
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                if isinstance(data, list) and all(isinstance(item, str) for item in data):
                    users = data
                elif isinstance(data, dict):
                    if 'users' in data and isinstance(data['users'], list):
                        users = [u if isinstance(u, str) else u.get('id', '') for u in data['users'] if u]
                    elif 'players' in data and isinstance(data['players'], list):
                        for item in data['players']:
                            if isinstance(item, str):
                                users.append(item)
                            elif isinstance(item, dict):
                                users.append(item.get('id') or item.get('username') or '')
                        users = [u for u in users if u]
            except (json.JSONDecodeError, FileNotFoundError) as e:
                print(f"Error loading {filepath}: {e}")
        elif filepath.endswith('.csv'):
            try:
                with open(filepath, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        uid = row.get('user_id') or row.get('username') or row.get('queryId') or row.get('id')
                        if uid:
                            users.append(uid)
            except FileNotFoundError:
                print(f"Error: File not found at {filepath}")

        if not users:
            print(f"Warning: No user IDs found in {filepath}")
        else:
            print(f"Loaded {len(users)} users from {filepath}")
        return users

    def fetch_page(self, url, as_json=False):
        """Fetch a page with retry logic. Returns text or parsed JSON dict."""
        max_retries = 3
        headers = {}
        if as_json:
            headers['Accept'] = 'application/json'
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30, headers=headers)
                response.raise_for_status()
                if as_json:
                    return response.json()
                return response.text
            except (requests.exceptions.RequestException, ValueError) as e:
                print(f"  Error fetching {url} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
        return None

    def is_golden_tee_game(self, game_name):
        if not game_name:
            return False
        target_games = [
            'golden tee unplugged 2019',
            'golden tee live 2019',
            'golden tee unplugged 2018',
            'golden tee live 2018',
            'golden tee unplugged 2017',
            'golden tee live 2017',
            'golden tee unplugged 2016',
            'golden tee live 2016',
            'power putt live 2013',
            'golden tee live 2007',
            'golden tee live 2006',
        ]
        game_lower = game_name.lower()
        return any(t in game_lower for t in target_games)

    # ------------------------------------------------------------------
    # Entry link extraction — four strategies + debug fallback
    # ------------------------------------------------------------------

    def extract_entry_links(self, html, user_id):
        soup = BeautifulSoup(html, 'html.parser')
        entry_links = []

        # Strategy 1: original <a href> containing "EntrySpecific"
        links = soup.find_all('a', href=re.compile(r'EntrySpecific', re.I))
        for link in links:
            href = link.get('href', '')
            if href:
                if not href.startswith('http'):
                    href = f"https://teknoparrot.com{href}"
                entry_links.append({'url': href, 'game': link.get_text(strip=True)})
        if entry_links:
            print(f"  Strategy 1 found {len(entry_links)} links")
            return entry_links

        # Strategy 2: any <a href> with "/Highscore/Entry" in path
        links = soup.find_all('a', href=re.compile(r'/Highscore/Entry', re.I))
        for link in links:
            href = link.get('href', '')
            if href:
                if not href.startswith('http'):
                    href = f"https://teknoparrot.com{href}"
                entry_links.append({'url': href, 'game': link.get_text(strip=True)})
        if entry_links:
            print(f"  Strategy 2 found {len(entry_links)} links")
            return entry_links

        # Strategy 3: data-href / data-url attributes
        for attr in ('data-href', 'data-url', 'data-link'):
            for elem in soup.find_all(attrs={attr: re.compile(r'Entry', re.I)}):
                href = elem.get(attr, '')
                if not href.startswith('http'):
                    href = f"https://teknoparrot.com{href}"
                entry_links.append({'url': href, 'game': elem.get_text(strip=True)})
        if entry_links:
            print(f"  Strategy 3 found {len(entry_links)} links via data-* attributes")
            return entry_links

        # Strategy 4: embedded JSON in <script> tags (Next.js / React hydration)
        embedded = self._extract_from_script_json(soup)
        if embedded:
            print(f"  Strategy 4 found {len(embedded)} links from embedded JSON")
            return embedded

        # Strategy 5: direct API attempt (JSON endpoint variants)
        api_links = self._try_api_endpoint(user_id)
        if api_links:
            print(f"  Strategy 5 found {len(api_links)} links via API endpoint")
            return api_links

        # Nothing worked — dump HTML for diagnosis
        self._save_debug_html(html, soup, user_id)
        return []

    def _extract_from_script_json(self, soup):
        entry_links = []
        for script in soup.find_all('script'):
            text = script.string or ''
            if not ('Entry' in text or 'entry' in text):
                continue
            # Next.js data block
            if script.get('id') == '__NEXT_DATA__':
                try:
                    data = json.loads(text)
                    self._walk_json_for_entries(data, entry_links)
                except json.JSONDecodeError:
                    pass
                continue
            # Inline JS with embedded JSON arrays/objects
            for match in re.finditer(r'(\{[^<]{20,}\}|\[[^<]{20,}\])', text):
                chunk = match.group(0)
                if 'Entry' not in chunk:
                    continue
                try:
                    data = json.loads(chunk)
                    self._walk_json_for_entries(data, entry_links)
                except json.JSONDecodeError:
                    pass
        return entry_links

    def _walk_json_for_entries(self, node, results, depth=0):
        if depth > 12:
            return
        if isinstance(node, dict):
            for k, v in node.items():
                if isinstance(v, str) and re.search(r'EntrySpecific|/Highscore/Entry', v, re.I):
                    href = v if v.startswith('http') else f"https://teknoparrot.com{v}"
                    game = node.get('game') or node.get('gameName') or node.get('title') or ''
                    results.append({'url': href, 'game': game})
                else:
                    self._walk_json_for_entries(v, results, depth + 1)
        elif isinstance(node, list):
            for item in node:
                self._walk_json_for_entries(item, results, depth + 1)

    def _try_api_endpoint(self, user_id):
        """Try common REST/JSON API patterns that TeknoParrot might use."""
        candidates = [
            f"https://teknoparrot.com/api/Highscore/UserSpecific?queryId={user_id}",
            f"https://teknoparrot.com/api/highscore/userspecific?queryId={user_id}",
            f"https://teknoparrot.com/en/api/Highscore/UserSpecific?queryId={user_id}",
        ]
        for url in candidates:
            data = self.fetch_page(url, as_json=True)
            if not data:
                continue
            links = []
            self._walk_json_for_entries(data, links)
            if links:
                return links
            # Maybe it's a list of entry objects directly
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        url_val = item.get('url') or item.get('href') or item.get('entryUrl')
                        if url_val:
                            if not url_val.startswith('http'):
                                url_val = f"https://teknoparrot.com{url_val}"
                            links.append({'url': url_val, 'game': item.get('game', '')})
                if links:
                    return links
        return []

    def _save_debug_html(self, html, soup, user_id):
        debug_file = f"debug_{user_id}.html"
        try:
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html)
            title_tag = soup.find('title')
            all_hrefs = [a.get('href', '') for a in soup.find_all('a')]
            print(f"  WARNING: No entry links found for {user_id}")
            print(f"  Page title : {title_tag.get_text() if title_tag else 'N/A'}")
            print(f"  Total <a> tags : {len(all_hrefs)}")
            print(f"  Sample hrefs   : {[h for h in all_hrefs if h][:15]}")
            # Check for likely JS-rendered page
            if len(html) < 5000 and not soup.find('table'):
                print("  LIKELY CAUSE: Page appears JavaScript-rendered.")
                print("  The HTML returned by requests has no table/data — try adding Playwright/Selenium.")
            print(f"  Full HTML saved to: {debug_file}")
        except Exception as e:
            print(f"  Could not save debug file: {e}")

    # ------------------------------------------------------------------
    # Scorecard parsing — flexible selectors
    # ------------------------------------------------------------------

    def parse_scorecard(self, html, entry_url):
        soup = BeautifulSoup(html, 'html.parser')
        scorecard_data = {'entry_url': entry_url}

        # Game title
        for tag in ('h1', 'h2', 'title'):
            elem = soup.find(tag)
            if elem:
                text = elem.get_text(strip=True)
                if text:
                    scorecard_data['game'] = text
                    break

        # Username — several possible structures
        username = None
        for pattern in (r'/ProfileViewer/Index/', r'/profile/', r'/user/'):
            link = soup.find('a', href=re.compile(pattern, re.I))
            if link:
                btn = link.find(['button', 'span']) or link
                username = btn.get_text(strip=True) or None
                if username:
                    break
        if not username:
            for sel in ('button.btn-info', 'button.btn-primary', '.player-name', '.username', '.badge'):
                elem = soup.select_one(sel)
                if elem:
                    text = elem.get_text(strip=True)
                    if text and len(text) < 50:
                        username = text
                        break
        if username:
            scorecard_data['username'] = username

        # Scorecard table
        table = (
            soup.find('table', class_=re.compile(r'scorecard', re.I)) or
            soup.find('table', class_=re.compile(r'score', re.I)) or
            soup.find('table')
        )
        if not table:
            return scorecard_data

        holes, distances, pars, player_scores = [], [], [], []
        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else table.find_all('tr')

        for row in rows:
            cells = row.find_all('td')
            if not cells:
                continue
            row_text = [c.get_text(strip=True) for c in cells]
            if not row_text:
                continue
            first = row_text[0].upper().strip()

            if first == 'DISTANCE':
                distances = row_text[1:]
            elif first == 'PAR':
                pars = row_text[1:]
            elif first.startswith('PLAYER'):
                num = first.split()[1] if len(first.split()) > 1 else '1'
                player_scores.append({'player': num, 'scores': row_text[1:]})
            elif first in ('COURSE:', 'COURSE'):
                if len(cells) > 1:
                    scorecard_data['course'] = cells[1].get_text(strip=True)
            elif first in ('DATE:', 'DATE'):
                if len(cells) > 1:
                    scorecard_data['date'] = cells[1].get_text(strip=True)
            elif first in ('CAPTURE ID:', 'CAPTURE ID'):
                if len(cells) > 1:
                    scorecard_data['capture_id'] = cells[1].get_text(strip=True)

        thead = table.find('thead')
        if thead:
            header_row = thead.find('tr')
            if header_row:
                holes = [c.get_text(strip=True) for c in header_row.find_all(['th', 'td'])]

        scorecard_data.update({
            'holes': holes, 'distances': distances, 'pars': pars, 'players': player_scores
        })

        if player_scores:
            p1 = player_scores[0]['scores']
            try:
                scorecard_data['total_score'] = p1[-3] if len(p1) > 3 else None
                scorecard_data['score_vs_par'] = p1[-2] if len(p1) > 2 else None
                scorecard_data['gsp'] = p1[-1] if len(p1) > 0 else None
            except (IndexError, ValueError):
                pass

        # YouTube video
        for card in soup.find_all('div', class_=re.compile(r'card', re.I)):
            header = card.find(['h3', 'h4', 'div'], string=re.compile(r'video', re.I))
            if not header:
                header = card.find(string=re.compile(r'video', re.I))
            iframe = card.find('iframe')
            if iframe and iframe.get('src'):
                src = iframe['src']
                if 'youtube.com/embed/' in src:
                    vid = src.split('youtube.com/embed/')[1].split('?')[0]
                    scorecard_data['youtube_video'] = f"https://www.youtube.com/watch?v={vid}"
                    scorecard_data['youtube_embed'] = src
                else:
                    scorecard_data['youtube_video'] = src
                break

        return scorecard_data

    # ------------------------------------------------------------------
    # Scraping orchestration
    # ------------------------------------------------------------------

    def scrape_user_entries(self, user_id):
        base_url = f"https://teknoparrot.com/en/Highscore/UserSpecific?queryId={user_id}"
        print(f"\n{'=' * 60}\nScraping: {user_id}\n{'=' * 60}")

        html = self.fetch_page(base_url)
        if not html:
            return []

        entry_links = self.extract_entry_links(html, user_id)
        if not entry_links:
            return []

        # Pre-filter to known Golden Tee / Power Putt game IDs so we don't
        # waste a network round-trip on every non-GT entry (arcade racers, etc.)
        GT_GAME_IDS = re.compile(r'gameId=(gt\d+|ppl\d+)', re.I)
        GT_ID_WHITELIST = {'gt06', 'gt07', 'gt16', 'gt17', 'gt18', 'gt19', 'ppl13'}

        def is_gt_url(url):
            m = GT_GAME_IDS.search(url)
            if not m:
                return True  # unknown pattern — fetch to be safe
            return m.group(1).lower() in GT_ID_WHITELIST

        filtered_links = [l for l in entry_links if is_gt_url(l['url'])]
        skipped = len(entry_links) - len(filtered_links)
        if skipped:
            print(f"  Pre-filtered {skipped} non-GT entries; checking {len(filtered_links)}")
        else:
            print(f"  Found {len(filtered_links)} entries to check")
        user_entries = []

        for i, entry_info in enumerate(filtered_links, 1):
            scorecard_html = self.fetch_page(entry_info['url'])
            if not scorecard_html:
                continue

            scorecard_data = self.parse_scorecard(scorecard_html, entry_info['url'])
            if not scorecard_data.get('game'):
                scorecard_data['game'] = entry_info.get('game', '')

            if not self.is_golden_tee_game(scorecard_data.get('game', '')):
                continue

            scorecard_data['scraped_at'] = datetime.now().isoformat()
            scorecard_data['query_user_id'] = user_id
            user_entries.append(scorecard_data)

            video_tag = ' [VIDEO]' if scorecard_data.get('youtube_video') else ''
            print(f"  OK {i}/{len(filtered_links)} | {scorecard_data.get('game')} | {scorecard_data.get('course')} | {scorecard_data.get('total_score')}{video_tag}")
            time.sleep(1)

        return user_entries

    def scrape_all_users(self):
        all_entries = []
        if not self.user_ids:
            return []
        for idx, user_id in enumerate(self.user_ids, 1):
            print(f"\n[User {idx}/{len(self.user_ids)}]")
            all_entries.extend(self.scrape_user_entries(user_id))
            if idx < len(self.user_ids):
                time.sleep(2)
        return all_entries

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    def save_to_csv(self, entries, filename='golden_tee_leaderboard.csv'):
        if not entries:
            return
        flattened = []
        for entry in entries:
            flat = {
                'game': entry.get('game', ''), 'username': entry.get('username', ''),
                'query_user_id': entry.get('query_user_id', ''), 'course': entry.get('course', ''),
                'date': entry.get('date', ''), 'capture_id': entry.get('capture_id', ''),
                'total_score': entry.get('total_score', ''), 'score_vs_par': entry.get('score_vs_par', ''),
                'gsp': entry.get('gsp', ''), 'youtube_video': entry.get('youtube_video', ''),
                'entry_url': entry.get('entry_url', ''), 'scraped_at': entry.get('scraped_at', '')
            }
            if entry.get('players'):
                p1_scores = entry['players'][0].get('scores', [])
                headers = entry.get('holes', [])
                hole_n = 0
                for i, score in enumerate(p1_scores):
                    if (i + 1) < len(headers) and headers[i + 1].isdigit():
                        hole_n += 1
                        flat[f'hole_{hole_n}'] = score
                    elif (i + 1) >= len(headers):
                        break
            flattened.append(flat)

        all_keys = set().union(*(d.keys() for d in flattened))
        standard_keys = ['game', 'username', 'query_user_id', 'course', 'date', 'total_score', 'score_vs_par', 'gsp', 'youtube_video', 'entry_url']
        hole_keys = sorted([k for k in all_keys if k.startswith('hole_')], key=lambda x: int(x.split('_')[1]))

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=standard_keys + hole_keys, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(flattened)
        print(f"\n✓ Saved {filename}")

    def save_to_json(self, entries, filename='golden_tee_leaderboard.json'):
        if not entries:
            return
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(entries, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved {filename}")


def main():
    if getattr(sys, 'frozen', False):
        application_path = os.path.dirname(sys.executable)
    else:
        try:
            application_path = os.path.dirname(os.path.abspath(__file__))
        except Exception:
            application_path = os.path.abspath('.')

    user_json = os.path.join(application_path, "users.json")
    if not os.path.exists(user_json):
        print(f"Error: 'users.json' not found in {application_path}")
        input("Press Enter to exit...")
        return

    scraper = TeknoParrotScraper(user_json)
    entries = scraper.scrape_all_users()

    if entries:
        scraper.save_to_csv(entries)
        scraper.save_to_json(entries)
        print(f"\n{'=' * 60}\nSUMMARY\n{'=' * 60}")
        games = {}
        for e in entries:
            g = e.get('game', 'Unknown')
            games[g] = games.get(g, 0) + 1
        for g, c in games.items():
            print(f"  {g}: {c} entries")
    else:
        print("\nNo entries found.")
        print("Check any debug_<user>.html files for diagnosis.")
        input("Press Enter to exit...")


if __name__ == "__main__":
    main()
