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
        """
        Initialize scraper with list of user IDs
        user_ids: list of user query IDs or path to CSV/JSON file
        """
        self.user_ids = []
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        # Load user IDs from various sources
        if user_ids:
            if isinstance(user_ids, list):
                self.user_ids = user_ids
            elif isinstance(user_ids, str):
                if os.path.isfile(user_ids):
                    self.user_ids = self.load_users_from_file(user_ids)
                else:
                    self.user_ids = [user_ids]

    def load_users_from_file(self, filepath):
        """Load user IDs from CSV or JSON file"""
        users = []

        if filepath.endswith('.json'):
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)

                    if isinstance(data, list) and all(isinstance(item, str) for item in data):
                        users = data
                    elif isinstance(data, dict):
                        if 'users' in data and isinstance(data['users'], list):
                            if all(isinstance(item, str) for item in data['users']):
                                users = data['users']
                        elif 'players' in data and isinstance(data['players'], list):
                            if all(isinstance(item, str) for item in data['players']):
                                users = data['players']
                            elif all(isinstance(item, dict) and 'id' in item for item in data['players']):
                                users = [player['id'] for player in data['players']]
            except json.JSONDecodeError:
                print(f"Error: Could not decode JSON from {filepath}")
            except FileNotFoundError:
                print(f"Error: File not found at {filepath}")

        elif filepath.endswith('.csv'):
            try:
                with open(filepath, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        user_id = row.get('user_id') or row.get('username') or row.get('queryId') or row.get('id')
                        if user_id:
                            users.append(user_id)
            except FileNotFoundError:
                print(f"Error: File not found at {filepath}")

        if not users:
            print(f"Warning: Could not find any user IDs in {filepath}. Check file format.")
        else:
            print(f"Loaded {len(users)} users from {filepath}")

        return users

    def fetch_page(self, url):
        """Fetch a page with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                print(f"Error fetching {url} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    return None

    def is_golden_tee_game(self, game_name):
        """Check if the game is a Golden Tee variant"""
        if not game_name:
            return False
        # UPDATED LIST: Added 2018 variants
        target_games = [
            'golden tee unplugged 2018',
            'golden tee live 2018',
            'golden tee unplugged 2017',
            'golden tee live 2017',
            'golden tee unplugged 2016',
            'power putt live 2013',
            'golden tee live 2007',
            'golden tee live 2006'
        ]
        game_lower = game_name.lower()
        return any(target in game_lower for target in target_games)

    def parse_scorecard(self, html, entry_url):
        """Parse the detailed scorecard page"""
        soup = BeautifulSoup(html, 'html.parser')
        scorecard_data = {'entry_url': entry_url}

        game_title = soup.find('h1')
        if game_title:
            scorecard_data['game'] = game_title.get_text(strip=True)

        username_link = soup.find('a', href=re.compile(r'/ProfileViewer/Index/'))
        if username_link:
            username_btn = username_link.find('button', class_='btn-info')
            if username_btn:
                scorecard_data['username'] = username_btn.get_text(strip=True)

        table = soup.find('table', class_='scorecard-table')
        if not table:
            return scorecard_data

        holes, distances, pars, player_scores = [], [], [], []
        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else table.find_all('tr')

        for row in rows:
            cells = row.find_all('td')
            if not cells: continue
            row_text = [cell.get_text(strip=True) for cell in cells]
            if not row_text: continue
            first_cell = row_text[0].upper()

            if first_cell == 'DISTANCE':
                distances = row_text[1:]
            elif first_cell == 'PAR':
                pars = row_text[1:]
            elif first_cell.startswith('PLAYER'):
                player_num = first_cell.split()[1]
                player_scores.append({'player': player_num, 'scores': row_text[1:]})
            elif first_cell == 'COURSE:':
                if len(cells) > 1: scorecard_data['course'] = cells[1].get_text(strip=True)
            elif first_cell == 'DATE:':
                if len(cells) > 1: scorecard_data['date'] = cells[1].get_text(strip=True)
            elif first_cell == 'CAPTURE ID:':
                if len(cells) > 1: scorecard_data['capture_id'] = cells[1].get_text(strip=True)

        thead = table.find('thead')
        if thead:
            header_row = thead.find('tr')
            if header_row:
                holes = [cell.get_text(strip=True) for cell in header_row.find_all('th')]

        scorecard_data.update({
            'holes': holes, 'distances': distances, 'pars': pars, 'players': player_scores
        })

        if player_scores:
            p1_scores = player_scores[0]['scores']
            if len(p1_scores) > 0:
                try:
                    scorecard_data['total_score'] = p1_scores[-3] if len(p1_scores) > 3 else None
                    scorecard_data['score_vs_par'] = p1_scores[-2] if len(p1_scores) > 2 else None
                    scorecard_data['gsp'] = p1_scores[-1] if len(p1_scores) > 0 else None
                except (IndexError, ValueError): pass

        video_card = soup.find('div', class_='card')
        if video_card:
            header = video_card.find('h3', class_='card-header')
            if header and 'Video' in header.get_text():
                iframe = video_card.find('iframe')
                if iframe and iframe.get('src'):
                    youtube_url = iframe.get('src')
                    if 'youtube.com/embed/' in youtube_url:
                        video_id = youtube_url.split('youtube.com/embed/')[1].split('?')[0]
                        scorecard_data['youtube_video'] = f"https://www.youtube.com/watch?v={video_id}"
                        scorecard_data['youtube_embed'] = youtube_url
                    else:
                        scorecard_data['youtube_video'] = youtube_url

        return scorecard_data

    def extract_entry_links(self, html):
        """Extract entry links from the main leaderboard page"""
        soup = BeautifulSoup(html, 'html.parser')
        entry_links = []
        links = soup.find_all('a', href=re.compile(r'EntrySpecific', re.I))

        for link in links:
            href = link.get('href')
            if href:
                if not href.startswith('http'):
                    href = f"https://teknoparrot.com{href}"
                game_name = link.get_text(strip=True)
                parent = link.find_parent(['tr', 'div'])
                if parent:
                    game_elem = parent.find(text=re.compile('Golden Tee|Power Putt', re.I))
                    if game_elem: game_name = game_elem.strip()

                entry_links.append({'url': href, 'game': game_name})
        return entry_links

    def scrape_user_entries(self, user_id):
        """Scrape all Golden Tee entries for a specific user"""
        base_url = f"https://teknoparrot.com/en/Highscore/UserSpecific?queryId={user_id}"
        print(f"\n{'=' * 60}\nScraping entries for user: {user_id}\n{'=' * 60}")

        html = self.fetch_page(base_url)
        if not html: return []

        entry_links = self.extract_entry_links(html)
        if not entry_links: return []

        print(f"  Found {len(entry_links)} total entries")
        user_entries = []

        for i, entry_info in enumerate(entry_links, 1):
            scorecard_html = self.fetch_page(entry_info['url'])
            if not scorecard_html: continue

            scorecard_data = self.parse_scorecard(scorecard_html, entry_info['url'])
            if not scorecard_data.get('game'):
                scorecard_data['game'] = entry_info['game']

            if not self.is_golden_tee_game(scorecard_data.get('game', '')):
                continue

            scorecard_data['scraped_at'] = datetime.now().isoformat()
            scorecard_data['query_user_id'] = user_id
            user_entries.append(scorecard_data)

            summary = f"  ✓ {i}/{len(entry_links)} | {scorecard_data.get('game')} | {scorecard_data.get('course')} | Score: {scorecard_data.get('total_score')}"
            print(summary + (" 📹" if scorecard_data.get('youtube_video') else ""))
            time.sleep(1)

        return user_entries

    def scrape_all_users(self):
        all_entries = []
        if not self.user_ids: return []
        for idx, user_id in enumerate(self.user_ids, 1):
            print(f"\n[User {idx}/{len(self.user_ids)}]")
            all_entries.extend(self.scrape_user_entries(user_id))
            if idx < len(self.user_ids): time.sleep(2)
        return all_entries

    def save_to_csv(self, entries, filename='golden_tee_leaderboard.csv'):
        if not entries: return
        flattened = []
        for entry in entries:
            flat_entry = {
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
                hole_count = 0
                for i, score in enumerate(p1_scores):
                    if (i + 1) < len(headers) and headers[i+1].isdigit():
                        hole_count += 1
                        flat_entry[f'hole_{hole_count}'] = score
                    elif (i + 1) >= len(headers): break
            flattened.append(flat_entry)

        all_keys = set().union(*(d.keys() for d in flattened))
        standard_keys = ['game', 'username', 'query_user_id', 'course', 'date', 'total_score', 'score_vs_par', 'gsp', 'youtube_video', 'entry_url']
        hole_keys = sorted([k for k in all_keys if k.startswith('hole_')], key=lambda x: int(x.split('_')[1]))
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=standard_keys + hole_keys, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(flattened)
        print(f"\n✓ Saved to {filename}")

    def save_to_json(self, entries, filename='golden_tee_leaderboard.json'):
        if not entries: return
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(entries, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved to {filename}")

def main():
    if getattr(sys, 'frozen', False):
        application_path = os.path.dirname(sys.executable)
    else:
        try: application_path = os.path.dirname(os.path.abspath(__file__))
        except: application_path = os.path.abspath('.')

    user_json_file_path = os.path.join(application_path, "users.json")

    if not os.path.exists(user_json_file_path):
        print(f"Error: 'users.json' not found in: {application_path}")
        input("Press Enter to exit...")
        return

    scraper = TeknoParrotScraper(user_json_file_path)
    entries = scraper.scrape_all_users()

    if entries:
        scraper.save_to_csv(entries)
        scraper.save_to_json(entries)
        print(f"\n{'=' * 60}\nSUMMARY\n{'=' * 60}")
        games = {}
        for e in entries: games[e.get('game', 'Unknown')] = games.get(e.get('game', 'Unknown'), 0) + 1
        for g, c in games.items(): print(f"  {g}: {c} entries")
    else:
        print("\nNo entries found.")
        input("Press Enter to exit...")

if __name__ == "__main__":
    main()
