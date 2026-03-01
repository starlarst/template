# dashboard_app.py
from flask import Flask, render_template, jsonify
import json
import os
import threading
import time
from datetime import datetime
import sqlite3
from database import init_db

# --- Configuration ---
app = Flask(__name__)
GAME_DATA_FILE = 'game_data.json'  # Path to your bot's save file
UPDATE_INTERVAL = 30  # Seconds between data refreshes (adjust as needed)

# Global variable to store the loaded data
cached_data = {}
last_updated = None
lock = threading.Lock() # To prevent race conditions when updating cached_data

def load_game_data():
    """Load game data from the JSON file."""
    global cached_data, last_updated
    try:
        # Ensure DB exists and tables are ready
        init_db()
        conn = sqlite3.connect('veyra.db')
        cursor = conn.cursor()

        # Load players into a dict similar to previous JSON shape
        players = {}
        cursor.execute("SELECT user_id, data FROM players")
        for user_id, data_str in cursor.fetchall():
            try:
                players[str(user_id)] = json.loads(data_str)
            except Exception:
                players[str(user_id)] = {}

        # Load guilds
        guilds = {}
        cursor.execute("SELECT name, data FROM guilds")
        for name, data_str in cursor.fetchall():
            try:
                guilds[name] = json.loads(data_str)
            except Exception:
                guilds[name] = {}

        # Load world state
        world_state = {}
        cursor.execute("SELECT data FROM world_state WHERE id = 1")
        row = cursor.fetchone()
        if row:
            try:
                world_state = json.loads(row[0])
            except Exception:
                world_state = {}

        new_data = {
            'players': players,
            'guilds': guilds,
            'world_state': world_state,
            'trading_post': [],
            'auction_house': []
        }
        conn.close()
        with lock:
            cached_data = new_data
            last_updated = datetime.now()
        print(f"[Dashboard] Data loaded successfully at {last_updated.strftime('%Y-%m-%d %H:%M:%S')}")
    except json.JSONDecodeError as e:
        print(f"[Dashboard] Error decoding JSON from {GAME_DATA_FILE}: {e}")
        with lock:
            cached_data = {}
            last_updated = datetime.now()
    except Exception as e:
        print(f"[Dashboard] Unexpected error loading data: {e}")
        with lock:
            cached_data = {}
            last_updated = datetime.now()

def data_updater():
    """Background thread function to periodically update the cached data."""
    while True:
        load_game_data()
        time.sleep(UPDATE_INTERVAL)

@app.route('/')
def index():
    """Render the main dashboard page."""
    with lock:
        data = cached_data.copy() # Work with a copy to prevent issues if data changes mid-render
        updated_time = last_updated

    # Calculate statistics
    num_players = len(data.get('players', {}))
    num_guilds = len(data.get('guilds', {}))
    num_coop_parties = len(data.get('coop_parties', {}))
    num_trades = len(data.get('trading_post', []))
    num_auctions = len(data.get('auction_house', []))
    world_state = data.get('world_state', {})

    # Prepare player list for display (limiting fields for clarity)
    players_list = []
    for pid, pdata in data.get('players', {}).items():
        # Safely extract player details
        name = pdata.get('name', 'Unknown')
        level = pdata.get('level', 0)
        char_class = pdata.get('char_class', 'None')
        gold = pdata.get('gold', 0)
        guild = pdata.get('guild', 'None')
        # Calculate XP progress bar percentage
        xp = pdata.get('xp', 0)
        xp_needed = pdata.get('xp_needed', 100)
        xp_percentage = (xp / xp_needed * 100) if xp_needed > 0 else 0
        players_list.append({
            'id': pid,
            'name': name,
            'level': level,
            'class': char_class,
            'gold': gold,
            'guild': guild,
            'xp_percentage': xp_percentage,
            'xp': xp,
            'xp_needed': xp_needed
        })

    # Sort players by level (descending) and then by name (ascending)
    players_list.sort(key=lambda p: (-p['level'], p['name']))

    # Prepare guild list for display (basic info)
    guilds_list = []
    for gname, gdata in data.get('guilds', {}).items():
        member_count = len(gdata.get('members', []))
        guilds_list.append({'name': gname, 'member_count': member_count})

    # Sort guilds by member count (descending) and then by name (ascending)
    guilds_list.sort(key=lambda g: (-g['member_count'], g['name']))

    # Get world state details
    invasion_active = world_state.get('invasion_active', False)
    invasion_details = world_state.get('current_invasion', 'None') if invasion_active else 'Inactive'
    king = world_state.get('king', 'None')
    season = world_state.get('season', 'Normal')
    weather = world_state.get('weather', 'Clear')
    active_events = world_state.get('active_events', [])

    return render_template('dashboard.html',
                           num_players=num_players,
                           num_guilds=num_guilds,
                           num_coop_parties=num_coop_parties,
                           num_trades=num_trades,
                           num_auctions=num_auctions,
                           players=players_list,
                           guilds=guilds_list,
                           invasion_details=invasion_details,
                           king=king,
                           season=season,
                           weather=weather,
                           active_events=active_events,
                           last_updated=updated_time.strftime('%Y-%m-%d %H:%M:%S') if updated_time else 'Never')

if __name__ == '__main__':
    # Start background updater
    updater_thread = threading.Thread(target=data_updater, daemon=True)
    updater_thread.start()

    # Load initial data
    load_game_data()

    print("🎮 Game Dashboard Starting...")

    # IMPORTANT: Railway dynamic port
    port = int(os.environ.get("PORT", 5000))

    app.run(host="0.0.0.0", port=port, debug=False)