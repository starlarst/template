# database.py - FINAL FIXED VERSION
import os
import sqlite3
import json
from datetime import datetime, timezone
import time
import threading
import logging
import traceback
import aiosqlite 
# Get absolute path for database
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
DB_PATH = os.path.join(BASE_DIR, "veyra.db")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Lock to serialize concurrent DB operations from threads
_db_lock = threading.Lock()

def init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)

    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")

    cursor = conn.cursor()

    # Traditional tables
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        gold INTEGER DEFAULT 0,
        gems INTEGER DEFAULT 0,
        wish_unlocked INTEGER DEFAULT 0,
        xp INTEGER DEFAULT 0,
        level INTEGER DEFAULT 1,
        last_daily TIMESTAMP,
        last_weekly TIMESTAMP,
        last_wish_time TIMESTAMP,
        last_levelup TIMESTAMP,
        pity_counter INTEGER DEFAULT 0,
        total_pulls INTEGER DEFAULT 0,
        unlocked_characters TEXT DEFAULT '[]',
        unlocked_weapons TEXT DEFAULT '[]',
        UNIQUE(user_id)
    )
    """)
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN wish_unlocked INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:
       cursor.execute("ALTER TABLE inventory ADD COLUMN rarity TEXT")
    except sqlite3.OperationalError:
        pass
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS inventory (
        user_id INTEGER,
        item_name TEXT,
        quantity INTEGER DEFAULT 1,
        PRIMARY KEY (user_id, item_name)
    )
    """)

    # JSON system tables
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS players (
        user_id INTEGER PRIMARY KEY,
        data TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS guilds (
        name TEXT PRIMARY KEY,
        data TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS world_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        data TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS misc (
        key TEXT PRIMARY KEY,
        data TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()

    print("Database initialized successfully")
def ensure_user_exists(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO users (user_id, gold, gems)
        VALUES (?, 0, 0)
    """, (user_id,))

    conn.commit()
    conn.close()


def get_currency(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT gold, gems FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()

    conn.close()

    if row:
        return row[0], row[1]
    return 0, 0


def add_currency(user_id: int, gold=0, gems=0):
    ensure_user_exists(user_id)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE users
        SET gold = gold + ?, gems = gems + ?
        WHERE user_id = ?
    """, (gold, gems, user_id))

    conn.commit()
    conn.close()


def remove_gems(user_id: int, amount: int):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT gems FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()

    if not row or row[0] < amount:
        conn.close()
        return False

    cursor.execute("""
        UPDATE users
        SET gems = gems - ?
        WHERE user_id = ?
    """, (amount, user_id))

    conn.commit()
    conn.close()
    return True
def add_item(user_id: int, item_name: str, quantity: int = 1):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO inventory (user_id, item_name, rarity, quantity)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, item_name)
        DO UPDATE SET quantity = quantity + excluded.quantity
    """, (user_id, item_name, quantity))

    conn.commit()
    conn.close()
def serialize_player(player):
    """Convert Player object to JSON-serializable dictionary"""
    player_dict = {}
    
    for attr_name, attr_value in player.__dict__.items():
        try:
            # Handle datetime objects
            if attr_name in ['last_daily', 'last_weekly', 'last_wish_time', 'last_levelup']:
                if attr_value:
                    if hasattr(attr_value, 'isoformat'):
                        player_dict[attr_name] = attr_value.isoformat()
                    elif isinstance(attr_value, (int, float)):
                        player_dict[attr_name] = datetime.fromtimestamp(attr_value, timezone.utc).isoformat()
                    else:
                        player_dict[attr_name] = str(attr_value)
                else:
                    player_dict[attr_name] = None
            
            # Handle primitives
            elif isinstance(attr_value, (int, float, str, bool, type(None))):
                player_dict[attr_name] = attr_value
            
            # Handle collections (deep copy)
            elif isinstance(attr_value, dict):
                player_dict[attr_name] = json.loads(json.dumps(attr_value, default=str))
            elif isinstance(attr_value, (list, tuple)):
                player_dict[attr_name] = json.loads(json.dumps(list(attr_value), default=str))
            elif isinstance(attr_value, set):
                player_dict[attr_name] = list(attr_value)
            
            # Everything else gets stringified
            else:
                player_dict[attr_name] = str(attr_value)
                
        except Exception as e:
            logging.warning(f"Could not serialize {attr_name}: {e}")
            player_dict[attr_name] = str(attr_value)
    
    return player_dict
def save_all_data(players, guilds, world_state,
                  coop_parties, trading_post, auction_house,
                  clan_wars, tournaments, announcements, suggestions):
    """Synchronous save using standard sqlite3. Safe to call from threads."""
    if players is None or guilds is None:
        logger.error("Attempted to save with None data containers!")
        return

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    cursor = conn.cursor()
    
    try:
        # Save players
        for user_id, player in list(players.items()):
            try:
                player_dict = serialize_player(player)
                cursor.execute("INSERT OR REPLACE INTO players (user_id, data) VALUES (?, ?)",
                               (player.user_id, json.dumps(player_dict, default=str)))
            except Exception as e:
                logger.error(f"Failed to save player {user_id}: {e}")
        
        # Save guilds
        for name, data in guilds.items():
            cursor.execute("INSERT OR REPLACE INTO guilds (name, data) VALUES (?, ?)",
                           (name, json.dumps(data, default=str)))
        
        # Save world state
        ws_copy = world_state.copy()
        for key in ['invasion_time', 'last_update']:
            if key in ws_copy and ws_copy[key] and hasattr(ws_copy[key], 'isoformat'):
                ws_copy[key] = ws_copy[key].isoformat()
        
        cursor.execute("INSERT OR REPLACE INTO world_state (id, data) VALUES (1, ?)",
                       (json.dumps(ws_copy, default=str),))
        
        # Save misc
        aux_items = {
            'coop_parties': coop_parties, 'trading_post': trading_post,
            'auction_house': auction_house, 'clan_wars': clan_wars,
            'tournaments': tournaments, 'announcements': announcements,
            'suggestions': suggestions
        }
        
        for key, obj in aux_items.items():
            cursor.execute("INSERT OR REPLACE INTO misc (key, data) VALUES (?, ?)",
                           (key, json.dumps(obj, default=str)))
        
        conn.commit()
        # print("All game data saved to database (sync)") # Optional: spammy
    except Exception as e:
        logger.error(f"Critical DB save error: {e}")
        conn.rollback()
    finally:
        conn.close()

# ✅ ASYNC SAVE (Optional: Only use if calling directly from an async function without threads)
async def save_all_data_async(players, guilds, world_state,
                              coop_parties, trading_post, auction_house,
                              clan_wars, tournaments, announcements, suggestions):
    """Async version using aiosqlite."""
    async with aiosqlite.connect(DB_PATH) as conn:
        try:
            for user_id, player in list(players.items()):
                try:
                    player_dict = serialize_player(player)
                    await conn.execute("INSERT OR REPLACE INTO players (user_id, data) VALUES (?, ?)",
                                       (player.user_id, json.dumps(player_dict, default=str)))
                except: continue
            
            for name, data in guilds.items():
                await conn.execute("INSERT OR REPLACE INTO guilds (name, data) VALUES (?, ?)",
                                   (name, json.dumps(data, default=str)))
            
            ws_copy = world_state.copy()
            for key in ['invasion_time', 'last_update']:
                if key in ws_copy and ws_copy[key] and hasattr(ws_copy[key], 'isoformat'):
                    ws_copy[key] = ws_copy[key].isoformat()
            
            await conn.execute("INSERT OR REPLACE INTO world_state (id, data) VALUES (1, ?)",
                               (json.dumps(ws_copy, default=str),))
            
            aux_items = {
                'coop_parties': coop_parties, 'trading_post': trading_post,
                'auction_house': auction_house, 'clan_wars': clan_wars,
                'tournaments': tournaments, 'announcements': announcements,
                'suggestions': suggestions
            }
            for key, obj in aux_items.items():
                await conn.execute("INSERT OR REPLACE INTO misc (key, data) VALUES (?, ?)",
                                   (key, json.dumps(obj, default=str)))
            
            await conn.commit()
        except Exception as e:
            logger.error(f"Async save error: {e}")

def load_all_data(players, guilds, world_state,
                  coop_parties, trading_post, auction_house,
                  clan_wars, tournaments, announcements, suggestions,
                  PlayerClass=None):


    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    cursor = conn.cursor()
    
    # 1. Load Players
    cursor.execute("SELECT user_id, data FROM players")
    for row in cursor.fetchall():
        user_id, data = row
        try:
            player_data = json.loads(data)
            if PlayerClass:
                # Create new instance
                name = player_data.get("name", "Unknown")
                gender = player_data.get("gender", "Unknown")
                player_obj = PlayerClass(user_id=int(user_id), name=name, gender=gender)
                
                # ✅ RESTORE ALL ATTRIBUTES from saved data
                for key, value in player_data.items():
                    # Skip methods and special attrs
                    if not key.startswith('_') and key not in ['user_id', 'name', 'gender']:
                        try:
                            setattr(player_obj, key, value)
                        except Exception as e:
                            logging.warning(f"Could not restore {key} for {user_id}: {e}")
                
                players[int(user_id)] = player_obj
            else:
                players[int(user_id)] = player_data
        except Exception as e:
            logging.error(f"Failed to load player {user_id}: {e}")
            traceback.print_exc()

    conn.close()    
# --- Helper Functions (Unchanged) ---
def get_joinable_guilds(user_id=None):

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    cursor = conn.cursor()
    try: cursor.execute("SELECT name, data FROM guilds")
    except: return []
    results = []
    for name, data_str in cursor.fetchall():
        try: g = json.loads(data_str)
        except: g = {}
        members = g.get('members') if isinstance(g, dict) else None
        max_members = g.get('max_members') if isinstance(g, dict) else None
        joinable = True
        if isinstance(members, list) and user_id is not None:
            if int(user_id) in [int(x) for x in members if x is not None]: joinable = False
        if isinstance(max_members, int) and members is not None:
            if len(members) >= max_members: joinable = False
        if joinable:
            results.append({'name': name, 'members': len(members) if members else 0, 'max_members': max_members})
    conn.close()
    return results

def save_guild(name, data_obj):
    with _db_lock:
       

        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.commit()
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO guilds (name, data) VALUES (?, ?)", (name, json.dumps(data_obj, default=str)))
        conn.commit()
        conn.close()

def add_member_to_guild(user_id, guild_name):
    user_id = int(user_id)
    with _db_lock:
        

        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.commit()
        cur = conn.cursor()
        cur.execute("SELECT data FROM guilds WHERE name = ?", (guild_name,))
        row = cur.fetchone()
        if not row: return False, "Guild not found"
        try: g = json.loads(row[0])
        except: g = {}
        members = g.get('members', [])
        if user_id in members: return False, "Already a member"
        if g.get('max_members') and len(members) >= g['max_members']: return False, "Guild full"
        members.append(user_id)
        g['members'] = members
        cur.execute("INSERT OR REPLACE INTO guilds (name, data) VALUES (?, ?)", (guild_name, json.dumps(g, default=str)))
        conn.commit()
        conn.close()
    return True, "Joined successfully"

def remove_member_from_guild(user_id, guild_name):
    user_id = int(user_id)
    with _db_lock:
        

        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.commit()
        cur = conn.cursor()
        cur.execute("SELECT data FROM guilds WHERE name = ?", (guild_name,))
        row = cur.fetchone()
        if not row: return False, "Guild not found"
        try: g = json.loads(row[0])
        except: return False, "Error"
        members = g.get('members', [])
        if user_id not in members: return False, "Not a member"
        members.remove(user_id)
        g['members'] = members
        cur.execute("INSERT OR REPLACE INTO guilds (name, data) VALUES (?, ?)", (guild_name, json.dumps(g, default=str)))
        conn.commit()
        conn.close()
    return True, "Left successfully"

def save_misc(key, data_obj):
    

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO misc (key, data) VALUES (?, ?)", (key, json.dumps(data_obj, default=str)))
    conn.commit()
    conn.close()

def load_misc(key):
   

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    cursor = conn.cursor()
    cursor.execute("SELECT data FROM misc WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    if not row: return None
    try: return json.loads(row[0])
    except: return row[0]

# --- Autosave Logic (FIXED) ---
_autosave_stop_event = None

def safe_backup_db():
    try:
        src = DB_PATH
        bak = DB_PATH + '.bak'
        with sqlite3.connect(src) as src_conn:
            with sqlite3.connect(bak) as bak_conn:
                src_conn.backup(bak_conn)
    except Exception as e:
        logger.error(f"Backup failed: {e}")

# ✅ FIXED: Uses SYNCHRONOUS save_all_data inside the thread
def _autosave_worker(interval, stop_event, players, guilds, world_state,
                     coop_parties, trading_post, auction_house,
                     clan_wars, tournaments, announcements, suggestions):
    print(f"Autosave started (interval={interval}s)")
    while not stop_event.is_set():
        with _db_lock:
            try:
                # Call the SYNC version here. It is safe in a thread.
                save_all_data(
                    players=players, guilds=guilds, world_state=world_state,
                    coop_parties=coop_parties, trading_post=trading_post,
                    auction_house=auction_house, clan_wars=clan_wars,
                    tournaments=tournaments, announcements=announcements,
                    suggestions=suggestions
                )
                safe_backup_db()
            except Exception as e:
                logger.error(f"Autosave error: {e}")
        stop_event.wait(interval)
    print("Autosave stopped")

def start_autosave(interval=25, players=None, guilds=None, world_state=None,
                   coop_parties=None, trading_post=None, auction_house=None,
                   clan_wars=None, tournaments=None, announcements=None, suggestions=None):
    global _autosave_stop_event
    if players is None or guilds is None:
        raise ValueError("Data containers required for autosave")
    if _autosave_stop_event and not _autosave_stop_event.is_set():
        return _autosave_stop_event
    
    _autosave_stop_event = threading.Event()
    t = threading.Thread(
        target=_autosave_worker,
        args=(interval, _autosave_stop_event, players, guilds, world_state,
              coop_parties, trading_post, auction_house, clan_wars,
              tournaments, announcements, suggestions),
        daemon=True
    )
    t.start()
    return _autosave_stop_event

def stop_autosave(event):
    try: event.set()
    except: pass