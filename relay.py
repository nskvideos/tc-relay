"""
Multi-client relay for the TokyoCatch claw-machine dashboard — Option B.

Why this exists:
TokyoCatch's live WebSocket (wss://api.tokyocatch.com/subscriptions/v2) only
sends data to connections whose Origin header is https://tokyocatch.com.
Browsers set that header themselves based on the page's real address, so a
dashboard hosted anywhere else can never talk to TokyoCatch directly. This
relay is a small server that DOES connect with the right Origin header, and
forwards the live data down to any number of browsers watching the
dashboard, wherever it's hosted.

What changed from the old (per-room) relay:
Previously, all counting/win-detection/history logic lived in each browser's
JS + localStorage, and the relay just piped raw TokyoCatch messages through.
That meant: (1) counting stopped the moment every browser closed, because
each room's upstream connection was torn down ~30s after the last browser
disconnected, and (2) two people watching the same machine could see two
different counts, since each browser counted independently from whatever
moment it happened to connect.

Now the relay itself owns the shared, durable state for every tracked
machine:
  - play count (`livePlays`)
  - win detection + history
  - the machine list itself (which machineId/prizeId pairs are tracked)
These are broadcast to every connected browser, so everyone always sees the
same numbers, and tracking continues even with zero browsers connected —
tracking only stops when a browser explicitly asks the relay to stop.

Still personal/local per browser (unchanged, lives in that browser's own
localStorage, never sent here): alert threshold + armed/fired state, the
THREE_CLAW fixed-payout tracker's "usual rate" and derived predictions, and
each machine's display name. The relay broadcasts `lastAutoWin` (the play
count a win landed on) so each browser can compute its own payout math
against its own personal rate.

Persistence: the `Store` class below talks to Upstash Redis over its REST
API (a plain HTTPS POST per command — no TCP connection to manage, which
suits this relay's already-async design). It needs two environment
variables set wherever this relay runs: UPSTASH_REDIS_REST_URL and
UPSTASH_REDIS_REST_TOKEN (from the Upstash console's Connect > REST tab).
If they're not set, Store quietly falls back to an in-memory dict instead
of failing outright — handy for a quick local test run, but state in that
mode does NOT survive a restart, so production (Render) must have both env
vars set.

Known remaining gap even with Upstash wired in: Render's free tier fully
sleeps the process after 15 minutes with zero incoming traffic, which
would freeze counting. That needs an external uptime pinger (e.g.
UptimeRobot) hitting this relay's URL every 5-10 minutes — a required
follow-up piece, not optional, given the "always counting" goal.
"""

import asyncio
import json
from http import HTTPStatus
import os
import random
import signal
import time
import httpx
import websockets

TOKYOCATCH_WS_URL = "wss://api.tokyocatch.com/subscriptions/v2"
PORT = int(os.environ.get("PORT", "8788"))
UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")
TRACKED_SET_KEY = "claw:tracked_machines"
MACHINE_KEY_PREFIX = "claw:machine:"
FLUSH_SECONDS = 5      # how often changed machines are written to Upstash
MAX_HISTORY = 300      # step-history rows kept per prize (dashboard shows the latest 80)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def machine_key(machine_id, prize_id):
    return f"{machine_id}:{prize_id}"


# ---------------------------------------------------------------------------
# Persistence layer, backed by Upstash Redis's REST API.
#
# Each command is sent as a single POST to the REST URL itself (not a
# subpath), with the command + args as a JSON array body — e.g.
# POST {url}  body: ["SET", "claw:machine:abc:def", "{...json...}"]
# This form (rather than building the command into the URL path) sidesteps
# any URL-encoding headaches from the JSON blobs we store as values.
#
# Falls back to a plain in-memory dict if the two UPSTASH_* env vars aren't
# set, so `python relay.py` still works for a quick local test without
# Upstash credentials on hand — just without persistence across restarts.
# ---------------------------------------------------------------------------
class Store:
    """State lives in memory (`_cache`) and is the source of truth while the
    relay runs. Upstash is only read at startup (or on a cache miss) and is
    written by a background flush every FLUSH_SECONDS, not on every single
    TokyoCatch message. That removes a GET + parse + SET + SADD per message,
    which was the main source of memory churn and Upstash command usage."""

    def __init__(self):
        self._use_redis = bool(UPSTASH_URL and UPSTASH_TOKEN)
        self._cache = {}      # key -> state dict
        self._known = None    # set of tracked keys (redis mode, loaded lazily)
        self._dirty = set()   # keys changed since the last flush
        if self._use_redis:
            self._client = httpx.AsyncClient(timeout=10)
            log("Store: using Upstash Redis for persistence")
        else:
            log("Store: UPSTASH_REDIS_REST_URL/TOKEN not set — using in-memory storage "
                "(state will NOT survive a restart; fine for local testing, not for production)")

    async def _cmd(self, *args):
        resp = await self._client.post(
            UPSTASH_URL,
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            json=list(args),
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("error"):
            raise RuntimeError(f"Upstash error on {args[0]}: {data['error']}")
        return data.get("result")

    async def _ensure_known(self):
        if self._known is None:
            self._known = set(await self._cmd("SMEMBERS", TRACKED_SET_KEY) or [])

    async def list_keys(self):
        if not self._use_redis:
            return list(self._cache.keys())
        await self._ensure_known()
        return list(self._known)

    async def load(self, key):
        if key in self._cache:
            return self._cache[key]
        if not self._use_redis:
            return None
        await self._ensure_known()
        if key not in self._known:
            return None
        raw = await self._cmd("GET", MACHINE_KEY_PREFIX + key)
        if raw is None:
            return None
        state = json.loads(raw)
        self._cache[key] = state
        return state

    async def save(self, key, state):
        self._cache[key] = state
        if not self._use_redis:
            return
        await self._ensure_known()
        self._dirty.add(key)
        if key not in self._known:
            self._known.add(key)
            await self._cmd("SADD", TRACKED_SET_KEY, key)

    async def delete(self, key):
        self._cache.pop(key, None)
        self._dirty.discard(key)
        if not self._use_redis:
            return
        await self._ensure_known()
        self._known.discard(key)
        await self._cmd("DEL", MACHINE_KEY_PREFIX + key)
        await self._cmd("SREM", TRACKED_SET_KEY, key)

    async def flush(self):
        """Write every changed machine to Upstash (one SET each)."""
        if not self._use_redis or not self._dirty:
            return
        keys = list(self._dirty)
        self._dirty.clear()
        for key in keys:
            state = self._cache.get(key)
            if state is None:
                continue  # deleted since it was marked dirty
            try:
                await self._cmd("SET", MACHINE_KEY_PREFIX + key, json.dumps(state))
            except Exception as e:
                self._dirty.add(key)  # try again next round
                log(f"Store flush failed for {key}: {e}")

    async def flush_loop(self):
        while True:
            await asyncio.sleep(FLUSH_SECONDS)
            try:
                await self.flush()
            except Exception as e:
                log(f"Store flush loop error: {e}")


store = Store()

# Live, non-persisted runtime bits per tracked machine: the upstream
# TokyoCatch connection and its background tasks. Keyed the same as `store`.
# runtime[key] = {"upstream":.., "pump_task":.., "keepalive_task":..}
runtime = {}

# Every currently-connected browser. State updates are broadcast to all of
# them — there's no more per-(machineId,prizeId) subscription filtering,
# since every browser sees every tracked machine now.
browsers = set()


# ---------------------------------------------------------------------------
# Ported from index.html's handleStatusData(). Mirrors it field-for-field,
# minus the parts that depend on personal/local browser state:
#   - checkPlayCountAlert(), beep(), notify() -> personal alert threshold,
#     stays client-side; the client re-derives "did we cross my threshold"
#     from the livePlays value broadcast after every update.
#   - the THREE_CLAW payout-tracker math -> depends on each browser's own
#     "usual rate", stays client-side; the client re-derives it from the
#     `lastAutoWin` count broadcast on a win.
#   - pendingPlayer -> dead code upstream (the manual "who's playing" input
#     was removed from the HTML), so it's dropped here too; player comes
#     only from `currentPlayingUser`.
# Returns True if this call just detected a win (so the caller can log it).
# ---------------------------------------------------------------------------
def trim_history(m):
    """Keep the step history bounded so memory and every saved/broadcast
    message stop growing forever. Counts are stored separately (livePlays,
    lastAutoWin), so trimming old rows doesn't affect them."""
    h = m.get("history")
    if h is not None and len(h) > MAX_HISTORY + 100:
        del h[:-MAX_HISTORY]


MIN_PLAY_GAP = 10  # seconds; two plays/wins on one machine can never be closer


def apply_prize_meta(m, data):
    """Prize title, label, picture etc. — per-prize facts that never touch the
    shared count, so any connection may apply them."""
    # Only MACHINE_INIT carries these — capture once and keep them for the
    # life of the tracked machine.
    if data.get("machineType") is not None:
        m["machineType"] = data["machineType"]
    prize = data.get("prize")
    if prize:
        title = prize.get("title")
        if title:
            if "en" in title:
                m["prizeTitleEn"] = title["en"]
            if "ja" in title:
                m["prizeTitleJa"] = title["ja"]
        if "gemCost" in prize:
            m["gemCost"] = prize["gemCost"]
        # Label such as "LAST_CHANCE" (MACHINE_INIT > data > prize > label).
        # Re-read on every full prize object so it also clears when
        # TokyoCatch removes the label from a prize.
        if "label" in prize or "title" in prize:
            m["prizeLabel"] = prize.get("label")
        # Prize picture (MACHINE_INIT > data > prize > imageUrl).
        # Only overwrite when TokyoCatch actually sends one.
        if prize.get("imageUrl"):
            m["prizeImageUrl"] = prize["imageUrl"]


def handle_status_data(m, data):
    trim_history(m)
    status = data.get("status")
    current_playing_user = data.get("currentPlayingUser")
    player_id = current_playing_user.get("id") if current_playing_user else None
    last_status = m.get("lastStatus")
    won = False

    # A new play begins when status moves INTO "playing" from either
    # play_wait (a fresh play starting) or continue (another attempt in the
    # same session).
    now_ts = time.time()
    if status == "playing" and last_status in ("play_wait", "continue"):
        # A real play takes far longer than MIN_PLAY_GAP, so a second "play"
        # inside that window is an echo of the same one and is not counted.
        if now_ts - (m.get("lastPlayAt") or 0.0) < MIN_PLAY_GAP:
            log(f"{m['machineId']}: ignored a play {now_ts - m['lastPlayAt']:.1f}s after the last one (echo)")
        else:
            m["lastPlayAt"] = now_ts
            m["livePlays"] = m.get("livePlays", 0) + 1
            m.setdefault("history", []).append({
                "t": now_iso(),
                "step": m["livePlays"],
                "sinceWin": m["livePlays"],
                "player": player_id or "",
                "isWin": False,
            })
    # "get" is the confirmed win signal: log it, snapshot the count that led
    # to it, then reset the running counter back to zero.
    elif status == "get" and last_status != "get" and now_ts - (m.get("lastWinAt") or 0.0) >= MIN_PLAY_GAP:
        m["lastWinAt"] = now_ts
        win_player = player_id or ""
        history = m.setdefault("history", [])
        last_row = history[-1] if history else None
        if last_row and not last_row.get("isWin"):
            # this is the same attempt we already logged at the
            # play_wait/continue -> playing transition — flip it to a win
            # instead of adding a separate row for it.
            last_row["isWin"] = True
            if not last_row.get("player") and win_player:
                last_row["player"] = win_player
        else:
            # no matching row to update (e.g. "get" arrived with no prior
            # logged attempt) — log one directly.
            history.append({
                "t": now_iso(),
                "step": m.get("livePlays", 0),
                "sinceWin": m.get("livePlays", 0),
                "player": win_player,
                "isWin": True,
            })
        m["lastAutoWin"] = {"count": m.get("livePlays", 0), "player": win_player, "time": now_iso()}
        won = True
        # THREE_CLAW fixed-payout math, now computed here (shared) rather
        # than by each browser individually — see the payoutAction handler
        # below for how threeClawUsualRate gets set in the first place.
        # Only runs once a usual rate has actually been set for this
        # machine; until then these three fields just stay None.
        if m.get("threeClawUsualRate") is not None:
            prior_payout = m.get("threeClawPayout")
            if prior_payout is None:
                prior_payout = m["threeClawUsualRate"]
            m["threeClawLastPayoutOwed"] = prior_payout - m.get("livePlays", 0)
            m["threeClawPayout"] = m["threeClawUsualRate"] + m["threeClawLastPayoutOwed"]
        m["livePlays"] = 0
    # "playing" -> "continue" means this attempt didn't win yet; no count
    # change. status -> "playable" means the player stopped; no count
    # change either way.

    apply_prize_meta(m, data)

    if status:
        m["lastStatus"] = status
    prev = m.get("lastFields", {})
    m["lastFields"] = {
        "status": status if status is not None else prev.get("status"),
        "viewerCount": data.get("viewerCount", prev.get("viewerCount")),
        "isMaintenance": data.get("isMaintenance", prev.get("isMaintenance", False)),
        "currentPlayingUser": current_playing_user if "currentPlayingUser" in data else prev.get("currentPlayingUser"),
        "queuedUsers": data.get("queuedUsers", prev.get("queuedUsers", [])),
    }
    return won


def new_machine_state(machine_id, prize_id):
    return {
        "machineId": machine_id,
        "prizeId": prize_id,
        "livePlays": 0,
        "lastStatus": None,
        "history": [],
        "lastAutoWin": None,
        "machineType": None,
        "prizeTitleEn": None,
        "prizeTitleJa": None,
        "gemCost": None,
        "prizeLabel": None,
        "prizeImageUrl": None,
        "prizeStale": False,
        "lastFields": {},
        "liveStatus": {"ok": False, "msg": "Connecting…"},
        # Shared (not personal) — an explicit category override. When None,
        # every browser derives the display category itself from
        # prizeTitleEn (e.g. "Figure - ..." -> "Figures"), so most machines
        # never need this set at all; it only exists so someone can move a
        # machine into a different category than its title would imply,
        # with that change visible to everyone watching the dashboard.
        "category": None,
        # THREE_CLAW fixed-payout tracker — shared so one person setting the
        # usual rate benefits everyone watching, instead of each friend
        # having to know how to use it themselves. None until someone sets
        # a usual rate via a payoutAction message.
        "threeClawUsualRate": None,
        "threeClawLastPayoutOwed": None,
        "threeClawPayout": None,
    }


# ---------------------------------------------------------------------------
# Prizes on the same machine share one count. The play count, step history,
# last win, current status and THREE_CLAW payout numbers describe the
# MACHINE, so every prize record on a machineId carries identical copies of
# them. Each prize still has its own upstream connection and its own title,
# picture, label, stale flag and live status. Because the shared `lastStatus`
# is updated by whichever connection sees an event first, the same event
# arriving on a second connection finds the status already set and is not
# counted twice.
# ---------------------------------------------------------------------------
SHARED_FIELDS = ("livePlays", "history", "lastAutoWin", "lastStatus", "lastFields",
                 "lastPlayAt", "lastWinAt",
                 "threeClawUsualRate", "threeClawLastPayoutOwed", "threeClawPayout")


async def siblings_of(machine_id, exclude_key):
    """Loaded state dicts for the other prizes tracked on this machine."""
    out = []
    for k in await store.list_keys():
        if k == exclude_key:
            continue
        o = await store.load(k)
        if o and o.get("machineId") == machine_id:
            out.append(o)
    return out


def shared_sig(m):
    return (m.get("livePlays"), len(m.get("history") or []), m.get("lastAutoWin"),
            m.get("lastStatus"), m.get("threeClawUsualRate"),
            m.get("threeClawLastPayoutOwed"), m.get("threeClawPayout"),
            m.get("lastFields"))


def mirror_shared(src, others):
    """Copy the shared fields from src onto every other prize record. The
    history list is shared by reference, so there is one list per machine."""
    if src.get("history") is None:
        src["history"] = []
    for o in others:
        for f in SHARED_FIELDS:
            o[f] = src.get(f)


def pick_leader(states):
    """Which record's numbers win when records on one machine disagree: the
    one with the most recent win (its count is already after that win), then
    the highest play count."""
    def rank(s):
        win = (s.get("lastAutoWin") or {}).get("time") or ""
        return (win, s.get("livePlays", 0))
    return max(states, key=rank)


async def reconcile_groups():
    """At startup, make every machine's prize records agree before any
    TokyoCatch data arrives. Differences are logged so nothing is silently
    lost."""
    groups = {}
    for k in await store.list_keys():
        m = await store.load(k)
        if m:
            groups.setdefault(m["machineId"], []).append((k, m))
    for machine_id, items in groups.items():
        if len(items) < 2:
            continue
        states = [m for _, m in items]
        leader = pick_leader(states)
        if any(shared_sig(m)[:7] != shared_sig(leader)[:7] for m in states):
            log(f"Reconcile {machine_id}: using prize {leader['prizeId']} "
                f"(plays {leader.get('livePlays', 0)}); others had "
                + ", ".join(f"{m['prizeId'][:6]}={m.get('livePlays', 0)}" for m in states if m is not leader))
        mirror_shared(leader, [m for m in states if m is not leader])
        for k, m in items:
            await store.save(k, m)


async def broadcast(msg):
    if not browsers:
        return
    raw = json.dumps(msg)
    dead = []
    for ws in list(browsers):
        try:
            await ws.send(raw)
        except Exception:
            dead.append(ws)
    for d in dead:
        browsers.discard(d)


# Browsers only ever show the latest 80 step-history rows, so that is all they
# are sent (the relay still stores up to MAX_HISTORY). `historyTotal` carries
# the real row count for the "Step history (N)" heading. This is what keeps a
# new connection from receiving 26 prizes x 300 rows.
HISTORY_TO_BROWSER = 80


def history_rev(h):
    """Changes whenever a row is added or the newest row becomes a win, so the
    dashboard knows when its copy of the history is out of date."""
    if not h:
        return "0"
    last = h[-1]
    return f"{len(h)}:{last.get('t', '')}:{int(bool(last.get('isWin')))}:{last.get('step', '')}"


def slim_state(m):
    """What every browser receives on connect and on every update: the prize
    without its step history (about 9 KB saved per prize). The dashboard asks
    for the history with a getHistory message, and only for cards that are
    expanded."""
    d = dict(m)
    h = m.get("history") or []
    d["history"] = []
    d["historyTotal"] = len(h)
    d["historyRev"] = history_rev(h)
    return d


def history_payload(m):
    h = m.get("history") or []
    return {"machineId": m["machineId"], "prizeId": m["prizeId"],
            "history": h[-HISTORY_TO_BROWSER:], "historyRev": history_rev(h)}


# At most one update per prize per second goes to browsers; anything that
# arrives sooner is folded into a single later send of the newest state.
BROADCAST_MIN_GAP = 1.0
_bcast_last = {}      # prize key -> time of the last send
_bcast_pending = {}   # prize key -> task that will send the newest state


async def broadcast_machine(state):
    if not browsers:
        return
    key = machine_key(state["machineId"], state["prizeId"])
    wait = BROADCAST_MIN_GAP - (time.monotonic() - _bcast_last.get(key, 0.0))
    if wait <= 0:
        _bcast_last[key] = time.monotonic()
        await broadcast({"type": "machineUpdate", "data": slim_state(state)})
        return
    if key in _bcast_pending:
        return

    async def later():
        try:
            await asyncio.sleep(wait)
            _bcast_pending.pop(key, None)
            st = await store.load(key)
            if st is not None and browsers:
                _bcast_last[key] = time.monotonic()
                await broadcast({"type": "machineUpdate", "data": slim_state(st)})
        finally:
            _bcast_pending.pop(key, None)

    _bcast_pending[key] = asyncio.create_task(later())


# ---------------------------------------------------------------------------
# Per-machine upstream connection lifecycle
# ---------------------------------------------------------------------------
async def keepalive(key, upstream):
    """TokyoCatch expects a periodic app-level ping to keep the subscription
    alive. One per upstream connection (bound to THAT connection, so a stale
    task can never keep running after a reconnect)."""
    try:
        while True:
            await asyncio.sleep(15)
            try:
                await upstream.send(json.dumps({"type": "ping"}))
            except Exception:
                break
    except asyncio.CancelledError:
        pass


# Several prizes on one machine each have their own connection, and all of
# them receive the same events. Their timing differs by up to a second or
# more, so letting them all feed one shared status let events arrive out of
# order and be counted twice. Instead ONE connection per machine (the
# "leader") drives the shared count; the others only keep their prize
# details (title, picture, label) current. Leadership passes on if the
# leader goes quiet, is flagged irrelevant, or disconnects.
LEADER_TIMEOUT = 30
_leader = {}   # machineId -> (prize key, time of its last event)


def claim_leader(machine_id, key):
    now = time.monotonic()
    cur = _leader.get(machine_id)
    if cur is None or cur[0] == key or now - cur[1] > LEADER_TIMEOUT:
        _leader[machine_id] = (key, now)
        return True
    return False


def resign_leader(machine_id, key):
    cur = _leader.get(machine_id)
    if cur and cur[0] == key:
        _leader.pop(machine_id, None)


async def pump_upstream(key):
    """Read messages from TokyoCatch for this machine, update shared state,
    persist it, and broadcast the update to every connected browser."""
    rt = runtime[key]
    upstream = rt["upstream"]
    try:
        async for raw in upstream:
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            m = await store.load(key)
            if m is None:
                continue

            if msg.get("error"):
                err = msg["error"]
                if err.get("code") == "IRRELEVANT_PRIZE_FOR_MACHINE":
                    log(f"{key}: TokyoCatch error message: {raw}")
                    if not m.get("prizeStale"):
                        m["prizeStale"] = True
                        resign_leader(m["machineId"], key)
                        # Flips to True only if TokyoCatch actually keeps
                        # sending play data after the error — see below.
                        m["dataSinceStale"] = False
                        m["liveStatus"] = {
                            "ok": False,
                            "msg": "⚠️ Prize no longer on this machine — waiting to see if TokyoCatch keeps sending play data.",
                        }
                else:
                    m["liveStatus"] = {
                        "ok": False,
                        "msg": "Server error: " + (err.get("message") or err.get("code") or "unknown error"),
                    }
                await store.save(key, m)
                await broadcast_machine(m)
                continue

            msg_type = msg.get("type")
            if msg_type in ("MACHINE_INIT", "MACHINE_STATUS_UPDATED", "MACHINE_QUEUE_UPDATED"):
                # Counting continues even when prizeStale is set: the flag
                # only raises a warning on the dashboard. The count is shared
                # by every prize on the machine, so it keeps going whichever
                # of them TokyoCatch is sending data for.
                # Load the machine's other prize records first, then handle the
                # event and mirror the result with no await in between, so two
                # connections can never interleave inside one update.
                if not claim_leader(m["machineId"], key):
                    # Another connection on this machine drives the count.
                    apply_prize_meta(m, msg.get("data") or {})
                    if not m.get("prizeStale"):
                        m["liveStatus"] = {"ok": True, "msg": "Live — last update " + now_iso()}
                    await store.save(key, m)
                    await broadcast_machine(m)
                    continue
                sibs = await siblings_of(m["machineId"], key)
                before = shared_sig(m)
                handle_status_data(m, msg.get("data") or {})
                changed = shared_sig(m) != before
                mirror_shared(m, sibs)
                if m.get("prizeStale"):
                    m["dataSinceStale"] = True
                    m["liveStatus"] = {"ok": False, "msg": "⚠️ Prize no longer on this machine — plays are counted on the machine as a whole. Last update " + now_iso()}
                else:
                    m["liveStatus"] = {"ok": True, "msg": "Live — last update " + now_iso()}
                await store.save(key, m)
                await broadcast_machine(m)
                if changed:
                    for o in sibs:
                        await store.save(machine_key(o["machineId"], o["prizeId"]), o)
                        await broadcast_machine(o)
            # other message types (pings, etc.) are ignored
    except Exception as e:
        log(f"Upstream for {key} closed: {e}")
    finally:
        mid = key.split(":", 1)[0]
        resign_leader(mid, key)
        m = await store.load(key)
        if m is not None:
            m["liveStatus"] = {"ok": False, "msg": "Upstream connection to TokyoCatch closed — reconnecting…"}
            await store.save(key, m)
            await broadcast_machine(m)
        # Stop this connection's keepalive and release the socket.
        ka = rt.get("keepalive_task")
        if ka:
            ka.cancel()
        try:
            await upstream.close()
        except Exception:
            pass
        # Unexpected close (not an explicit stopTracking) — reconnect, and
        # keep retrying with a delay if TokyoCatch isn't accepting us yet.
        if runtime.get(key) is rt:
            runtime.pop(key, None)
            if m is not None:
                asyncio.create_task(reconnect(m["machineId"], m["prizeId"]))


async def start_tracking(machine_id, prize_id, category=None):
    key = machine_key(machine_id, prize_id)
    if key in runtime:
        # already tracking — but still honour a category supplied with the request
        if category:
            await set_category(machine_id, prize_id, category)
        return True

    existing = await store.load(key)
    if existing is None:
        existing = new_machine_state(machine_id, prize_id)
        if category:
            existing["category"] = category
        # A prize joining a machine that is already tracked shares that
        # machine's count, history and payout from the start.
        sibs = await siblings_of(machine_id, key)
        if sibs:
            mirror_shared(pick_leader(sibs), [existing])
        await store.save(key, existing)
    else:
        if category:
            existing["category"] = category
        # resuming a previously-tracked machine (e.g. after a relay
        # restart) — keep its history/count, just reconnect upstream.
        existing["liveStatus"] = {"ok": False, "msg": "Reconnecting…"}
        await store.save(key, existing)

    log(f"Opening upstream connection for {key}")
    try:
        try:
            upstream = await websockets.connect(
                TOKYOCATCH_WS_URL,
                additional_headers={"Origin": "https://tokyocatch.com"},
                compression=None,
            )
        except TypeError:
            # Older/legacy websockets versions use "extra_headers" instead
            # of "additional_headers" for the same thing.
            upstream = await websockets.connect(
                TOKYOCATCH_WS_URL,
                extra_headers={"Origin": "https://tokyocatch.com"},
                compression=None,
            )
    except Exception as e:
        existing["liveStatus"] = {"ok": False, "msg": f"Could not connect to TokyoCatch: {e}"}
        await store.save(key, existing)
        await broadcast_machine(existing)
        return False

    await upstream.send(json.dumps({
        "type": "machineSubscription",
        "data": {"id": machine_id, "prizeId": prize_id, "type": "view"},
    }))
    await upstream.send(json.dumps({
        "type": "initClient",
        "data": {"device": "web", "clientVersion": "44e8d84", "language": "en"},
    }))

    runtime[key] = {"upstream": upstream}
    runtime[key]["pump_task"] = asyncio.create_task(pump_upstream(key))
    runtime[key]["keepalive_task"] = asyncio.create_task(keepalive(key, upstream))

    existing["liveStatus"] = {"ok": True, "msg": "Connected — waiting for machine data…"}
    await store.save(key, existing)
    await broadcast_machine(existing)
    return True


async def reconnect(machine_id, prize_id):
    """Re-open a dropped upstream connection. Waits a moment (with jitter, so
    all prizes don't hit TokyoCatch in the same instant after a mass drop) and
    keeps retrying with a growing delay until it works or tracking is stopped."""
    key = machine_key(machine_id, prize_id)
    delay = 2
    while True:
        await asyncio.sleep(delay + random.random() * 3)
        if await store.load(key) is None:
            return  # tracking was stopped while we were waiting
        if key in runtime:
            return  # something else already reconnected it
        if await start_tracking(machine_id, prize_id):
            return
        delay = min(delay * 2, 60)


async def stop_tracking(machine_id, prize_id):
    key = machine_key(machine_id, prize_id)
    rt = runtime.pop(key, None)
    if rt:
        rt.get("keepalive_task", None) and rt["keepalive_task"].cancel()
        try:
            await rt["upstream"].close()
        except Exception:
            pass
        pump_task = rt.get("pump_task")
        if pump_task:
            pump_task.cancel()
    await store.delete(key)
    log(f"Stopped tracking {key}")
    await broadcast({"type": "machineRemoved", "data": {"machineId": machine_id, "prizeId": prize_id}})


async def set_category(machine_id, prize_id, category):
    """Shared category override, settable by anyone watching the dashboard.
    If the machine isn't tracked yet (e.g. this arrives a moment before its
    startTracking call finishes), create a stub record now — start_tracking
    will see it already exists and only fill in the connection-related
    fields, leaving this category untouched."""
    key = machine_key(machine_id, prize_id)
    m = await store.load(key)
    if m is None:
        m = new_machine_state(machine_id, prize_id)
    m["category"] = category if category else None
    await store.save(key, m)
    log(f"Set category for {key} to {m['category']!r}")
    await broadcast_machine(m)


async def handle_payout_action(machine_id, prize_id, action, value):
    """One consolidated shared-state entry point for the THREE_CLAW payout
    tracker's four controls (usual rate, last-payout-owed, compute,
    reset-to-usual) — mirrors what each button used to do to a browser's
    local prefs, just applied to the relay's shared machine record instead
    so every browser sees the same numbers."""
    key = machine_key(machine_id, prize_id)
    m = await store.load(key)
    if m is None:
        m = new_machine_state(machine_id, prize_id)

    if action == "setRate":
        m["threeClawUsualRate"] = value if isinstance(value, (int, float)) else None
    elif action == "setOwed":
        m["threeClawLastPayoutOwed"] = value if isinstance(value, (int, float)) else None
    elif action == "compute":
        rate = m.get("threeClawUsualRate")
        owed = m.get("threeClawLastPayoutOwed")
        if isinstance(rate, (int, float)) and isinstance(owed, (int, float)):
            m["threeClawPayout"] = rate + owed
    elif action == "reset":
        m["threeClawPayout"] = None
    else:
        return  # unknown action — ignore rather than save a no-op change

    sibs = await siblings_of(machine_id, key)
    mirror_shared(m, sibs)
    await store.save(key, m)
    await broadcast_machine(m)
    for o in sibs:
        await store.save(machine_key(o["machineId"], o["prizeId"]), o)
        await broadcast_machine(o)


async def resume_all_tracked():
    """On relay startup, reconnect upstream for anything the store already
    has recorded as tracked (relevant once Store is backed by Upstash and
    survives restarts)."""
    for key in await store.list_keys():
        m = await store.load(key)
        if m:
            await start_tracking(m["machineId"], m["prizeId"])


# ---------------------------------------------------------------------------
# Browser-facing WebSocket handler
# ---------------------------------------------------------------------------
async def send_full_list(websocket):
    machines = []
    for key in await store.list_keys():
        m = await store.load(key)
        if m:
            machines.append(m)
    await websocket.send(json.dumps({"type": "machineList", "data": [slim_state(m) for m in machines]}))


# One address may open at most CONNECT_MAX connections per CONNECT_WINDOW
# seconds. Each connection costs a full prize list, so a client stuck in a
# reconnect loop used gigabytes. Over the limit, the connection is closed
# before any data is sent.
CONNECT_WINDOW = 60
CONNECT_MAX = 6
CONNECT_BLOCK = 1800    # seconds an address stays blocked once it goes over
_recent_connects = {}   # address -> times of recent connections
_blocked_until = {}     # address -> time the block ends


def client_ip(websocket):
    try:
        fwd = websocket.request_headers.get("X-Forwarded-For")
        if fwd:
            return fwd.split(",")[0].strip()
    except Exception:
        pass
    ra = getattr(websocket, "remote_address", None)
    return ra[0] if ra else "?"


def client_label(websocket):
    try:
        h = websocket.request_headers
        return f"ip={client_ip(websocket)} origin={h.get('Origin', '-')} ua={(h.get('User-Agent') or '-')[:70]}"
    except Exception:
        return f"ip={client_ip(websocket)}"


async def handle_browser(websocket):
    ip = client_ip(websocket)
    now = time.monotonic()
    if now < _blocked_until.get(ip, 0.0):
        try:
            await websocket.close(code=1013, reason="Too many connections")
        except Exception:
            pass
        return
    recent = [t for t in _recent_connects.get(ip, []) if now - t < CONNECT_WINDOW]
    recent.append(now)
    _recent_connects[ip] = recent
    if len(_recent_connects) > 500:
        for k in [k for k, v in _recent_connects.items() if not v or now - v[-1] >= CONNECT_WINDOW]:
            _recent_connects.pop(k, None)
        for k in [k for k, t in _blocked_until.items() if t <= now]:
            _blocked_until.pop(k, None)
    if len(recent) > CONNECT_MAX:
        _blocked_until[ip] = now + CONNECT_BLOCK
        log(f"Blocked {client_label(websocket)} for {CONNECT_BLOCK // 60} min — more than {CONNECT_MAX} connections in {CONNECT_WINDOW}s")
        try:
            await websocket.close(code=1013, reason="Too many connections")
        except Exception:
            pass
        return

    last_history_req = {}   # prize key -> time of this browser's last getHistory
    browsers.add(websocket)
    log(f"Browser connected ({len(browsers)} watching) {client_label(websocket)}")
    try:
        await send_full_list(websocket)
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            msg_type = msg.get("type")
            data = msg.get("data", {}) or {}

            if msg_type == "listMachines":
                await send_full_list(websocket)

            elif msg_type == "getHistory":
                mid, pid = data.get("machineId"), data.get("prizeId")
                if mid and pid:
                    hk = machine_key(mid, pid)
                    if time.monotonic() - last_history_req.get(hk, 0.0) >= 0.3:
                        last_history_req[hk] = time.monotonic()
                        st = await store.load(hk)
                        if st is not None:
                            await websocket.send(json.dumps({"type": "history", "data": history_payload(st)}))

            elif msg_type == "startTracking":
                machine_id = data.get("machineId")
                prize_id = data.get("prizeId")
                category = (data.get("category") or "").strip() or None
                if machine_id and prize_id:
                    asyncio.create_task(start_tracking(machine_id, prize_id, category))

            elif msg_type == "stopTracking":
                machine_id = data.get("machineId")
                prize_id = data.get("prizeId")
                if machine_id and prize_id:
                    asyncio.create_task(stop_tracking(machine_id, prize_id))

            elif msg_type == "setCategory":
                machine_id = data.get("machineId")
                prize_id = data.get("prizeId")
                category = data.get("category")
                if machine_id and prize_id:
                    asyncio.create_task(set_category(machine_id, prize_id, category))

            elif msg_type == "payoutAction":
                machine_id = data.get("machineId")
                prize_id = data.get("prizeId")
                action = data.get("action")
                value = data.get("value")
                if machine_id and prize_id and action:
                    asyncio.create_task(handle_payout_action(machine_id, prize_id, action, value))

            # 'initClient' and 'ping' from the browser are no-ops here — the
            # relay owns its own upstream connections and keepalives
            # independently of any browser.
    except Exception:
        pass
    finally:
        browsers.discard(websocket)
        log(f"Browser disconnected ({len(browsers)} watching)")


# ---------------------------------------------------------------------------
# HEAD support (for UptimeRobot and similar monitors).
#
# The websockets library refuses any HTTP request that isn't a GET with a
# "400 Bad Request" BEFORE process_request ever runs. UptimeRobot's free plan
# pings with HEAD, so it always saw a 400 and showed a permanent "incident".
# This subclass reads the request line itself, and answers HEAD with a plain
# 200. GET and WebSocket handshakes behave exactly as before. If the library
# internals ever differ from what this expects, the import below fails and the
# relay simply falls back to the stock behaviour (monitor shows 400 again, but
# everything else keeps working).
# ---------------------------------------------------------------------------
try:
    from websockets.datastructures import Headers as _Headers
    from websockets.exceptions import InvalidMessage as _InvalidMessage
    from websockets.legacy.exceptions import AbortHandshake as _AbortHandshake
    from websockets.legacy.http import read_headers as _read_headers, read_line as _read_line
    from websockets.legacy.server import WebSocketServerProtocol as _WSProtocol

    class HeadAwareProtocol(_WSProtocol):
        async def read_http_request(self):
            try:
                request_line = await _read_line(self.reader)
                method, raw_path, _version = request_line.split(b" ", 2)
                if method not in (b"GET", b"HEAD"):
                    raise ValueError(f"unsupported HTTP method: {method!r}")
                path = raw_path.decode("ascii", "surrogateescape")
                headers = await _read_headers(self.reader)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise _InvalidMessage("did not receive a valid HTTP request") from exc
            self.path = path
            self.request_headers = headers
            if method == b"HEAD":
                # Same "relay is up" answer a GET gets, minus the body.
                raise _AbortHandshake(HTTPStatus.OK, _Headers(), b"")
            return path, headers

    HEAD_SUPPORT = True
except Exception as _e:  # pragma: no cover
    HeadAwareProtocol = None
    HEAD_SUPPORT = False
    print(f"HEAD support unavailable, using stock websockets behaviour: {_e!r}", flush=True)


async def process_request(path, request_headers):
    """Let plain HTTP requests (e.g. someone opening the URL in a browser tab
    directly, or a hosting platform's health check) get a friendly response
    instead of a WebSocket handshake error."""
    if request_headers.get("Upgrade", "").lower() != "websocket":
        return (200, [], b"TokyoCatch relay is running.\n")
    return None


async def main():
    await reconcile_groups()
    await resume_all_tracked()
    flusher = asyncio.create_task(store.flush_loop())
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    serve_kwargs = {"process_request": process_request}
    if HeadAwareProtocol is not None:
        serve_kwargs["create_protocol"] = HeadAwareProtocol
    async with websockets.serve(
        handle_browser,
        "0.0.0.0",
        PORT,
        **serve_kwargs,
    ):
        log(f"Relay listening on 0.0.0.0:{PORT} (HEAD support: {'on' if HEAD_SUPPORT else 'off'})")
        await stop.wait()  # run until Render asks us to shut down
    flusher.cancel()
    await store.flush()  # don't lose the last few seconds of changes on redeploy
    log("Shut down cleanly")


if __name__ == "__main__":
    asyncio.run(main())
