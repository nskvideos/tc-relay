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
import os
import time
import httpx
import websockets

TOKYOCATCH_WS_URL = "wss://api.tokyocatch.com/subscriptions/v2"
PORT = int(os.environ.get("PORT", "8788"))
UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")
TRACKED_SET_KEY = "claw:tracked_machines"
MACHINE_KEY_PREFIX = "claw:machine:"


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
    def __init__(self):
        self._use_redis = bool(UPSTASH_URL and UPSTASH_TOKEN)
        if self._use_redis:
            self._client = httpx.AsyncClient(timeout=10)
            log("Store: using Upstash Redis for persistence")
        else:
            self._memory = {}
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

    async def list_keys(self):
        if not self._use_redis:
            return list(self._memory.keys())
        result = await self._cmd("SMEMBERS", TRACKED_SET_KEY)
        return result or []

    async def load(self, key):
        if not self._use_redis:
            return self._memory.get(key)
        raw = await self._cmd("GET", MACHINE_KEY_PREFIX + key)
        return json.loads(raw) if raw is not None else None

    async def save(self, key, state):
        if not self._use_redis:
            self._memory[key] = state
            return
        await self._cmd("SET", MACHINE_KEY_PREFIX + key, json.dumps(state))
        await self._cmd("SADD", TRACKED_SET_KEY, key)

    async def delete(self, key):
        if not self._use_redis:
            self._memory.pop(key, None)
            return
        await self._cmd("DEL", MACHINE_KEY_PREFIX + key)
        await self._cmd("SREM", TRACKED_SET_KEY, key)


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
def handle_status_data(m, data):
    status = data.get("status")
    current_playing_user = data.get("currentPlayingUser")
    player_id = current_playing_user.get("id") if current_playing_user else None
    last_status = m.get("lastStatus")
    won = False

    # A new play begins when status moves INTO "playing" from either
    # play_wait (a fresh play starting) or continue (another attempt in the
    # same session).
    if status == "playing" and last_status in ("play_wait", "continue"):
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
    elif status == "get" and last_status != "get":
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


async def broadcast_machine(state):
    await broadcast({"type": "machineUpdate", "data": state})


# ---------------------------------------------------------------------------
# Per-machine upstream connection lifecycle
# ---------------------------------------------------------------------------
async def keepalive(key):
    """TokyoCatch expects a periodic app-level ping to keep the subscription
    alive. One per tracked machine, regardless of how many browsers are
    watching — no need for a per-browser keepalive timer."""
    try:
        while True:
            await asyncio.sleep(15)
            rt = runtime.get(key)
            if not rt:
                break
            try:
                await rt["upstream"].send(json.dumps({"type": "ping"}))
            except Exception:
                break
    except asyncio.CancelledError:
        pass


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
                    if not m.get("prizeStale"):
                        m["prizeStale"] = True
                        m["liveStatus"] = {
                            "ok": False,
                            "msg": "⚠️ Prize no longer on this machine — still counting plays.",
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
                # Counting deliberately CONTINUES even when prizeStale is
                # set: the flag only raises a warning on the dashboard. This
                # keeps the count/history intact if the prize is swapped out
                # and later comes back or new prizes are added. Trade-off:
                # if the machine is really running a different prize, its
                # plays still get counted under this one.
                handle_status_data(m, msg.get("data") or {})
                if m.get("prizeStale"):
                    m["liveStatus"] = {"ok": False, "msg": "⚠️ Prize no longer on this machine — still counting plays. Last update " + now_iso()}
                else:
                    m["liveStatus"] = {"ok": True, "msg": "Live — last update " + now_iso()}
                await store.save(key, m)
                await broadcast_machine(m)
            # other message types (pings, etc.) are ignored
    except Exception as e:
        log(f"Upstream for {key} closed: {e}")
    finally:
        m = await store.load(key)
        if m is not None:
            m["liveStatus"] = {"ok": False, "msg": "Upstream connection to TokyoCatch closed — reconnecting…"}
            await store.save(key, m)
            await broadcast_machine(m)
        # Unexpected close (not an explicit stopTracking) — try to
        # reconnect rather than dropping tracking silently.
        if key in runtime:
            runtime.pop(key, None)
            if m is not None:
                asyncio.create_task(start_tracking(m["machineId"], m["prizeId"]))


async def start_tracking(machine_id, prize_id):
    key = machine_key(machine_id, prize_id)
    if key in runtime:
        return  # already tracking

    existing = await store.load(key)
    if existing is None:
        existing = new_machine_state(machine_id, prize_id)
        await store.save(key, existing)
    else:
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
            )
        except TypeError:
            # Older/legacy websockets versions use "extra_headers" instead
            # of "additional_headers" for the same thing.
            upstream = await websockets.connect(
                TOKYOCATCH_WS_URL,
                extra_headers={"Origin": "https://tokyocatch.com"},
            )
    except Exception as e:
        existing["liveStatus"] = {"ok": False, "msg": f"Could not connect to TokyoCatch: {e}"}
        await store.save(key, existing)
        await broadcast_machine(existing)
        return

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
    runtime[key]["keepalive_task"] = asyncio.create_task(keepalive(key))

    existing["liveStatus"] = {"ok": True, "msg": "Connected — waiting for machine data…"}
    await store.save(key, existing)
    await broadcast_machine(existing)


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

    await store.save(key, m)
    await broadcast_machine(m)


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
    await websocket.send(json.dumps({"type": "machineList", "data": machines}))


async def handle_browser(websocket):
    browsers.add(websocket)
    log(f"Browser connected ({len(browsers)} watching)")
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

            elif msg_type == "startTracking":
                machine_id = data.get("machineId")
                prize_id = data.get("prizeId")
                if machine_id and prize_id:
                    asyncio.create_task(start_tracking(machine_id, prize_id))

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


async def process_request(path, request_headers):
    """Let plain HTTP requests (e.g. someone opening the URL in a browser tab
    directly, or a hosting platform's health check) get a friendly response
    instead of a WebSocket handshake error."""
    if request_headers.get("Upgrade", "").lower() != "websocket":
        return (200, [], b"TokyoCatch relay is running.\n")
    return None


async def main():
    await resume_all_tracked()
    async with websockets.serve(
        handle_browser,
        "0.0.0.0",
        PORT,
        process_request=process_request,
    ):
        log(f"Relay listening on 0.0.0.0:{PORT}")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
