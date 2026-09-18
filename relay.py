"""
Multi-client relay for the TokyoCatch claw-machine dashboard.

Why this exists:
TokyoCatch's live WebSocket (wss://api.tokyocatch.com/subscriptions/v2) only
sends data to connections whose Origin header is https://tokyocatch.com.
Browsers set that header themselves based on the page's real address, so a
dashboard hosted anywhere else can never talk to TokyoCatch directly. This
relay is a small server that DOES connect with the right Origin header, and
then forwards the live data down to any number of browsers watching the
dashboard, wherever it's hosted.

Design:
Multiple visitors watching the SAME machine/prize share ONE upstream
connection to TokyoCatch (a "room"), rather than each opening their own.
This is both more efficient and more polite to TokyoCatch's servers than
opening one upstream connection per visitor.
"""

import asyncio
import json
import os
import time
import websockets

TOKYOCATCH_WS_URL = "wss://api.tokyocatch.com/subscriptions/v2"
PORT = int(os.environ.get("PORT", "8788"))

# One room per (machineId, prizeId) pair that's currently being watched.
# room = {
#   "upstream": <websocket to TokyoCatch>,
#   "browsers": set of <websocket to a visitor's browser>,
#   "keepalive_task": <asyncio.Task>,
#   "closing_task": <asyncio.Task or None>,
# }
rooms = {}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


async def keepalive(room, key):
    """TokyoCatch expects a periodic app-level ping to keep the subscription
    alive. One per room is enough, regardless of how many browsers are
    watching it — no need to forward each browser's own keepalive timer."""
    try:
        while True:
            await asyncio.sleep(15)
            try:
                await room["upstream"].send(json.dumps({"type": "ping"}))
            except Exception:
                break
    except asyncio.CancelledError:
        pass


async def pump_upstream(room, key):
    """Read messages from TokyoCatch and fan them out to every browser
    currently watching this machine/prize."""
    try:
        async for raw in room["upstream"]:
            dead = []
            for browser in list(room["browsers"]):
                try:
                    await browser.send(raw)
                except Exception:
                    dead.append(browser)
            for d in dead:
                room["browsers"].discard(d)
    except Exception as e:
        log(f"Upstream for {key} closed: {e}")
    finally:
        for browser in list(room["browsers"]):
            try:
                await browser.close(code=1011, reason="Upstream connection to TokyoCatch closed")
            except Exception:
                pass
        room["keepalive_task"].cancel()
        rooms.pop(key, None)
        log(f"Room {key} cleaned up")


async def get_or_create_room(machine_id, prize_id):
    key = (machine_id, prize_id)
    existing = rooms.get(key)
    if existing:
        # Someone re-joined a room that was about to be torn down — cancel that.
        if existing.get("closing_task"):
            existing["closing_task"].cancel()
            existing["closing_task"] = None
        return existing

    log(f"Opening upstream connection for {key}")
    try:
        upstream = await websockets.connect(
            TOKYOCATCH_WS_URL,
            additional_headers={"Origin": "https://tokyocatch.com"},
        )
    except TypeError:
        # Older/legacy websockets versions use "extra_headers" instead of
        # "additional_headers" for the same thing.
        upstream = await websockets.connect(
            TOKYOCATCH_WS_URL,
            extra_headers={"Origin": "https://tokyocatch.com"},
        )
    await upstream.send(json.dumps({
        "type": "machineSubscription",
        "data": {"id": machine_id, "prizeId": prize_id, "type": "view"},
    }))
    await upstream.send(json.dumps({
        "type": "initClient",
        "data": {"device": "web", "clientVersion": "44e8d84", "language": "en"},
    }))

    room = {"upstream": upstream, "browsers": set(), "closing_task": None}
    rooms[key] = room
    room["pump_task"] = asyncio.create_task(pump_upstream(room, key))
    room["keepalive_task"] = asyncio.create_task(keepalive(room, key))
    return room


async def maybe_close_room_later(key):
    """If a room has no browsers left, wait a bit (in case of a quick
    reconnect) before actually closing the upstream connection."""
    await asyncio.sleep(30)
    room = rooms.get(key)
    if room and not room["browsers"]:
        log(f"No browsers left for {key} — closing upstream")
        try:
            await room["upstream"].close()
        except Exception:
            pass
        # pump_upstream's finally block will pop it from `rooms`


async def handle_browser(websocket):
    room = None
    key = None
    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            msg_type = msg.get("type")
            if msg_type == "machineSubscription":
                data = msg.get("data", {})
                machine_id = data.get("id")
                prize_id = data.get("prizeId")
                if not machine_id or not prize_id:
                    continue
                key = (machine_id, prize_id)
                room = await get_or_create_room(machine_id, prize_id)
                room["browsers"].add(websocket)
                log(f"Browser joined {key} ({len(room['browsers'])} watching)")
            # 'initClient' and 'ping' from the browser are no-ops here — the
            # room already sent initClient once, and keepalive is handled
            # per-room rather than per-browser.
    except Exception:
        pass
    finally:
        if room is not None and key is not None:
            room["browsers"].discard(websocket)
            log(f"Browser left {key} ({len(room['browsers'])} watching)")
            if not room["browsers"] and room.get("closing_task") is None:
                room["closing_task"] = asyncio.create_task(maybe_close_room_later(key))


async def process_request(path, request_headers):
    """Let plain HTTP requests (e.g. someone opening the URL in a browser tab
    directly, or a hosting platform's health check) get a friendly response
    instead of a WebSocket handshake error."""
    if request_headers.get("Upgrade", "").lower() != "websocket":
        return (200, [], b"TokyoCatch relay is running.\n")
    return None


async def main():
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
