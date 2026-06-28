"""
WebSocket中转联机服务器
- 部署到Render.com
- 房间号匹配
- 消息中转（1v1和2v2）
"""

import asyncio
import json
import os
import random
import string
import time
from aiohttp import web

# 房间存储: {room_code: Room}
rooms = {}

# 玩家连接: {ws: PlayerInfo}
players = {}


def generate_room_code(length=6):
    """生成6位房间号"""
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


class PlayerInfo:
    def __init__(self, ws, player_id, room_code, team, faction, slot):
        self.ws = ws
        self.player_id = player_id
        self.room_code = room_code
        self.team = team  # "left" or "right"
        self.faction = faction  # "holy_order" or "eastern"
        self.slot = slot  # 1v2中0或1（2v2中0/1/2/3）


class Room:
    def __init__(self, code, max_players=2, game_mode="1v1"):
        self.code = code
        self.max_players = max_players
        self.game_mode = game_mode  # "1v1" or "2v2"
        self.players = {}  # {slot: PlayerInfo}
        self.created_at = time.time()
        self.game_started = False
        self.seed = random.randint(0, 999999)

    def is_full(self):
        return len(self.players) >= self.max_players

    def get_all_ws(self):
        return [p.ws for p in self.players.values()]

    def is_expired(self):
        """30分钟未开始则过期"""
        return not self.game_started and time.time() - self.created_at > 1800


async def send_json(ws, data):
    """发送JSON消息"""
    try:
        await ws.send_json(data)
    except Exception:
        pass


async def broadcast_to_room(room, data, exclude_ws=None):
    """向房间内所有玩家广播"""
    for p in room.players.values():
        if p.ws != exclude_ws:
            await send_json(p.ws, data)


async def handle_websocket(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    player = None

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                msg_type = data.get("type")

                if msg_type == "create_room":
                    # 创建房间
                    game_mode = data.get("game_mode", "1v1")
                    max_players = 4 if game_mode == "2v2" else 2
                    faction = data.get("faction", "holy_order")

                    code = generate_room_code()
                    while code in rooms:
                        code = generate_room_code()

                    room = Room(code, max_players, game_mode)
                    rooms[code] = room

                    # 创建者默认为蓝方slot 0
                    slot = 0
                    player = PlayerInfo(ws, id(ws), code, "left", faction, slot)
                    room.players[slot] = player
                    players[ws] = player

                    await send_json(
                        ws,
                        {
                            "type": "room_created",
                            "room_code": code,
                            "slot": slot,
                            "team": "left",
                            "faction": faction,
                        },
                    )

                elif msg_type == "join_room":
                    # 加入房间
                    code = data.get("room_code", "").upper()
                    faction = data.get("faction", "eastern")

                    if code not in rooms:
                        await send_json(
                            ws,
                            {
                                "type": "join_failed",
                                "reason": "房间不存在",
                            },
                        )
                        continue

                    room = rooms[code]

                    if room.is_full():
                        await send_json(
                            ws,
                            {
                                "type": "join_failed",
                                "reason": "房间已满",
                            },
                        )
                        continue

                    if room.game_started:
                        await send_json(
                            ws,
                            {
                                "type": "join_failed",
                                "reason": "游戏已开始",
                            },
                        )
                        continue

                    # 分配slot和team
                    slot = len(room.players)
                    if room.game_mode == "1v1":
                        team = "right" if slot == 1 else "left"
                    else:
                        # 2v2: slot 0,1=蓝方, slot 2,3=红方
                        team = "left" if slot < 2 else "right"

                    player = PlayerInfo(ws, id(ws), code, team, faction, slot)
                    room.players[slot] = player
                    players[ws] = player

                    # 通知加入者
                    host_faction = (
                        room.players[0].faction if 0 in room.players else "holy"
                    )
                    await send_json(
                        ws,
                        {
                            "type": "join_success",
                            "room_code": code,
                            "slot": slot,
                            "team": team,
                            "faction": faction,
                            "game_mode": room.game_mode,
                            "host_faction": host_faction,
                        },
                    )

                    # 通知房间内其他人
                    await broadcast_to_room(
                        room,
                        {
                            "type": "player_joined",
                            "slot": slot,
                            "team": team,
                            "faction": faction,
                            "player_count": len(room.players),
                            "max_players": room.max_players,
                        },
                        exclude_ws=ws,
                    )

                elif msg_type == "start_game":
                    # 开始游戏（房主发起）
                    if player is None:
                        continue
                    room_code = player.room_code
                    if room_code not in rooms:
                        continue
                    room = rooms[room_code]
                    room.game_started = True

                    # 广播游戏开始
                    await broadcast_to_room(
                        room,
                        {
                            "type": "game_start",
                            "seed": room.seed,
                            "game_mode": room.game_mode,
                            "players": {
                                str(s): {
                                    "team": p.team,
                                    "faction": p.faction,
                                }
                                for s, p in room.players.items()
                            },
                        },
                    )

                elif msg_type == "game_input":
                    # 游戏中的输入中转
                    if player is None:
                        continue
                    room_code = player.room_code
                    if room_code not in rooms:
                        continue
                    room = rooms[room_code]

                    # 添加发送者信息后转发
                    relay_data = dict(data)
                    relay_data["from_slot"] = player.slot
                    relay_data["from_team"] = player.team

                    # 广播给房间内其他人
                    await broadcast_to_room(room, relay_data, exclude_ws=ws)

                elif msg_type == "ping":
                    await send_json(ws, {"type": "pong"})

            elif msg.type == web.WSMsgType.ERROR:
                break

    finally:
        # 清理
        if ws in players:
            p = players[ws]
            room_code = p.room_code
            if room_code in rooms:
                room = rooms[room_code]
                # 通知其他玩家
                await broadcast_to_room(
                    room,
                    {
                        "type": "player_left",
                        "slot": p.slot,
                        "team": p.team,
                    },
                )
                # 从房间移除
                if p.slot in room.players:
                    del room.players[p.slot]
                # 房间空了就删除
                if not room.players:
                    del rooms[room_code]

            del players[ws]

    return ws


async def cleanup_expired_rooms():
    """定期清理过期房间"""
    while True:
        await asyncio.sleep(60)
        expired = [code for code, room in rooms.items() if room.is_expired()]
        for code in expired:
            del rooms[code]


async def on_startup(app):
    asyncio.create_task(cleanup_expired_rooms())


def create_app():
    app = web.Application()
    app.router.add_get("/ws", handle_websocket)
    app.on_startup.append(on_startup)
    return app


if __name__ == "__main__":
    app = create_app()
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, host="0.0.0.0", port=port)
