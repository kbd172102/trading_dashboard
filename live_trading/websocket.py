# live_trading/websocket.py
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

class LiveWebSocket:
    def __init__(self, jwt, api_key, client_id, on_tick):
        self.jwt = jwt
        self.api_key = api_key
        self.client_id = client_id
        self.on_tick = on_tick

    def start(self, token_list):
        ws = SmartWebSocketV2(self.jwt, self.api_key, self.client_id)

        ws.on_ticks = lambda ticks: self.on_tick(ticks)
        ws.on_open = lambda: ws.subscribe(token_list)
        ws.on_error = lambda e: print("WS Error:", e)
        ws.on_close = lambda: print("WS Closed")

        ws.connect()
