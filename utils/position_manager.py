# utils/position_manager.py

from datetime import datetime, timedelta
from logzero import logger
from django.utils import timezone

from utils.placeorder import buy_order, sell_order
from utils.pnl_utils import get_pnl_from_angelone

class PositionManager:
    def __init__(self, user_id, token):
        self.user_id = user_id
        self.token = token

        self.position = None
        self.cooldown = 0

        self.win_streak = 0
        self.loss_streak = 0
        self.base_lots = 2

    def has_open_position(self):
        return self.position is not None

    def in_cooldown(self):
        return self.cooldown > 0

    def calculate_lots(self):
        lots = self.base_lots

        if self.loss_streak >= 3:
            lots *= 2
        if self.win_streak >= 3:
            lots += 1

        return max(1, lots)

    def open_position(self, side, price, lots, quantity):
        self.position = {
            "side": side,
            "entry": price,
            "lots": lots,
            "qty": quantity,
            "sl": price * (0.985 if side == "LONG" else 1.015)
        }

    def manage_open_position(self, candle, ema_fast, ema_slow):
        if not self.position:
            return

        price = candle["close"]

        if (
            self.position["side"] == "LONG"
            and price < self.position["sl"]
        ):
            self.close_position(win=False)

        elif (
            self.position["side"] == "SHORT"
            and price > self.position["sl"]
        ):
            self.close_position(win=False)

    def close_position(self, win):
        self.position = None
        self.cooldown = 3

        if win:
            self.win_streak += 1
            self.loss_streak = 0
        else:
            self.loss_streak += 1
            self.win_streak = 0

    def mark_position_open(self, side, qty, price, order_id):
        self.position_open = True
        self.side = side
        self.quantity = qty
        self.entry_price = price
        self.order_id = order_id
        self.entry_time = timezone.now()


#
# class PositionManager:
#     def __init__(self, user, token):
#         self.user = user
#         self.token = token
#
#         # --------------------
#         # POSITION STATE
#         # --------------------
#         self.position = None  # dict when open, None when flat
#         self.cooldown_until = None
#
#         # --------------------
#         # LOT ENGINE STATE
#         # --------------------
#         self.base_lots = 2
#         self.current_lots = 2
#
#         self.win_streak = 0
#         self.loss_streak = 0
#         self.reward_boost = 0
#
#         self.margin_per_lot = 30000
#         self.lot_size = 5
#
#     # ====================
#     # STATE HELPERS
#     # ====================
#     def has_open_position(self):
#         return self.position is not None
#
#     def in_cooldown(self):
#         return self.cooldown_until and timezone.now() < self.cooldown_until
#
#     # ====================
#     # LOT ENGINE
#     # ====================
#     def _max_lots_by_cash(self, available_cash):
#         max_lots = int((0.5 * available_cash) / self.margin_per_lot)
#         return max(1, max_lots)
#
#     def calculate_lots(self, available_cash):
#         lots = self.current_lots + self.reward_boost
#         lots = min(lots, self._max_lots_by_cash(available_cash))
#         return max(1, lots)
#
#     def update_after_trade(self, pnl):
#         if pnl > 0:
#             self.win_streak += 1
#             self.loss_streak = 0
#             self.current_lots = max(1, self.current_lots // 2)
#
#             if self.win_streak >= 3:
#                 self.reward_boost += 1
#         else:
#             self.loss_streak += 1
#             self.win_streak = 0
#             self.current_lots *= 2
#
#             if self.loss_streak >= 3:
#                 self.reward_boost += 1
#             if self.loss_streak >= 5:
#                 self.reward_boost += 2
#
#     # ====================
#     # ENTRY
#     # ====================
#     def open_position(self, side, price, lots, quantity):
#         if self.position:
#             return
#
#         self.position = {
#             "side": side,
#             "entry_price": price,
#             "lots": lots,
#             "quantity": quantity,
#             "fixed_sl": price * (0.985 if side == "LONG" else 1.015),
#             "trailing_sl": price * (0.975 if side == "LONG" else 1.025),
#             "entry_time": timezone.now()
#         }
#
#         logger.info(
#             "[POSITION OPEN] %s | Price=%s Lots=%s Qty=%s",
#             side, price, lots, quantity
#         )
#
#     # ====================
#     # EXIT MANAGEMENT
#     # ====================
#     def manage_open_position(self, candle, ema_fast, ema_slow):
#         if not self.position:
#             return
#
#         price = candle["close"]
#         side = self.position["side"]
#
#         # ----- FIXED SL -----
#         if side == "LONG" and price <= self.position["fixed_sl"]:
#             self._close_position("FIXED_SL", price)
#             return
#
#         if side == "SHORT" and price >= self.position["fixed_sl"]:
#             self._close_position("FIXED_SL", price)
#             return
#
#         # ----- TRAILING SL -----
#         if side == "LONG":
#             self.position["trailing_sl"] = max(
#                 self.position["trailing_sl"], price * 0.975
#             )
#             if price <= self.position["trailing_sl"]:
#                 self._close_position("TRAIL_SL", price)
#                 return
#
#         if side == "SHORT":
#             self.position["trailing_sl"] = min(
#                 self.position["trailing_sl"], price * 1.025
#             )
#             if price >= self.position["trailing_sl"]:
#                 self._close_position("TRAIL_SL", price)
#                 return
#
#         # ----- EMA REVERSAL -----
#         if side == "LONG" and ema_fast < ema_slow:
#             self._close_position("EMA_REVERSAL", price)
#             return
#
#         if side == "SHORT" and ema_fast > ema_slow:
#             self._close_position("EMA_REVERSAL", price)
#             return
#
#     # ====================
#     # FORCE EXIT
#     # ====================
#     def force_exit(self, reason, price):
#         if self.position:
#             self._close_position(reason, price)
#
#     # ====================
#     # INTERNAL CLOSE
#     # ====================
#     def _close_position(self, reason, price):
#         side = self.position["side"]
#
#         logger.info("[POSITION CLOSE] %s | %s @ %s", side, reason, price)
#
#         pnl = get_pnl_from_angelone(self.user)
#         self.update_after_trade(pnl)
#
#         self.position = None
#         self.cooldown_until = timezone.now() + timedelta(minutes=45)
