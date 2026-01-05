# utils/position_manager.py

from datetime import datetime, timedelta
from logzero import logger
from django.utils import timezone

from utils.placeorder import buy_order, sell_order
from utils.pnl_utils import get_pnl_from_angelone

class PositionManager:
    def __init__(self, user, token):
        self.user = user
        self.token = token
        self.position = None
        self.cooldown_until = None
        self.base_lots = 2
        self.current_lots = 2
        self.win_streak = 0
        self.loss_streak = 0
        self.reward_boost = 0
        self.lot_size = 5

    def has_open_position(self):
        return self.position is not None

    def in_cooldown(self):
        return self.cooldown_until and timezone.now() < self.cooldown_until

    def _max_lots_by_cash(self, available_cash, margin_per_lot):
        if margin_per_lot == 0:
            return 1
        max_lots = int((0.5 * available_cash) / margin_per_lot)
        return max(1, max_lots)

    def calculate_lots(self, available_cash, margin_per_lot):
        lots = self.current_lots + self.reward_boost
        lots = min(lots, self._max_lots_by_cash(available_cash, margin_per_lot))
        return max(1, lots)

    def update_after_trade(self, pnl):
        if pnl > 0:
            self.win_streak += 1
            self.loss_streak = 0
            self.current_lots = max(1, self.current_lots // 2)
            if self.win_streak >= 3:
                self.reward_boost += 1
        else:
            self.loss_streak += 1
            self.win_streak = 0
            self.current_lots *= 2
            if self.loss_streak >= 3:
                self.reward_boost += 1
            if self.loss_streak >= 5:
                self.reward_boost += 2

    def open_position(self, side, price, lots, quantity):
        if self.position:
            return
        self.position = {
            "side": side,
            "entry_price": price,
            "lots": lots,
            "quantity": quantity,
            "fixed_sl": price * (0.985 if side == "LONG" else 1.015),
            "trailing_sl": price * (0.975 if side == "LONG" else 1.025),
            "entry_time": timezone.now()
        }
        logger.info(
            "[POSITION OPEN] %s | Price=%s Lots=%s Qty=%s",
            side, price, lots, quantity
        )

    def check_exit_on_tick(self, price):
        if not self.position:
            return

        side = self.position["side"]

        # Check fixed stop-loss
        if side == "LONG" and price <= self.position["fixed_sl"]:
            self._close_position("FIXED_SL", price)
            return
        if side == "SHORT" and price >= self.position["fixed_sl"]:
            self._close_position("FIXED_SL", price)
            return

        # Update and check trailing stop-loss
        if side == "LONG":
            self.position["trailing_sl"] = max(self.position["trailing_sl"], price * 0.975)
            if price <= self.position["trailing_sl"]:
                self._close_position("TRAIL_SL", price)
                return
        if side == "SHORT":
            self.position["trailing_sl"] = min(self.position["trailing_sl"], price * 1.025)
            if price >= self.position["trailing_sl"]:
                self._close_position("TRAIL_SL", price)
                return

    def manage_open_position(self, candle, ema_fast, ema_slow):
        # This method is now used for candle-based exit logic
        # which is handled in run_strategy_live based on C3-confirmed EMA reversals.
        # The core logic is moved to live_data_runner.
        pass

    def force_exit(self, reason, price):
        if self.position:
            self._close_position(reason, price)

    def _close_position(self, reason, price):
        if not self.position:
            return

        side = self.position["side"]
        quantity = self.position["quantity"]
        logger.info("[POSITION CLOSE] %s | %s @ %s", side, reason, price)

        if side == "LONG":
            sell_order(self.user, self.token, quantity)
        elif side == "SHORT":
            buy_order(self.user, self.token, quantity)

        pnl = get_pnl_from_angelone(self.user)
        self.update_after_trade(pnl)

        self.position = None
        self.cooldown_until = timezone.now() + timedelta(minutes=45)