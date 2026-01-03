# utils/lot_manager.py

from logzero import logger
from live_trading.models import TradeStats


class LotManager:
    LOT_QTY = 5        # 1 lot = 5 quantity
    BASE_LOTS = 2

    def __init__(self, user, margin_per_lot):
        self.user = user
        self.margin_per_lot = margin_per_lot

        stats, _ = TradeStats.objects.get_or_create(user=user)
        self.stats = stats

    # ----------------------------------------------------
    # 🔹 UPDATE AFTER TRADE EXIT
    # ----------------------------------------------------
    def update_after_trade(self, pnl):
        """
        pnl > 0 → WIN
        pnl <= 0 → LOSS
        """
        if pnl > 0:
            self.stats.wins += 1
            self.stats.losses = 0
            self.stats.position_size = max(
                1, self.stats.position_size // 2
            )
            logger.info("WIN → halving position size")

        else:
            self.stats.losses += 1
            self.stats.wins = 0
            self.stats.position_size = self.stats.position_size * 2
            logger.info("LOSS → doubling position size")

        self.stats.save()

    # ----------------------------------------------------
    # 🔹 BOOST LOGIC
    # ----------------------------------------------------
    def get_boost(self):
        boost = 0

        if self.stats.losses >= 5:
            boost = 2
        elif self.stats.losses >= 3:
            boost = 1
        elif self.stats.wins >= 3:
            boost = 1

        return boost

    # ----------------------------------------------------
    # 🔹 CALCULATE FINAL LOTS
    # ----------------------------------------------------
    def calculate_lots(self, cash_balance):
        """
        Final Lots Logic:
        lots = min(position_size + boost, max_lots_by_cash)
        """
        boost = self.get_boost()
        desired_lots = self.stats.position_size + boost

        # 50% cash rule
        max_lots_by_cash = int((0.5 * cash_balance) / self.margin_per_lot)

        final_lots = min(desired_lots, max_lots_by_cash)

        final_lots = max(1, final_lots)

        logger.info(
            f"Lot Calc → desired={desired_lots}, cash_limit={max_lots_by_cash}, final={final_lots}"
        )

        return final_lots

    # ----------------------------------------------------
    # 🔹 LOT → QUANTITY
    # ----------------------------------------------------
    def lots_to_quantity(self, lots):
        return lots * self.LOT_QTY
