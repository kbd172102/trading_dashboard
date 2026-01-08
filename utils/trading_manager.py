import time
from accounts.models import User
from utils.engine_manager import start_live_engine
from logzero import logger

# --- HARDCODED INSTRUMENT TOKEN ---
# Using a fixed instrument token as requested.
# 26009 is the example token for the NIFTY BANK index.
# Change this value if you intend to trade a different instrument.
HARDCODED_INSTRUMENT_TOKEN = "458305"


class LiveTradingManager:
    """
    Manages live trading engines for all users with trading enabled.
    """
    def start(self):
        """
        Starts the live trading engines for users.
        """
        logger.info("Starting Live Trading Manager...")
        
        # Get all users with trading enabled
        users_with_trading_enabled = User.objects.filter(trading_enabled=True)
        
        if not users_with_trading_enabled:
            logger.warning("No users with trading_enabled=True found.")
            return

        for user in users_with_trading_enabled:
            try:
                logger.info(f"Starting engine for user {user.id} with hardcoded instrument token {HARDCODED_INSTRUMENT_TOKEN}")
                start_live_engine(user.id, HARDCODED_INSTRUMENT_TOKEN)
            except Exception as e:
                logger.error(f"Error starting engine for user {user.id}: {e}")

    def stop(self):
        """
        Stops the live trading manager (not used in one-time script).
        """
        pass