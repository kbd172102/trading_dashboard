import time
from accounts.models import User
from backtest_runner.models import AngelOneKey
from utils.engine_manager import start_live_engine, stop_live_engine
from logzero import logger

class LiveTradingManager:
    """
    Manages live trading engines for all users with trading enabled.
    """
    def __init__(self):
        self.running = True

    def start(self):
        """
        Starts the live trading manager.
        """
        logger.info("Starting Live Trading Manager...")
        while self.running:
            self.check_and_manage_engines()
            time.sleep(60)  # Check every 60 seconds

    def stop(self):
        """
        Stops the live trading manager.
        """
        logger.info("Stopping Live Trading Manager...")
        self.running = False

    def check_and_manage_engines(self):
        """
        Checks for users with trading enabled and starts/stops engines accordingly.
        """
        users_with_trading_enabled = User.objects.filter(trading_enabled=True)
        logger.info(f"Found {len(users_with_trading_enabled)} users with trading enabled.")

        for user in users_with_trading_enabled:
            try:
                angel_key = AngelOneKey.objects.get(user=user)
                if angel_key.jwt_token:
                    logger.info(f"Starting engine for user {user.id}...")
                    start_live_engine(user.id, angel_key.jwt_token)
                else:
                    logger.warning(f"User {user.id} has trading enabled but no JWT token.")
            except AngelOneKey.DoesNotExist:
                logger.warning(f"User {user.id} has trading enabled but no AngelOneKey.")
            except Exception as e:
                logger.error(f"Error starting engine for user {user.id}: {e}")