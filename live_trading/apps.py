# live_trading/apps.py
from django.apps import AppConfig

class LiveTradingConfig(AppConfig):
    name = 'live_trading'

    def ready(self):
        from accounts.models import User  # <-- Move import here!
        from utils.engine_manager import start_live_engine

        enabled_users = User.objects.filter(trading_enabled=True)
        for user in enabled_users:
            token = "451669"  # Replace with user's token if needed
            start_live_engine(user.id, token)