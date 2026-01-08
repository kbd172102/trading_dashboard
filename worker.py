import os
import django
from django.conf import settings

def main():
    """
    Main function to initialize the Django application and start the LiveTradingEngine.
    """
    print("Initializing Django...")
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'portal.settings')
    django.setup()
    print("Django initialized.")

    # We need to ensure that the settings are configured before importing the engine
    # as it may depend on Django models or other settings.
    from live_trading.engine import LiveEngine

    print("Starting Live Trading Engine...")
    try:
        engine = LiveEngine()
        engine.start()
        print("Live Trading Engine started successfully.")
    except Exception as e:
        print(f"Failed to start Live Trading Engine: {e}")

if __name__ == "__main__":
    main()