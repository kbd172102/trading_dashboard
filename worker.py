import os
import django

def main():
    """
    Main function to initialize the Django application and start the LiveTradingManager.
    """
    print("Initializing Django...")
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'portal.settings')
    django.setup()
    print("Django initialized.")

    from utils.trading_manager import LiveTradingManager

    print("Starting Live Trading Manager...")
    try:
        manager = LiveTradingManager()
        manager.start()
    except Exception as e:
        print(f"Failed to start Live Trading Manager: {e}")

if __name__ == "__main__":
    main()