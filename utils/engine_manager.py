import threading

from logzero import logger

from utils.live_data_runner import candle_and_strategy_thread, db_writer_thread, UserEngine, websocket_thread

ENGINES = {}   # user_id → engine
user_engines = {}  # Global dictionary

def start_live_engine(user_id, token):
    if user_id in ENGINES and ENGINES[user_id].running.is_set():
        logger.info("Engine already running for user %s", user_id)
        return

    engine = UserEngine(user_id, token)
    ENGINES[user_id] = engine

    logger.info(f"Starting engine for user {user_id}")

    engine.thread_ws = threading.Thread(
        target=websocket_thread,
        args=(engine,),
        daemon=True,
        name="ws-thread"
    )
    engine.thread_ws.start()

    engine.thread_db = threading.Thread(
        target=db_writer_thread,
        args=(engine,),
        daemon=True,
        name="db-writer"
    )
    engine.thread_db.start()

    engine.thread_candle = threading.Thread(
        target=candle_and_strategy_thread,
        args=(engine,),
        daemon=True,
        name="candle-strategy"
    )
    engine.thread_candle.start()

    logger.info(f"Engine started for user {user_id}")


def stop_live_engine(user_id, token=None):
    global user_engines
    if user_id in user_engines:
        engine = user_engines[user_id]
        # Add your logic to stop the engine/thread here
        engine.stop()  # or whatever your stop method is
        del user_engines[user_id]
        print(f"Stopped engine for user {user_id}")
    else:
        print(f"No live engine found for user {user_id}")