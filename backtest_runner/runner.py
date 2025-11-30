# import os
# import uuid
# import pandas as pd
# from django.conf import settings
#
# # Import your backtest logic from the strategy file
# from backtest_runner.Bro_gaurd_SILVERMINI import (
#     backtest,
#     build_pnl_from_events,
#     save_outputs,
#     save_balance_chart,
#     STARTING_CASH
# )
#
# def run_backtest(df: pd.DataFrame, strategy_obj=None):
#     """
#     Universal backtest runner for:
#     - Uploaded static CSV data
#     - Live AngelOne OHLC candles
#
#     Returns:
#         output_chart  → PNG path
#         output_files  → list of dicts [{name, url}]
#     """
#
#     # Create isolated output directory
#     run_id = uuid.uuid4().hex
#     output_dir = os.path.join(settings.MEDIA_ROOT, "runs", run_id)
#     os.makedirs(output_dir, exist_ok=True)
#
#     # Switch working directory for backtest outputs
#     current_dir = os.getcwd()
#     os.chdir(output_dir)
#
#     # RUN BACKTEST
#     events_df, trades_df, stats = backtest(df, STARTING_CASH)
#     pnl_df = build_pnl_from_events(df, events_df)
#
#     # SAVE ALL CSVs & PNG
#     from backtest_runner.Bro_gaurd_SILVERMINI import (
#         TRADES_CSV, EVENTS_CSV, PNL_CSV, BALANCE_PNG
#     )
#
#     save_outputs(events_df, trades_df, pnl_df)
#     save_balance_chart(events_df)
#
#     # Restore path
#     os.chdir(current_dir)
#
#     # Prepare return objects
#     output_chart = os.path.join(settings.MEDIA_URL, "runs", run_id, BALANCE_PNG)
#
#     output_files = [
#         {"name": "Trades CSV", "url": os.path.join(settings.MEDIA_URL, "runs", run_id, TRADES_CSV)},
#         {"name": "Events CSV", "url": os.path.join(settings.MEDIA_URL, "runs", run_id, EVENTS_CSV)},
#         {"name": "PnL CSV",    "url": os.path.join(settings.MEDIA_URL, "runs", run_id, PNL_CSV)},
#     ]
#
#     return output_chart, output_files
