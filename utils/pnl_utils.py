# utils/pnl_utils.py

from logzero import logger


def get_pnl_from_angelone(user):
    """
    Fetch latest closed trade PnL from AngelOne.
    This is a placeholder – plug AngelOne API here.
    """

    # TODO: replace with AngelOne TradeBook API
    logger.info("Fetching PnL for user %s", user.id)

    # TEMP mock (safe default)
    return 100.0
