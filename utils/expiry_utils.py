# utils/expiry_utils.py

from datetime import date, timedelta
import pytz


IST = pytz.timezone("Asia/Kolkata")

from datetime import date, datetime, timedelta

def is_last_friday_before_expiry(expiry_date, now=None):
    """
    expiry_date : date or 'YYYY-MM-DD' string
    returns True if today is the last Friday before expiry
    """

    # Convert expiry_date to date if needed
    if isinstance(expiry_date, str):
        expiry_date = datetime.strptime(expiry_date, "%Y-%m-%d").date()

    if now is None:
        now = date.today()
    elif isinstance(now, datetime):
        now = now.date()

    # Find last Friday strictly BEFORE expiry_date
    d = expiry_date - timedelta(days=1)

    while d.weekday() != 4:  # 4 = Friday
        d -= timedelta(days=1)

    return now == d

def is_one_week_before_expiry(expiry_date, now=None):
    """
    expiry_date : date or 'YYYY-MM-DD' string
    returns True if today is exactly 7 days before the expiry_date
    """

    # 1. Convert expiry_date to date object if it's a string
    if isinstance(expiry_date, str):
        expiry_date = datetime.strptime(expiry_date, "%Y-%m-%d").date()

    # 2. Normalize 'now' to a date object (ignoring time)
    if now is None:
        now = date.today()
    elif isinstance(now, datetime):
        now = now.date()

    # 3. Calculate the date exactly one week prior
    target_date = expiry_date - timedelta(weeks=1)

    return now == target_date

# def is_last_friday_before_expiry(expiry_date, now=None):
#     """
#     expiry_date : date (YYYY-MM-DD)
#     returns True if today is last Friday before expiry
#     """
#
#     if not now:
#         now = date.today()
#
#     # Find last Friday <= expiry_date
#     d = expiry_date
#     while d.weekday() != 4:   # 4 = Friday
#         d -= timedelta(days=1)
#
#     return now == d
