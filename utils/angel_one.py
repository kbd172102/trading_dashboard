import json
from datetime import datetime
import requests, pyotp
from django.utils import timezone
from datetime import timedelta
from SmartApi.smartConnect import SmartConnect
from logzero import logger
from django.utils import timezone
from datetime import timedelta


def ensure_fresh_token(key):
    """Ensure JWT token is not older than 1 hour, else refresh via SmartAPI."""

    if not key or not key.jwt_token:
        return key

    now = timezone.now()

    # If updated less than 1 hour ago → don't refresh
    if key.updated_at and (now - key.updated_at) < timedelta(hours=1):
        return key

    # Refresh via SmartAPI (more reliable)
    success, resp = refresh_jwt(key)

    if success:
        print("TOKEN REFRESH SUCCESS USING SMARTAPI")
        return key

    print("SMARTAPI TOKEN REFRESH FAILED:", resp)
    return key

def angel_login(client_code, password, totp_secret, api_key):
    otp = pyotp.TOTP(totp_secret).now()

    url = "https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword"

    payload = {
        "clientcode": client_code,
        "password": password,
        "totp": otp,
        "state": "live"
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress": "00:00:00:00:00:00",
        "X-PrivateKey": api_key
    }

    response = requests.post(url, json=payload, headers=headers)
    return response.json()


def refresh(key):
    """
    Ensures token freshness without ever returning None.
    """
    if not key:
        return key

    # If token was updated less than 1 hour ago → return as is
    if key.updated_at and key.updated_at > timezone.now() - timedelta(hours=1):
        return key

    # Token is old → refresh using SmartAPI login method (most stable)
    try:
        username = key.client_code
        pwd = key.password
        totp = pyotp.TOTP(key.totp_secret).now()

        smart_api = SmartConnect(key.api_key)

        # Login session
        session = smart_api.generateSession(username, pwd, totp)

        if "data" not in session:
            logger.error(f"SMARTAPI LOGIN FAILED: {session}")
            return key  # DO NOT BREAK SYSTEM

        # Get fresh JWT + Refresh token
        token_data = smart_api.generateToken(session["data"]["refreshToken"])

        if "data" not in token_data:
            logger.error(f"SMARTAPI TOKEN FAILED: {token_data}")
            return key

        # Save to DB
        key.jwt_token = token_data["data"]["jwtToken"]
        key.refresh_token = token_data["data"]["refreshToken"]
        key.updated_at = timezone.now()
        key.save()

        return key

    except Exception as e:
        logger.error(f"SMARTAPI REFRESH ERROR: {e}")
        return key  # VERY IMPORTANT


def refresh_jwt(key):
    """
    Refresh AngelOne JWT token using SmartAPI's correct renewAccessToken() format.
    """

    try:
        smart = SmartConnect(api_key=key.api_key)

        # CORRECT CALL — must pass dict, not keyword arg
        data = smart.renewAccessToken({
            "refreshToken": key.refresh_token
        })

        if data and "data" in data:
            new_data = data["data"]

            key.jwt_token = new_data.get("jwtToken", key.jwt_token)
            key.refresh_token = new_data.get("refreshToken", key.refresh_token)
            key.feed_token = new_data.get("feedToken", key.feed_token)
            key.save()

            return True, key

        return False, data

    except Exception as e:
        return False, {"status": False, "message": str(e)}


def safe_json(response):
    try:
        return response.json()
    except Exception:
        return {"status": False, "message": "Invalid JSON response", "raw": response.text}

import requests

def get_angelone_candles(jwt_token, api_key, exchange, symbol_token, interval, fromdate, todate):
    import pandas as pd
    import requests, json

    url = "https://apiconnect.angelone.in/rest/secure/angelbroking/historical/v1/getCandleData"
    payload = {
        "exchange": exchange,
        "symboltoken": symbol_token,
        "interval": interval,
        "fromdate": fromdate,
        "todate": todate
    }

    headers = {
        "X-PrivateKey": api_key,
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress": "AA:BB:CC:DD:EE:FF",
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    try:
        # response = requests.post(url, headers=headers, data=json.dumps(payload))
        response = requests.post(url, json=payload, headers=headers)
        data = response.json()
    except Exception as e:
        return None, f"Invalid JSON response: {e}"

    if not data.get("status"):
        return None, data.get("message", "API failed.")

    rows = data.get("data", [])

    if not rows:
        return None, "No data available."

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=["datetime","open","high","low","close","volume"])
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert("Asia/Kolkata")
    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df, None

import requests

def get_rms_balance(user):
    """
    Fetch RMS balance (net, available cash, M2M etc.)
    """
    if not user.api_key or not user.jwt_token:
        return None, "API credentials missing"

    api_key = user.api_key.api_key
    jwt_token = user.jwt_token

    url = "https://apiconnect.angelone.in/rest/secure/angelbroking/user/v1/getRMS"

    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress": "00:00:00:00:00",
        "X-PrivateKey": api_key,
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)

        # ❗ Always inspect raw text first
        try:
            data = response.json()
        except ValueError:
            return None, f"Non-JSON response: {response.text}"

        # ❗ Handle string response
        if isinstance(data, str):
            return None, data

        # ❗ Normal success path
        if data.get("status") is True:
            return data.get("data"), None

        return None, data.get("message", "Unknown RMS API error")

    except requests.RequestException as e:
        return None, str(e)


def get_daily_pnl(user):
    """Get daily P&L (Live)"""
    # client wants graph → use AngelOne PNL API
    return [], None

def get_monthly_pnl(user):
    return [], None

def get_yearly_pnl(user):
    return [], None

from SmartApi import SmartConnect
import pandas as pd
import logzero
import websocket

def get_position_book(api_key, client_code, jwt_token):
    try:
        obj = SmartConnect(api_key=api_key)
        obj.setAccessToken(jwt_token)

        pos = obj.position()  # AngelOne API
        if "data" in pos and len(pos["data"]) > 0:
            df = pd.DataFrame(pos["data"])
            return df
        return pd.DataFrame()
    except Exception as e:
        print("Position book fetch error:", e)
        return pd.DataFrame()


def get_real_time_pnl(api_key, client_code, jwt_token):
    df = get_position_book(api_key, client_code, jwt_token)
    if df.empty:
        return 0, []

    # AngelOne already gives exact P&L:
    # netpnl = pnl (AngelOne computes it automatically)
    df['pnl'] = pd.to_numeric(df['pnl'], errors='coerce').fillna(0)

    total_pnl = df['pnl'].sum()

    # Convert for template
    positions = df.to_dict(orient="records")

    return total_pnl, positions

def get_smartapi_client(api_key, client_id, client_secret, totp=None):
    """
    Returns an authenticated SmartAPI client.
    """
    smart = SmartConnect(api_key=api_key)

    data = smart.generateSession(client_id, client_secret, totp)
    jwt_token = data['data']['jwtToken']

    return smart, jwt_token

# def get_angelone_candles(jwt_token, api_key, exchange, symbol_token, interval, fromdate, todate):
#     url = "https://apiconnect.angelone.in/rest/secure/angelbroking/historical/v1/getCandleData"
#
#     payload = {
#         "exchange": exchange,
#         "symboltoken": symbol_token,
#         "interval": interval,
#         "fromdate": fromdate,
#         "todate": todate
#     }
#
#     headers = {
#         "X-PrivateKey": api_key,
#         "X-UserType": "USER",
#         "X-SourceID": "WEB",
#         "X-ClientLocalIP": "127.0.0.1",
#         "X-ClientPublicIP": "127.0.0.1",
#         "X-MACAddress": "AA:BB:CC:DD:EE:FF",
#         "Authorization": f"Bearer {jwt_token}",
#         "Content-Type": "application/json",
#         "Accept": "application/json",
#     }
#
#     response = requests.post(url, headers=headers, data=json.dumps(payload))
#     return safe_json(response)

# utils/angelone_account.py

import requests
from logzero import logger

BASE_URL = "https://apiconnect.angelone.in/rest/secure/angelbroking"


def _headers(api_key, jwt_token):
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {jwt_token}",
        "X-PrivateKey": api_key,
    }


import requests
import logging

logger = logging.getLogger(__name__)

def get_account_balance(api_key, jwt_token):
    """
    Returns a dict ONLY:
    {
        available_cash,
        used_margin,
        net_balance
    }
    """

    url = f"{BASE_URL}/user/v1/getRMS"
    # headers = _headers(api_key, jwt_token)
    # headers = {"X-PrivateKey":"GV3q6BeG", "Authorization":"Bearer eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6Iko5MzA5NiIsInJvbGVzIjowLCJ1c2VydHlwZSI6IlVTRVIiLCJ0b2tlbiI6ImV5SmhiR2NpT2lKU1V6STFOaUlzSW5SNWNDSTZJa3BYVkNKOS5leUoxYzJWeVgzUjVjR1VpT2lKamJHbGxiblFpTENKMGIydGxibDkwZVhCbElqb2lkSEpoWkdWZllXTmpaWE56WDNSdmEyVnVJaXdpWjIxZmFXUWlPakV3TWl3aWMyOTFjbU5sSWpvaU15SXNJbVJsZG1salpWOXBaQ0k2SWpFellURXpZamcyTFRobE5HVXRNMlJoTUMwNU5EZGlMVFF5TWpaak1HTTBNMkZtWXlJc0ltdHBaQ0k2SW5SeVlXUmxYMnRsZVY5Mk1pSXNJbTl0Ym1WdFlXNWhaMlZ5YVdRaU9qRXdNaXdpY0hKdlpIVmpkSE1pT25zaVpHVnRZWFFpT25zaWMzUmhkSFZ6SWpvaVlXTjBhWFpsSW4wc0ltMW1JanA3SW5OMFlYUjFjeUk2SW1GamRHbDJaU0o5TENKdVluVk1aVzVrYVc1bklqcDdJbk4wWVhSMWN5STZJbUZqZEdsMlpTSjlmU3dpYVhOeklqb2lkSEpoWkdWZmJHOW5hVzVmYzJWeWRtbGpaU0lzSW5OMVlpSTZJa281TXpBNU5pSXNJbVY0Y0NJNk1UYzJPVFl5T0RBM01pd2libUptSWpveE56WTVOVFF4TkRreUxDSnBZWFFpT2pFM05qazFOREUwT1RJc0ltcDBhU0k2SWpWaU5qWTVNR0V3TFRnNU1UWXRORFU1WVMxaE5qaGlMV1l5TTJNNU1qVmtNREUzTWlJc0lsUnZhMlZ1SWpvaUluMC5QMjlRdDhIZGV3NUJYc1hsLUdlSWdnbVRKXzNZV0Zma2N5TXV4RVFLSktTd3RjdmtDV0pDVUp0WmpNMFJDbHhjbmFxemU1UC1ta1F1Q0Z4Si1oTGVqNDhyMFk5Sjc0QTFIc0hZMGxqel9vOWNJT2RNT3RKNjllY0Y5NDRsZUd6MTY1dnVfM2I1Mkh4V3lEcXNNNE5MWGVqYXEtS0JrRWFwQWo1V1ZxaHVyQ3ciLCJBUEktS0VZIjoiR1YzcTZCZUciLCJYLU9MRC1BUEktS0VZIjp0cnVlLCJpYXQiOjE3Njk1NDE2NzIsImV4cCI6MTc2OTYyNTAwMH0.5d8eqIG1fcuG7MfoKNGjZ-9t6XUmbWX2stVIRD99briYdrNL-_kBEq-j3_7UrkSna8XQkRVfixM3iU3eQUK46Q"}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6Iko5MzA5NiIsInJvbGVzIjowLCJ1c2VydHlwZSI6IlVTRVIiLCJ0b2tlbiI6ImV5SmhiR2NpT2lKU1V6STFOaUlzSW5SNWNDSTZJa3BYVkNKOS5leUoxYzJWeVgzUjVjR1VpT2lKamJHbGxiblFpTENKMGIydGxibDkwZVhCbElqb2lkSEpoWkdWZllXTmpaWE56WDNSdmEyVnVJaXdpWjIxZmFXUWlPakV3TWl3aWMyOTFjbU5sSWpvaU15SXNJbVJsZG1salpWOXBaQ0k2SWpFellURXpZamcyTFRobE5HVXRNMlJoTUMwNU5EZGlMVFF5TWpaak1HTTBNMkZtWXlJc0ltdHBaQ0k2SW5SeVlXUmxYMnRsZVY5Mk1pSXNJbTl0Ym1WdFlXNWhaMlZ5YVdRaU9qRXdNaXdpY0hKdlpIVmpkSE1pT25zaVpHVnRZWFFpT25zaWMzUmhkSFZ6SWpvaVlXTjBhWFpsSW4wc0ltMW1JanA3SW5OMFlYUjFjeUk2SW1GamRHbDJaU0o5TENKdVluVk1aVzVrYVc1bklqcDdJbk4wWVhSMWN5STZJbUZqZEdsMlpTSjlmU3dpYVhOeklqb2lkSEpoWkdWZmJHOW5hVzVmYzJWeWRtbGpaU0lzSW5OMVlpSTZJa281TXpBNU5pSXNJbVY0Y0NJNk1UYzJPVFl5T0RBM01pd2libUptSWpveE56WTVOVFF4TkRreUxDSnBZWFFpT2pFM05qazFOREUwT1RJc0ltcDBhU0k2SWpWaU5qWTVNR0V3TFRnNU1UWXRORFU1WVMxaE5qaGlMV1l5TTJNNU1qVmtNREUzTWlJc0lsUnZhMlZ1SWpvaUluMC5QMjlRdDhIZGV3NUJYc1hsLUdlSWdnbVRKXzNZV0Zma2N5TXV4RVFLSktTd3RjdmtDV0pDVUp0WmpNMFJDbHhjbmFxemU1UC1ta1F1Q0Z4Si1oTGVqNDhyMFk5Sjc0QTFIc0hZMGxqel9vOWNJT2RNT3RKNjllY0Y5NDRsZUd6MTY1dnVfM2I1Mkh4V3lEcXNNNE5MWGVqYXEtS0JrRWFwQWo1V1ZxaHVyQ3ciLCJBUEktS0VZIjoiR1YzcTZCZUciLCJYLU9MRC1BUEktS0VZIjp0cnVlLCJpYXQiOjE3Njk1NDE2NzIsImV4cCI6MTc2OTYyNTAwMH0.5d8eqIG1fcuG7MfoKNGjZ-9t6XUmbWX2stVIRD99briYdrNL-_kBEq-j3_7UrkSna8XQkRVfixM3iU3eQUK46Q",  # JWT token
        "X-PrivateKey": "GV3q6BeG",  # Your API key
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientPublicIP": "127.0.0.1",
        "X-ClientLocalIP": "127.0.0.1",
        "X-MACAddress": "AA-BB-CC-11-22-33",  # Any MAC string
        # Optional: if you have cookies from login session
        # "Cookie": "TS0179ac75=your_cookie_value",
    }

    try:
        res = requests.get(url, headers=headers, timeout=5)
        # logger.info("RMS status=%s body=%r", res.status_code, res.text)

        # Always inspect raw response when debugging
        try:
            payload = res.json()

        except ValueError:
            raise Exception(f"Non-JSON response: {res.text}")

        if not isinstance(payload, dict):
            raise Exception(f"Unexpected response: {payload}")

        if payload.get("status") is not True:
            raise Exception(payload.get("message", "RMS API failed"))

        data = payload.get("data", {})

        return {
            "available_cash": float(data.get("availablecash") or 0),
            "used_margin": float(data.get("utiliseddebits") or 0),
            "net_balance": float(data.get("net") or 0),
        }

    except Exception as e:
        logger.error("Balance fetch failed: %s", e)
        return {
            "available_cash": 0.0,
            "used_margin": 0.0,
            "net_balance": 0.0,
        }

def get_open_positions(api_key, jwt_token):
    """
    Returns list of broker open positions
    """
    try:
        url = f"{BASE_URL}/portfolio/v1/getPositions"
        res = requests.get(url, headers=_headers(api_key, jwt_token), timeout=5)
        return res.json().get("data", [])

    except Exception as e:
        logger.error("Position fetch failed: %s", e)
        return []


def get_total_pnl(api_key, jwt_token):
    pnl = 0.0
    for pos in get_open_positions(api_key, jwt_token):
        pnl += float(pos.get("pnl", 0))
    return pnl

# utils/angelone_auth.py

import pyotp
from logzero import logger
from SmartApi import SmartConnect


def login_and_get_tokens(angel_key):
    """
    Returns:
    {
        api_key,
        jwt_token,
        feed_token
    }
    """
    try:
        obj = SmartConnect(api_key=angel_key.api_key)
        totp = pyotp.TOTP(angel_key.totp_secret).now()

        session = obj.generateSession(
            angel_key.client_code,
            angel_key.password,
            totp
        )

        jwt = session["data"]["jwtToken"]
        feed_token = obj.getfeedToken()

        logger.info("AngelOne re-login successful")

        return {
            "api_key": angel_key.api_key,
            "jwt_token": jwt,
            "feed_token": feed_token
        }

    except Exception as e:
        logger.error("AngelOne login failed: %s", e)
        return None

def get_margin_required(api_key, jwt_token, exchange, tradingsymbol, symboltoken, transaction_type, quantity=1, product_type="INTRADAY", order_type="MARKET"):
    url = "https://apiconnect.angelone.in/rest/secure/angelbroking/margin/v1/batch"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        # "Authorization": f"Bearer eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6Iko5MzA5NiIsInJvbGVzIjowLCJ1c2VydHlwZSI6IlVTRVIiLCJ0b2tlbiI6ImV5SmhiR2NpT2lKU1V6STFOaUlzSW5SNWNDSTZJa3BYVkNKOS5leUoxYzJWeVgzUjVjR1VpT2lKamJHbGxiblFpTENKMGIydGxibDkwZVhCbElqb2lkSEpoWkdWZllXTmpaWE56WDNSdmEyVnVJaXdpWjIxZmFXUWlPakV3TWl3aWMyOTFjbU5sSWpvaU15SXNJbVJsZG1salpWOXBaQ0k2SWpFellURXpZamcyTFRobE5HVXRNMlJoTUMwNU5EZGlMVFF5TWpaak1HTTBNMkZtWXlJc0ltdHBaQ0k2SW5SeVlXUmxYMnRsZVY5Mk1pSXNJbTl0Ym1WdFlXNWhaMlZ5YVdRaU9qRXdNaXdpY0hKdlpIVmpkSE1pT25zaVpHVnRZWFFpT25zaWMzUmhkSFZ6SWpvaVlXTjBhWFpsSW4wc0ltMW1JanA3SW5OMFlYUjFjeUk2SW1GamRHbDJaU0o5TENKdVluVk1aVzVrYVc1bklqcDdJbk4wWVhSMWN5STZJbUZqZEdsMlpTSjlmU3dpYVhOeklqb2lkSEpoWkdWZmJHOW5hVzVmYzJWeWRtbGpaU0lzSW5OMVlpSTZJa281TXpBNU5pSXNJbVY0Y0NJNk1UYzJPVFl5TlRRM05Td2libUptSWpveE56WTVOVE00T0RrMUxDSnBZWFFpT2pFM05qazFNemc0T1RVc0ltcDBhU0k2SWpreU5tWTVObVZsTFRjNU1HRXROREkwWXkxaFpEQXlMVEExWmpnek9UTm1NelUyTWlJc0lsUnZhMlZ1SWpvaUluMC5DYzlMY3B2dFdYQUZvS1pJa3BwR2FsVUROS2xDNl9FOGdhUEVnamVHUkItVTBCeGotNDZ5Vl9zSEtRYmpVbG1HR1NhTmtXM0FaY0FGanpndjNSTjh4dW5ZRDhRN25kNGU3dnAwUG4zNEF2X0ZjVkN2cnFxTzl2bGhsVE5udWhPQXd4ZFU1NnQ3TjFLeXFTU0FVN2hFSDd2cVZRTnVtRXdoV2JMNndvd042a1EiLCJBUEktS0VZIjoiR1YzcTZCZUciLCJYLU9MRC1BUEktS0VZIjp0cnVlLCJpYXQiOjE3Njk1MzkwNzUsImV4cCI6MTc2OTYyNTAwMH0.dnQV13BpOYyR8IOQZi9yh5OGE2QAKmQ7bO-Lk1yRs1HLVpVgJ-1e8q5f2tro-vhVokV3aFOX0ETPZdUe3zx6dA",
        "Authorization": f"{jwt_token}",
        # "X-PrivateKey": "GV3q6BeG",
        "X-UserType": "USER",
        "X-PrivateKey": api_key,
        "X-SourceID": "WEB",
        "X-ClientPublicIP": "127.0.0.1",
        "X-ClientLocalIP": "127.0.0.1",
        "X-MACAddress": "AA-BB-CC-11-22-33",
    }

    payload = {
                  "positions": [
                    {
                      "exchange": "MCX",
                      "qty": 1,
                      "price": 0,
                      "productType": "INTRADAY",
                      "orderType": "LIMIT",
                      "token": "451669",
                      "tradeType": transaction_type
                    }
                  ]
                }



    try:
        response = requests.post(url, headers=headers, json=payload)
        data = response.json()

        if data.get("status") and data.get("data"):
            margin_data = data["data"]
            if isinstance(margin_data, list) and len(margin_data) > 0:
                return margin_data[0].get("totalMarginRequired", 0)
            elif isinstance(margin_data, dict): # Sometimes it returns a dict
                return margin_data.get("totalMarginRequired", 0)

        logger.error("Margin API failed: %s", data)
        return 0

    except Exception as e:
        logger.exception("Margin API request failed: %s", e)
        return 0
    """
    Fetch required margin for a single lot from Angel One's margin API.
    """
    url = "https://apiconnect.angelone.in/rest/secure/angelbroking/margin/v1/batch"

