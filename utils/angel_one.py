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


def get_rms_balance(user):
    """
    Fetch RMS balance (net, available cash, M2M etc.)
    """
    if not user.api_key:
        return None, "API credentials missing"

    access_token = user.api_key.access_token
    api_key = user.api_key.api_key

    url = "https://apiconnect.angelone.in/rest/secure/angelbroking/user/v1/getRMS"

    headers = {
        "Authorization": f"Bearer {access_token}",
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
        response = requests.get(url, headers=headers)
        data = response.json()

        if data.get("status") is True:
            return data["data"], None
        else:
            return None, data.get("message", "Unknown RMS API error")

    except Exception as e:
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


def get_account_balance(api_key, jwt_token):
    """
    Returns:
    {
        available_cash,
        used_margin,
        net_balance
    }
    """
    try:
        url = f"{BASE_URL}/user/v1/getRMS"
        res = requests.get(url, headers=_headers(api_key, jwt_token), timeout=5)
        data = res.json()["data"]

        return {
            "available_cash": float(data.get("availablecash", 0)),
            "used_margin": float(data.get("utilisedmargin", 0)),
            "net_balance": float(data.get("net", 0)),
        }

    except Exception as e:
        logger.error("Balance fetch failed: %s", e)
        return {
            "available_cash": 0,
            "used_margin": 0,
            "net_balance": 0,
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
    """
    Fetch required margin for a single lot from Angel One's margin API.
    """
    url = "https://apiconnect.angelone.in/rest/secure/angelbroking/margin/v1/batch"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {jwt_token}",
        "X-PrivateKey": api_key,
        "X-UserType": "USER",
        "X-SourceID": "WEB",
    }

    payload = [
        {
            "exchange": exchange.upper(),
            "margin_type": "REQUIRED",
            "tradingsymbol": tradingsymbol,
            "symboltoken": symboltoken,
            "transaction_type": transaction_type.upper(),
            "variety": "NORMAL",
            "product": product_type.upper(),
            "order_type": order_type.upper(),
            "quantity": quantity,
            "price": 0, # Market order
        }
    ]

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