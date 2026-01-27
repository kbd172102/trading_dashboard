# utils/angel_one_orders.py

import json
import requests
from logzero import logger

# Angel One Official REST Order Endpoint
ORDER_URL = "https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/placeOrder"


def place_order(api_key: str,
                jwt_token: str,
                client_code: str,
                exchange: str,
                tradingsymbol: str,
                symboltoken: str,
                quantity: int,
                transaction_type: str,
                order_type: str = "MARKET",
                product_type: str = "INTRADAY",
                variety: str = "NORMAL",
                duration: str = "DAY",
                scripconsent: str = "yes"):
    """
    Place an order using Angel One SmartAPI V2 REST endpoint.
    This works for MCX, NSE, BSE, FNO, Comex etc.

    Returns: dict (API response)
    """

    headers = {
        "Content-Type": "application/json",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress": "00:00:00:00:00:00",
        "X-PrivateKey": api_key,
        "X-UserType": "USER",  # REQUIRED
        "X-SourceID": "WEB",  # REQUIRED
        "Authorization": f"{jwt_token}"
    }

    payload = {
        "exchange": exchange.upper(),               # MCX / NSE / BSE
        "tradingsymbol": tradingsymbol,             # e.g. SILVERM27FEB26FUT
        "symboltoken": symboltoken,                 # e.g. 451669
        "quantity": int(quantity),
        "transactiontype": transaction_type.upper(),    # BUY / SELL
        "ordertype": order_type.upper(),                # MARKET / LIMIT
        "variety": variety,                              # NORMAL
        "producttype": product_type.upper(),             # INTRADAY / DELIVERY
        "scripconsent": scripconsent,
        "duration": duration
    }

    logger.info(f"PLACEMENT PAYLOAD: {payload}")

    try:
        response = requests.post(ORDER_URL, headers=headers, data=json.dumps(payload))

        try:
            data = response.json()
        except:
            return {"status": False, "message": response.text}

        logger.info("Order Response: %s", data)
        return data

    except Exception as e:
        logger.exception("REST order failed: %s", e)
        return {"status": False, "message": str(e)}


# ----------------------------------------------------------
# Convenience wrappers for BUY/SELL
# ----------------------------------------------------------

# def buy_order(api_key, jwt_token, client_code,
#               exchange, tradingsymbol, symboltoken,
#               quantity):
#     """
#     Place a BUY order.
#     """
#     return place_order(
#         api_key=api_key,
#         jwt_token=jwt_token,
#         client_code=client_code,
#         exchange=exchange,
#         tradingsymbol=tradingsymbol,
#         symboltoken=symboltoken,
#         quantity=quantity,
#         transaction_type="BUY"
#     )
#
#
# def sell_order(api_key, jwt_token, client_code,
#                exchange, tradingsymbol, symboltoken,
#                quantity):
#     """
#     Place a SELL order.
#     """
#     return place_order(
#         api_key=api_key,
#         jwt_token=jwt_token,
#         client_code=client_code,
#         exchange=exchange,
#         tradingsymbol=tradingsymbol,
#         symboltoken=symboltoken,
#         quantity=quantity,
#         transaction_type="SELL"
#     )

def buy_order(api_key, jwt, client_code, exchange, tradingsymbol, token, qty):
    return place_order(
        api_key=api_key,
        client_code=client_code,
        jwt_token=jwt,
        exchange=exchange,
        tradingsymbol=tradingsymbol,
        symboltoken=token,
        quantity=qty,
        transaction_type="BUY"
    )


def sell_order(api_key, jwt, client_code, exchange, tradingsymbol, token, qty):
    return place_order(
        api_key=api_key,
        client_code=client_code,
        jwt_token=jwt,
        exchange=exchange,
        tradingsymbol=tradingsymbol,
        symboltoken=token,
        quantity=qty,
        transaction_type="SELL"
    )
