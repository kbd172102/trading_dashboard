# live_trading/views.py (optional)
import threading

from django.http import JsonResponse
from backtest_runner.models import AngelOneKey
# from utils.live_data_runner import process_user_ticks

def start_single_live(request):
    user = request.user
    creds = AngelOneKey.objects.filter(user=user).first()
    if not creds:
        return JsonResponse({"error":"no credentials"}, status=400)
    # default token list - replace with user's token(s)
    token_list = [{"exchangeType":5, "tokens":["57920"]}]
    # threading.Thread(target=process_user_ticks, args=(creds, token_list), daemon=True).start()
    return JsonResponse({"status":"started"})
