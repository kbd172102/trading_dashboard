# dashboard/views.py
import base64
import os, sys, subprocess
from django.db.models import Sum
from django.shortcuts import render, redirect, get_object_or_404
from django.conf import settings
from django.core.files.storage import FileSystemStorage
from django.contrib.auth.decorators import login_required
import pandas as pd
from accounts.models import User  # Adjust if your user model is custom
from dashboard.forms import AngelOneKeyForm, LiveBacktestForm
from utils.angel_one import get_daily_pnl, get_monthly_pnl, get_yearly_pnl, angel_login, \
    get_rms_balance, get_angelone_candles, get_real_time_pnl, refresh
from utils.backtest import backtest, balance_chart_base64
from django.contrib import messages
import csv
from backtest_runner.models import Strategy, AngelOneKey, RunRequest
from utils.engine_manager import start_live_engine, stop_live_engine
from django.http import JsonResponse

from .models import BacktestResult
from django.utils import timezone
from datetime import timedelta


@login_required
def dashboard_home(request):
    user = request.user

    # ------------------------------
    # 1. Summary Cards (Totals)
    # ------------------------------
    profile = None
    total_balance = 0
    net_profit = 0
    available_cash = 0
    loss_profit = 0

    api_key_obj = getattr(user, "angel_api", None)

    # ==== If API key available → Use RMS ====
    if api_key_obj:
        rms = get_rms_balance(api_key_obj)
        if rms.get("status"):
            data = rms.get("data", {})
            total_balance = float(data.get("net", 0))
            available_cash = float(data.get("availablecash", 0))
            m2m_real = float(data.get("m2mrealized", 0))
            m2m_unreal = float(data.get("m2munrealized", 0))

            net_profit = m2m_real + m2m_unreal
            loss_profit = net_profit  # same meaning
    else:
        # ==== Fallback: last backtest result ====
        last = BacktestResult.objects.filter(user=user, status="success").order_by("-created_at").first()
        if last:
            net_profit = float(last.realized_pnl)
            total_balance = float(last.ending_cash)
            available_cash = total_balance

    # ------------------------------
    # 2. Graph Data (Daily, Monthly, Yearly)
    # ------------------------------
    graph_type = request.GET.get("range", "daily")

    graph_data = []
    labels = []

    today = timezone.now().date()

    # DAILY GRAPH — last 30 days
    if graph_type == "daily":
        for i in range(30):
            day = today - timedelta(days=i)
            labels.append(day.strftime("%d %b"))
            graph_data.append(float(available_cash))  # replace later with dynamic RMS history
        labels.reverse()
        graph_data.reverse()

    # MONTHLY GRAPH — last 12 months
    elif graph_type == "monthly":
        for i in range(12):
            month = (today.replace(day=1) - pd.DateOffset(months=i)).date()
            labels.append(month.strftime("%b %Y"))
            graph_data.append(float(available_cash))
        labels.reverse()
        graph_data.reverse()

    # YEARLY GRAPH — last 5 years
    elif graph_type == "yearly":
        for i in range(5):
            year = today.year - i
            labels.append(str(year))
            graph_data.append(float(available_cash))
        labels.reverse()
        graph_data.reverse()

    return render(request, "dashboard/dashboard_home.html", {
        "total_balance": total_balance,
        "available_cash": available_cash,
        "net_profit": net_profit,
        "loss_profit": loss_profit,
        "graph_labels": labels,
        "graph_data": graph_data,
        "graph_type": graph_type,
    })


@login_required
def pnl_graph(request):
    gtype = request.GET.get("type", "daily")

    if gtype == "daily":
        data, _ = get_daily_pnl(request.user)
    elif gtype == "monthly":
        data, _ = get_monthly_pnl(request.user)
    else:
        data, _ = get_yearly_pnl(request.user)

    return JsonResponse(data)


@login_required
def api_integration(request):
    try:
        existing_key = AngelOneKey.objects.get(user=request.user)
    except AngelOneKey.DoesNotExist:
        existing_key = None

    if request.method == "POST":
        form = AngelOneKeyForm(request.POST, instance=existing_key)

        if form.is_valid():
            obj = form.save(commit=False)
            obj.user = request.user

            # Generate tokens
            response = angel_login(
                client_code=obj.client_code,
                password=obj.password,
                totp_secret=obj.totp_secret,
                api_key=obj.api_key
            )

            if response.get("status") is True:
                data = response.get("data", {})

                obj.jwt_token = data.get("jwtToken")
                obj.refresh_token = data.get("refreshToken")
                obj.feed_token = data.get("feedToken")
                obj.save()

                start_live_engine(
                    user_id=request.user.id,
                    token="458305"  # this should be dynamic later
                )

                messages.success(request, "API Key connected successfully!")
                return redirect("dashboard:api_integration")
            else:
                messages.error(request, response.get("message", "Failed to connect"))
    else:
        form = AngelOneKeyForm(instance=existing_key)

    return render(request, "dashboard/api_integration.html", {
        "form": form,
        "existing": existing_key,
    })

@login_required
def live_backtest(request):
    user = request.user
    ang_key = AngelOneKey.objects.filter(user=user).first()
    if not ang_key:
        return render(request, "dashboard/live_backtest.html", {"error": "No AngelOne credentials found. Add them in API Integration."})


    form = LiveBacktestForm(request.POST or None)
    context = {"form": form}

    if request.method == "POST":
        if not form.is_valid():
            context["error"] = "Invalid form data."
            return render(request, "dashboard/live_backtest.html", context)

        strategy = form.cleaned_data["strategy"]    # Strategy model instance
        interval = 'FIFTEEN_MINUTE'
        from_dt = form.cleaned_data["from_date"]
        to_dt   = form.cleaned_data["to_date"]

        # AngelOne expects "YYYY-MM-DD HH:MM" (no seconds)
        def fmt(dt):
            if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
                dt = pd.to_datetime(dt).tz_convert(None)
            return dt.strftime("%Y-%m-%d %H:%M")

        from_date = fmt(from_dt)
        to_date   = fmt(to_dt)

        # symbol token should be numeric token stored in strategy.symbol (if you store plain symbol update DB)
        symbol_token = str(strategy.symbol).strip()
        # if ang_key.updated_at > timezone.now() - timedelta(hours=1):
        ang_key = refresh(ang_key)

        if not ang_key.jwt_token:
            context["error"] = "AngelOne session expired. Please reconnect API."
            return render(request, "dashboard/live_backtest.html", context)

        candles_df, err = get_angelone_candles(
            jwt_token=ang_key.jwt_token,
            api_key=ang_key.api_key,
            exchange=strategy.exchange,
            symbol_token=symbol_token,
            interval=interval,
            fromdate=from_date,
            todate= to_date
        )
        if err:
            context["error"] = f"API Error: {err}"
            return render(request, "dashboard/live_backtest.html", context)

        if candles_df is None or (isinstance(candles_df, (list,tuple)) and len(candles_df)==0):
            context["error"] = "No candle data returned for the given range."
            # still show empty chart
            context["chart_base64"] = None
            return render(request, "dashboard/live_backtest.html", context)

        # starting cash from RMS (preferred)
        starting_cash = getattr(settings, "DEFAULT_STARTING_CASH", 2_500_000)
        try:
            rms_data, rms_err = get_rms_balance(ang_key)
            if rms_data and isinstance(rms_data, dict):
                available = rms_data.get("availablecash") or rms_data.get("available_cash") or rms_data.get("net")
                if available is not None:
                    starting_cash = float(available)
        except Exception:
            pass

        # run backtest: engine accepts df, strategy object, starting_cash
        try:
            events_df, trades_df, stats = backtest(candles_df, strategy=strategy, starting_cash=starting_cash)

        except Exception as e:
            context["error"] = f"Backtest failed: {e}"
            return render(request, "dashboard/live_backtest.html", context)

        # build pnl table from events (if needed you can import an existing util)
        try:
            # using simple post-hoc: compute net pnl per exit included in events_df already
            pnl_df = pd.DataFrame([{
                "exit_time": r["time"],
                "net_pnl": r["realized_pnl"]

            } for _, r in events_df[events_df["event"]=="EXIT"].iterrows()]) if not events_df.empty else pd.DataFrame()
        except Exception:
            pnl_df = pd.DataFrame()

        # produce base64 chart (works even when no EXIT events, returns "No data" image)
        chart_b64 = balance_chart_base64(events_df)

        # persist CSVs into media/live_outputs/user_{id}/ (optional)
        out_dir = os.path.join(settings.MEDIA_ROOT, "live_outputs", f"user_{user.id}")
        os.makedirs(out_dir, exist_ok=True)
        events_path = os.path.join(out_dir, "events.csv")
        trades_path = os.path.join(out_dir, "trades.csv")
        pnl_path    = os.path.join(out_dir, "pnl.csv")
        try:
            events_df.to_csv(events_path, index=False)
            trades_df.to_csv(trades_path, index=False)
            pnl_df.to_csv(pnl_path, index=False)
            chart_url = os.path.join(settings.MEDIA_URL, "live_outputs", f"user_{user.id}", "balance.png")
            # also save chart as file for compatibility (decode base64)
            chart_file_path = os.path.join(out_dir, "balance.png")
            # decode and save
            header, b64 = chart_b64.split(",", 1)
            with open(chart_file_path, "wb") as fh:
                fh.write(base64.b64decode(b64))
        except Exception:
            chart_url = None

        events_df = format_numeric(events_df)
        trades_df = format_numeric(trades_df)
        pnl_df = format_numeric(pnl_df)

        context.update({
            "chart": chart_url,
            "chart_base64": chart_b64,
            "events": events_df.head(50).to_html(classes="table table-striped table-sm",index=False) if not events_df.empty else "",
            "trades": trades_df.head(50).to_html(classes="table table-striped table-sm", index=False) if not trades_df.empty else "",
            "pnl": pnl_df.head(50).to_html(classes="table table-striped table-sm", index=False) if not pnl_df.empty else "",
            "stats": stats,
            "starting_cash": starting_cash,
            "symbol_token": symbol_token,
            "strategy": strategy,
            "events_csv": settings.MEDIA_URL + f"live_outputs/user_{user.id}/events.csv",
            "trades_csv": settings.MEDIA_URL + f"live_outputs/user_{user.id}/trades.csv",
            "pnl_csv": settings.MEDIA_URL + f"live_outputs/user_{user.id}/pnl.csv",
        })
        return render(request, "dashboard/live_backtest.html", context)

    return render(request, "dashboard/live_backtest.html", context)

def format_numeric(df):
    if df is None or df.empty:
        return df
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].apply(lambda x: f"{x:,.2f}")
    return df


@login_required
def pnl_report(request):

    # Fetch saved AngelOne credentials
    key = AngelOneKey.objects.filter(user=request.user).first()

    if not key:
        return render(request, "dashboard/pnl_report.html", {
            "error": "No AngelOne API Key found. Please add your credentials in API Integration."
        })

    # Call AngelOne real-time P&L
    total_pnl, positions = get_real_time_pnl(
        api_key     = key.api_key,
        client_code = key.client_code,
        jwt_token   = key.jwt_token
    )

    return render(request, "dashboard/pnl_report.html", {
        "total_pnl": total_pnl,
        "positions": positions
    })

@login_required
def reports(request):
    runs = RunRequest.objects.filter(user=request.user).order_by('-created_at')
    return render(request, "dashboard/reports.html", {"runs": runs})


@login_required
def start_trading(request):
    if request.method == "POST":
        user = request.user
        # Get user's API key and trading token (customize as needed)
        creds = AngelOneKey.objects.filter(user=user).first()
        if not creds:
            return JsonResponse({"status": "error", "message": "Trading not enabled or API key missing."})

        # You may want to get the token dynamically, here it's hardcoded
        token = "458305"  # Replace with user's selected token if needed

        start_live_engine(user.id, token)
        user.trading_enabled = True
        user.save()
        return JsonResponse({"status": "success", "message": "Trading started."})
    return JsonResponse({"status": "error", "message": "Invalid request method."})

@login_required
def stop_trading(request):
    if request.method == "POST":
        user = request.user
        token = "458305"
        # Stop trading logic
        stop_live_engine(user.id, token)
        user.trading_enabled = False
        user.save()
        return JsonResponse({"status": "success", "message": "Trading stopped."})
    return JsonResponse({"status": "error", "message": "Invalid request method."})