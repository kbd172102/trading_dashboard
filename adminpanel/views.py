# adminpanel/views.py
from django.contrib.admin.views.decorators import staff_member_required
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import user_passes_test
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.db import transaction

from adminpanel.forms import StrategyForm
from backtest_runner.models import Strategy, AngelOneKey

User = get_user_model()


# --------------------------
# ADMIN LOGIN CHECK
# --------------------------
def admin_required(view):
    return user_passes_test(lambda u: u.is_superuser, login_url="accounts:login")(view)


# --------------------------
# ADMIN DASHBOARD
# --------------------------
@admin_required
def admin_home(request):
    total_clients = User.objects.filter(is_superuser=False).count()
    total_strategies = Strategy.objects.count()

    return render(request, "adminpanel/dashboard.html", {
        "total_clients": total_clients,
        "total_strategies": total_strategies,
    })


# --------------------------
# MANAGE CLIENTS PAGE
# --------------------------
@admin_required
def manage_clients(request):
    users = User.objects.filter(is_superuser=False).order_by("-id")
    api_credentials = {a.user_id: a for a in AngelOneKey.objects.filter(user__in=users)}

    return render(request, "adminpanel/clients.html", {"users": users, "api_credentials": api_credentials})


# --------------------------
# ADD CLIENT
# --------------------------
@admin_required
@transaction.atomic
def add_client(request):
    if request.method == "POST":
        username = request.POST.get("username")
        email = request.POST.get("email")
        password = request.POST.get("password")
        first_name = request.POST.get("first_name")
        last_name = request.POST.get("last_name")
        is_active = "is_active" in request.POST

        # Check duplicates
        if User.objects.filter(username=username).exists():
            messages.error(request, "Username already exists.")
            return redirect("adminpanel:manage_clients")

        if User.objects.filter(email=email).exists():
            messages.error(request, "Email already exists.")
            return redirect("adminpanel:manage_clients")

        User.objects.create_user(
            username=username,
            email=email,
            password=password,
            first_name=first_name,
            last_name=last_name,
            is_active=is_active,
        )

        messages.success(request, "Client added successfully.")
        return redirect("adminpanel:manage_clients")

    return redirect("adminpanel:manage_clients")

#add api credential
@staff_member_required
def add_client_api(request):
    if request.method == "POST":
        user_id = request.POST.get("user_id")
        user = get_object_or_404(get_user_model(), id=user_id)

        AngelOneKey.objects.update_or_create(
            user=user,
            defaults={
                "client_code": request.POST.get("client_code"),
                "password": request.POST.get("password"),
                "totp_secret": request.POST.get("totp_secret"),
                "api_key": request.POST.get("api_key")
            }
        )

        messages.success(request, "API Credentials added successfully.")
        return redirect("adminpanel:manage_clients")

    return redirect("adminpanel:manage_clients")


# --------------------------
# EDIT CLIENT
# --------------------------
@admin_required
@transaction.atomic
def edit_client(request, pk):
    user = get_object_or_404(User, pk=pk, is_superuser=False)

    if request.method == "POST":
        user.username = request.POST.get("username")
        user.email = request.POST.get("email")
        user.first_name = request.POST.get("first_name")
        user.last_name = request.POST.get("last_name")
        user.is_active = "is_active" in request.POST
        user.save()

        messages.success(request, "Client updated successfully.")
        return redirect("adminpanel:manage_clients")

    messages.error(request, "Invalid request.")
    return redirect("adminpanel:manage_clients")


# --------------------------
# DELETE CLIENT
# --------------------------
@admin_required
@transaction.atomic
def delete_client(request, pk):
    user = get_object_or_404(User, pk=pk, is_superuser=False)

    if request.method == "POST":
        user.delete()
        messages.success(request, "Client deleted successfully.")
        return redirect("adminpanel:manage_clients")

    messages.error(request, "Invalid delete request.")
    return redirect("adminpanel:manage_clients")


# ====================================================
#                STRATEGY MANAGEMENT
# ====================================================

@admin_required
def manage_strategies(request):
    if request.method == "POST":
        form = StrategyForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Strategy added successfully!")
            return redirect("adminpanel:manage_strategies")
    else:
        form = StrategyForm()

    strategies = Strategy.objects.all()
    return render(request, "adminpanel/manage_strategies.html", {
        "form": form,
        "strategies": strategies
    })


@admin_required
def add_strategy(request):
    if request.method == "POST":

        Strategy.objects.create(
            name=request.POST.get("name"),
            symbol=request.POST.get("symbol"),
            exchange=request.POST.get("exchange"),
            point_value=request.POST.get("point_value"),
            ema_short=request.POST.get("ema_short"),
            ema_long=request.POST.get("ema_long"),
            fixed_sl_pct=request.POST.get("fixed_sl_pct"),
            trail_sl_pct=request.POST.get("trail_sl_pct"),
            breakout_buffer=request.POST.get("breakout_buffer"),
            margin_factor=request.POST.get("margin_factor", 0.15),
        )

        messages.success(request, "Strategy added successfully.")
        return redirect("adminpanel:manage_strategies")

    messages.error(request, "Invalid request.")
    return redirect("adminpanel:manage_strategies")


@admin_required
def edit_strategy(request, strategy_id):
    strategy = get_object_or_404(Strategy, id=strategy_id)

    if request.method == "POST":
        form = StrategyForm(request.POST, instance=strategy)
        if form.is_valid():
            form.save()
            messages.success(request, "Strategy updated successfully!")
            return redirect("adminpanel:manage_strategies")

    return redirect("adminpanel:manage_strategies")



@admin_required
def delete_strategy(request, strategy_id):
    strategy = get_object_or_404(Strategy, id=strategy_id)
    strategy.delete()
    messages.success(request, "Strategy deleted successfully!")
    return redirect("adminpanel:manage_strategies")

