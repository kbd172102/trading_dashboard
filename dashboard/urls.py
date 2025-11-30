# dashboard/urls.py
from django.urls import path
from . import views

app_name = "dashboard"

urlpatterns = [
    path("", views.dashboard_home, name="home"),
    path("pnl_report/", views.pnl_report, name="pnl_report"),
    path("reports/", views.reports, name="reports"),
    path("api-integration/", views.api_integration, name="api_integration"),
    path("live-backtest/", views.live_backtest, name="live_backtest"),

]
