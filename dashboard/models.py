from django.db import models
from django.conf import settings

class BacktestResult(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)

    # Strategy: silver_mini, gold_mini, nifty, bank_nifty
    strategy = models.CharField(max_length=100)

    # Raw input file name (CSV or live-session CSV)
    input_filename = models.CharField(max_length=255)

    # Output file paths
    events_csv = models.CharField(max_length=255)
    trades_csv = models.CharField(max_length=255)
    pnl_csv = models.CharField(max_length=255)
    balance_png = models.CharField(max_length=255)

    # API Key used
    api_key = models.ForeignKey(
        "backtest_runner.AngelOneKey",
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )

    status = models.CharField(max_length=30, default="completed")
    error = models.TextField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.user} - {self.strategy} - {self.created_at}"
