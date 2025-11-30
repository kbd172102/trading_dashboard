# backtest_runner/models.py
from django.db import models
from django.conf import settings

class Strategy(models.Model):
    """
    Stores strategy configuration parameters for different instruments.
    """
    name = models.CharField(max_length=50, unique=True)          # SILVERMINI, GOLDMINI, NIFTY, BANKNIFTY
    exchange = models.CharField(max_length=10)                   # MCX, NSE
    symbol = models.CharField(max_length=20)                     # SILVERM, GOLDM, NIFTY, BANKNIFTY
    point_value = models.FloatField()
    ema_short = models.IntegerField()
    ema_long = models.IntegerField()
    fixed_sl_pct = models.FloatField()
    trail_sl_pct = models.FloatField()
    breakout_buffer = models.FloatField()
    margin_factor = models.FloatField(default=0.15)             # percentage of capital per lot

    def __str__(self):
        return self.name


class AngelOneKey(models.Model):
    """
    Stores user AngelOne API credentials and tokens.
    """
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)

    client_code = models.CharField(max_length=50)
    password = models.CharField(max_length=50)
    totp_secret = models.CharField(max_length=100)
    api_key = models.CharField(max_length=200)

    jwt_token = models.TextField(blank=True, null=True)
    refresh_token = models.TextField(blank=True, null=True)
    feed_token = models.TextField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.user.username} - {self.client_code}"


class RunRequest(models.Model):
    """
    Stores information about a backtest run.
    No CSV files are stored; results can be returned to the user dynamically.
    """
    STATUS_CHOICES = [
        ('pending','Pending'),
        ('running','Running'),
        ('done','Done'),
        ('failed','Failed'),
    ]

    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    strategy = models.ForeignKey(Strategy, on_delete=models.SET_NULL, null=True, blank=True)
    api_key = models.ForeignKey(AngelOneKey, on_delete=models.SET_NULL, null=True, blank=True)

    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    error = models.TextField(blank=True, null=True)

    # Optionally store results as JSON in the DB (not mandatory)
    results = models.JSONField(blank=True, null=True)

    def __str__(self):
        return f"Run #{self.id} by {self.user} ({self.status})"
