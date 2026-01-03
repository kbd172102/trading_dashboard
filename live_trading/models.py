# live_trading/models.py
from django.db import models

from accounts.forms import User


class LiveTick(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    token = models.CharField(max_length=20)
    ltp = models.FloatField()
    exchange_timestamp = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["user", "token", "exchange_timestamp"]),
        ]



class LiveCandle(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    token = models.CharField(max_length=20)
    interval = models.CharField(max_length=10, default="15m")

    start_time = models.DateTimeField()
    end_time = models.DateTimeField()

    open = models.FloatField()
    high = models.FloatField()
    low = models.FloatField()
    close = models.FloatField()

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('user','token','start_time')


class LivePosition(models.Model):
    SIDE_CHOICES = (
        ("LONG", "LONG"),
        ("SHORT", "SHORT"),
    )

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    token = models.CharField(max_length=20)

    side = models.CharField(max_length=5, choices=SIDE_CHOICES)
    entry_price = models.FloatField()

    lots = models.IntegerField()
    quantity = models.IntegerField()

    fixed_sl = models.FloatField()
    trailing_sl = models.FloatField()

    is_open = models.BooleanField(default=True)

    entry_time = models.DateTimeField(auto_now_add=True)
    exit_time = models.DateTimeField(null=True, blank=True)
    exit_price = models.FloatField(null=True, blank=True)

    pnl = models.FloatField(null=True, blank=True)
    exit_reason = models.CharField(max_length=50, null=True, blank=True)

    def __str__(self):
        return f"{self.user_id} {self.side} {self.token}"


class TradeStats(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)

    win_streak = models.IntegerField(default=0)
    loss_streak = models.IntegerField(default=0)

    trades_today = models.IntegerField(default=0)
    last_trade_day = models.DateField(null=True, blank=True)

    cooldown_until = models.DateTimeField(null=True, blank=True)

    def reset_daily(self, today):
        if self.last_trade_day != today:
            self.trades_today = 0
            self.last_trade_day = today
            self.save()

