from django.contrib.auth import get_user_model
from backtest_runner.models import AngelOneKey
from django import forms
from backtest_runner.models import Strategy

class StrategyForm(forms.ModelForm):
    class Meta:
        model = Strategy
        fields = [
            "name",
            "exchange",
            "symbol",
            "point_value",
            "ema_short",
            "ema_long",
            "fixed_sl_pct",
            "trail_sl_pct",
            "breakout_buffer",
            "margin_factor",
        ]

        widgets = {
            'name': forms.TextInput(attrs={'class': 'form-control'}),
            'exchange': forms.TextInput(attrs={'class': 'form-control'}),
            'symbol': forms.TextInput(attrs={'class': 'form-control'}),
            'point_value': forms.NumberInput(attrs={'class': 'form-control'}),
            'ema_short': forms.NumberInput(attrs={'class': 'form-control'}),
            'ema_long': forms.NumberInput(attrs={'class': 'form-control'}),
            'fixed_sl_pct': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.0001'}),
            'trail_sl_pct': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.0001'}),
            'breakout_buffer': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.0001'}),
            'margin_factor': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.0001'}),
        }


  # adjust path if needed

User = get_user_model()


class AdminAddClientForm(forms.ModelForm):
    password = forms.CharField(widget=forms.PasswordInput)

    class Meta:
        model = User
        fields = ["username", "email", "password"]


class AdminEditClientForm(forms.ModelForm):
    new_password = forms.CharField(
        required=False,
        widget=forms.PasswordInput,
        help_text="Leave blank to keep old password"
    )

    class Meta:
        model = User
        fields = ["username", "email"]


class AdminClientAPIForm(forms.ModelForm):
    class Meta:
        model = AngelOneKey
        fields = ["client_code", "password", "totp_secret", "api_key"]
        widgets = {
            "password": forms.PasswordInput(),
        }
