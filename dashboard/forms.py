from django import forms
from backtest_runner.models import AngelOneKey, RunRequest, Strategy
from django import forms


class RunRequestForm(forms.ModelForm):
    class Meta:
        model = RunRequest
        fields = ["strategy"]
        widgets = {
            "strategy": forms.Select(attrs={"class": "form-select"})
            # "api_key": forms.Select(attrs={"class": "form-select"}),
            # "input_csv": forms.ClearableFileInput(attrs={"class": "form-control"}),
        }



class AngelOneKeyForm(forms.ModelForm):
    class Meta:
        model = AngelOneKey
        fields = ["client_code", "password", "totp_secret", "api_key"]
        widgets = {
            "password": forms.PasswordInput(render_value=True),
            "totp_secret": forms.PasswordInput(render_value=True),
            "api_key": forms.PasswordInput(render_value=True),
        }

class LiveBacktestForm(forms.Form):
    strategy = forms.ModelChoiceField(queryset=Strategy.objects.all(), label="Select Strategy")
    # interval = forms.ChoiceField(choices=[("ONE_MINUTE","1min"),("FIVE_MINUTES","5min"),("ONE_HOUR","1h")])
    from_date = forms.DateTimeField(widget=forms.DateTimeInput(attrs={'type': 'datetime-local'}))
    to_date = forms.DateTimeField(widget=forms.DateTimeInput(attrs={'type': 'datetime-local'}))


# class LiveBacktestForm(forms.Form):
#     exchange = forms.ChoiceField(
#         choices=[("NSE", "NSE"), ("BSE", "BSE"), ("MCX", "MCX")],
#         widget=forms.Select(attrs={"class": "form-select"})
#     )
#
#     interval = forms.ChoiceField(
#         choices=[
#             ("ONE_MINUTE", "1 Minute"),
#             ("FIVE_MINUTE", "5 Minutes"),
#             ("FIFTEEN_MINUTE", "15 Minutes"),
#             ("THIRTY_MINUTE", "30 Minutes"),
#             ("ONE_HOUR", "1 Hour"),
#             ("ONE_DAY", "Daily")
#         ],
#         widget=forms.Select(attrs={"class": "form-select"})
#     )
#
#     symbol_token = forms.CharField(widget=forms.TextInput(attrs={"class": "form-control"}))
#     from_date = forms.DateTimeInput()
#     to_date = forms.DateTimeInput()
#
#     from_date = forms.DateTimeField(
#         widget=forms.DateTimeInput(attrs={"type": "datetime-local", "class": "form-control"})
#     )
#     to_date = forms.DateTimeField(
#         widget=forms.DateTimeInput(attrs={"type": "datetime-local", "class": "form-control"})
#     )

