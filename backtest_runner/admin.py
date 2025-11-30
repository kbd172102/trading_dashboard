# backtest_runner/admin.py
from django.contrib import admin
from .models import Strategy, AngelOneKey, RunRequest

@admin.register(Strategy)
class StrategyAdmin(admin.ModelAdmin):
    list_display = ['name', 'exchange', 'symbol']

@admin.register(AngelOneKey)
class AngelOneKeyAdmin(admin.ModelAdmin):
    list_display = ('id', 'user', 'client_code', 'api_key', 'created_at')
    search_fields = ('user__username', 'client_code', 'api_key')


@admin.register(RunRequest)
class RunRequestAdmin(admin.ModelAdmin):
    list_display = ('id','user','strategy','status','created_at','finished_at')
    list_filter = ('status','strategy')
    readonly_fields = ('created_at','finished_at')
