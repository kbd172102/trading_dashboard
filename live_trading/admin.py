from django.contrib import admin
from .models import LiveTick, LiveCandle

admin.site.register(LiveTick)
admin.site.register(LiveCandle)