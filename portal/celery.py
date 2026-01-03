import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "portal.settings")

app = Celery("portal")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

app.conf.beat_schedule = {
    "run-strategy-every-sec": {
        "task": "live_trading.tasks.process_live_data",
        "schedule": 1.0
    }
}
