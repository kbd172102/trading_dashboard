from django.contrib.auth.models import AbstractUser
from django.db import models

class User(AbstractUser):
    @property
    def is_client(self):
        return not self.is_superuser

    trading_enabled = models.BooleanField(default=False)