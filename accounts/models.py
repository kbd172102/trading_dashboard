from django.contrib.auth.models import AbstractUser
from django.db import models

# class User(AbstractUser):
#     # keep minimal — use is_staff/is_superuser for admin
#     # add phone/email preferences later only if client asks
#     pass


class User(AbstractUser):
    @property
    def is_client(self):
        return not self.is_superuser
