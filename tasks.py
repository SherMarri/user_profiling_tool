from celery import Celery
import settings
import os

from storage_handler import Handler
from user_profiler import UserProfiler


app = Celery('user_profiler', broker=os.getenv('CELERY_BROKER_ENDPOINT'))
handler = Handler()
profiler = UserProfiler(handler)


@app.task
def profile_users(s3_key: str, email: str):
  try:
    profiler.profile(s3_key, email)
  except Exception as e:
    print('Failed to complete this operation.')
    raise e
