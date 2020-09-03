import os
from uuid import uuid1
from s3_wrapper import S3Utils


PathLike = os.PathLike


class Handler:
  def __init__(self):
    self._s3 = S3Utils()

  def file_exists(self, key: str) -> bool:
    return self._s3.file_exists(key)
  
  def get_presigned_url(self, key: str, expiration: int) -> str:
    return self._s3.generate_presigned_url(key, expiration)
  
  def download_file(self, key: str) -> PathLike:
    file_path = f'/tmp/{uuid1()}.csv'
    self._s3.download_file(key, file_path)
    return file_path
  
  def delete_local_file(self, file: str):
    if os.path.exists(file):
      os.remove(file)
    else:
      raise FileNotFoundError
  
  def upload_file(self, key: str, file: PathLike):
    if not os.path.exists(file):
      raise FileNotFoundError
    self._s3.upload_file(key, file)
