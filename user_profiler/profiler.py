import os
import boto3
import re
import math
import random
import pandas as pd
from typing import List
from uuid import uuid1
from datetime import datetime
from botocore.exceptions import ClientError

import settings
from storage_handler.handler import Handler
from classifiers.gender_classifier import GenderClassifier
from . import exceptions


PathLike = os.PathLike


class UserProfiler:

  _EXTENSION = 'csv'
  _FORMAT = "%Y-%m-%d"
  _CHUNK_SIZE_IN_BYTES = 10485760  # 10 MB
  _DEFAULT_SEPARATOR = ';'
  _EMAIL_CHARSET = 'UTF-8'
  _EMAIL_SUBJECT = 'Citibeats - Your User Profile Report Is Ready'

  _EMAIL_TEXT = """Hello,

Your user profile report is ready. Please click the link below to download it:
{0}

Note: This link will expire in 7 days.
  """

  def __init__(self, handler: Handler=None):
    if handler:
      self.set_handler(handler)
    
    self._setup()
  
  def _setup(self):
    expiration_in_days = os.getenv('EXPIRATION_IN_DAYS', None)
    if expiration_in_days is None:
      raise KeyError('Missing environment variable "EXPIRATION_IN_DAYS".')
    expiration_in_days = int(expiration_in_days)
    self._expiration = expiration_in_days * 86400  # In seconds
    self._ses = self._get_SES_client()
    self._gender_classifier = self._get_gender_classifier()
  
  def _get_gender_classifier(self) -> GenderClassifier:
    model_directory = os.getenv('GENDER_CLASSIFIER_MODEL_DIRECTORY', None)
    tokenizer_directory = os.getenv('GENDER_CLASSIFIER_TOKENIZER_DIRECTORY', None)
    if model_directory is None:
      raise KeyError('Missing environment variable "GENDER_CLASSIFIER_MODEL_DIRECTORY".')

    if tokenizer_directory is None:
      raise KeyError('Missing environment variable "GENDER_CLASSIFIER_TOKENIZER_DIRECTORY".')

    classifier = GenderClassifier(model_directory, tokenizer_directory)
    return classifier
  
  def _get_SES_client(self):
    try:
      region_name = os.getenv('AWS_REGION_NAME')
      profile_name = os.getenv('AWS_PROFILE_NAME')
      if os.getenv('SES_EMAIL') is None:
        raise KeyError('"SES_EMAIL" environment variable missing.')
      session = boto3.Session(profile_name=profile_name)
      return session.client('ses', region_name=region_name)
    except Exception as e:
      raise exceptions.SESClientException

  def set_handler(self, handler: Handler):
    if handler is None:
      raise ValueError('"handler" cannot be None.')
    if not isinstance(handler, Handler):
      raise TypeError('"handler" must be of type Handler.')
    self.handler = handler
  
  def profile(self, s3_key: str, email: str):
    self._validate(s3_key, email)
    exists = self._check_report_exists(s3_key)
    processed_file_key = self._generate_processed_file_key(s3_key)
    if not exists:
      files_to_delete = []
      try:
        downloaded_file = self._download_dataset(s3_key)
        files_to_delete.append(downloaded_file)
        processed_file = self._profile_users(downloaded_file)
        files_to_delete.append(processed_file)
        processed_file_key = self._generate_processed_file_key(s3_key)
        self._upload_processed_dataset(processed_file_key, processed_file)
      except Exception as e:
        self._delete_files(files_to_delete)
        raise e
      finally:
        self._delete_files(files_to_delete)

    url = self._get_presigned_url(processed_file_key)
    sent = self._send_email(email, url)

  def _validate(self, key, email):
    self._validate_key(key)
    self._validate_email(email)
  
  def _validate_key(self, key: str):
    if key is None:
      raise ValueError('"key" cannot be None.')
    if not isinstance(key, str):
      raise TypeError('"key" must be of type str.')
    if len(key) == 0:
      raise ValueError('"key" cannot be empty.')
    extension = key.split('.')[-1]
    if extension != self._EXTENSION:
      raise exceptions.BadExtensionException
    if not self.handler.file_exists(key):
      raise FileNotFoundError

  def _validate_date_range(self, start_date: str, end_date: str):
    self._validate_date_format(start_date)
    self._validate_date_format(end_date)
    start = datetime.strptime(start_date, self._FORMAT)
    end = datetime.strptime(end_date, self._FORMAT)

    if start >= end:
      raise exceptions.InvalidDateRangeException

  def _validate_email(self, email: str):
    email_regex = re.compile(r"[^@]+@[^@]+\.[^@]+")
    if not email_regex.fullmatch(email):
      raise exceptions.InvalidEmailException

  def _validate_date_format(self, date: str):
    try:
      date = datetime.strptime(date, self._FORMAT)
    except Exception as e:
      raise exceptions.InvalidDateFormatException

  def _check_report_exists(self, s3_key: str) -> bool:
    processed_key = self._generate_processed_file_key(s3_key)
    exists = self.handler.file_exists(processed_key)
    return exists
  
  def _generate_processed_file_key(self, s3_key: str) -> str:
    if s3_key is None:
      raise ValueError('"s3_key" cannot be None.')
    elif not isinstance(s3_key, str):
      raise TypeError('"s3_key" must be of type str.')
    elif len(s3_key) == 0:
      raise ValueError('"s3_key" cannot be empty.')
    tokens = s3_key.split('/')
    tokens[-1] = 'user_profiling/' + tokens[-1]
    processed_key = '/'.join(tokens)
    return processed_key

  def _get_presigned_url(self, s3_key: str) -> str:
    return self.handler.get_presigned_url(s3_key, self._expiration)

  def _send_email(self, email: str, url: str) -> bool:
    try:
      self._validate_email(email)
      text = self._EMAIL_TEXT.format(url)
      destination = {
        'ToAddresses': [
          email,
        ]
      }
      message = {
        'Body': {
          'Text': {
            'Charset': self._EMAIL_CHARSET,
            'Data': text,
          },
        },
        'Subject': {
          'Charset': self._EMAIL_CHARSET,
          'Data': self._EMAIL_SUBJECT,
        }
      }
      sender = os.getenv('SES_EMAIL')
      response = self._ses.send_email(
        Destination=destination,
        Message=message,
        Source=sender
      )
      return True
    except ClientError as e:
      raise exceptions.EmailException(e.response['Error']['Message'])
    except:
        raise exceptions.EmailException

  def _download_dataset(self, s3_key: str) -> str:
    file_path = self.handler.download_file(s3_key)
    return file_path

  def _profile_users(self, file: str) -> str:
    print('Profiling started...')
    chunk_size = self._get_chunk_size(file)
    df = pd.read_csv(file, chunksize=chunk_size, sep=self._DEFAULT_SEPARATOR)
    processed_file = f'/tmp/{uuid1()}.csv'
    include_header = True
    try:
      for chunk in df:
        print('Processing chunk...')
        processed_chunk = self._process_chunk(chunk)
        headers = processed_chunk.columns.tolist() if include_header else False
        print('Writing chunk...')
        processed_chunk.to_csv(
          path_or_buf=processed_file, sep=self._DEFAULT_SEPARATOR,
          mode='a', header=headers, index=False
        )
        include_header = False
    except Exception as e:
      if os.path.exists(processed_file):
        self.handler.delete_local_file(processed_file)  # Clean up
      raise e
    return processed_file
      
  def _process_chunk(self, df: pd.DataFrame):
    df_copy = df.copy(deep=True)
    df_copy = self._gender_classifier.predict(df_copy)
    return df_copy
  
  def _get_chunk_size(self, file) -> int:
    '''Returns number of rows per chunk'''
    total_sample_rows = 10
    df = pd.read_csv(file, nrows=total_sample_rows, sep=self._DEFAULT_SEPARATOR)
    rows, cols = df.shape
    if rows < total_sample_rows:
      return rows if rows > 0 else 1

    bytes_in_sample = df.memory_usage(deep=True).sum()
    bytes_per_row = math.ceil(bytes_in_sample / rows)
    rows_per_chunk = math.ceil(self._CHUNK_SIZE_IN_BYTES / bytes_per_row)
    return rows_per_chunk

  def _upload_processed_dataset(self, s3_key: str, file: str):
    self.handler.upload_file(s3_key, file)

  def _delete_files(self, files: List[PathLike]):
    for file in files:
      try:
        self.handler.delete_local_file(file)
      except:
        pass
