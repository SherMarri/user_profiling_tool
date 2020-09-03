import os
import shutil
import pandas as pd
import numpy as np
from unittest import TestCase
from datetime import datetime, timedelta

import settings
from classifiers.gender_classifier import GenderClassifier
from user_profiler.profiler import UserProfiler
from user_profiler import exceptions
from storage_handler.handler import Handler
from storage_handler import exceptions as storage_exceptions
from s3_wrapper import exceptions as s3_exceptions
from s3_wrapper import S3Utils


TEST_DATA_DIRECTORY = os.path.join(os.path.dirname(__file__), 'data')
S3_BASE_DIRECTORY = 'tests'

s3 = S3Utils()


class TestMethodsExistence(TestCase):
  def setUp(self):
    self.profiler = UserProfiler()

  def test_methods_existence(self):
    self.assertTrue(hasattr(self.profiler, 'set_handler'))
    self.assertTrue(hasattr(self.profiler, '_setup'))
    self.assertTrue(hasattr(self.profiler, '_get_SES_client'))
    self.assertTrue(hasattr(self.profiler, '_get_gender_classifier'))
    self.assertTrue(hasattr(self.profiler, 'profile'))
    self.assertTrue(hasattr(self.profiler, '_validate'))
    self.assertTrue(hasattr(self.profiler, '_validate_key'))
    self.assertTrue(hasattr(self.profiler, '_validate_date_range'))
    self.assertTrue(hasattr(self.profiler, '_validate_date_format'))
    self.assertTrue(hasattr(self.profiler, '_validate_email'))
    self.assertTrue(hasattr(self.profiler, '_check_report_exists'))
    self.assertTrue(hasattr(self.profiler, '_generate_processed_file_key'))
    self.assertTrue(hasattr(self.profiler, '_download_dataset'))
    self.assertTrue(hasattr(self.profiler, '_get_chunk_size'))
    self.assertTrue(hasattr(self.profiler, '_process_chunk'))
    self.assertTrue(hasattr(self.profiler, '_profile_users'))
    self.assertTrue(hasattr(self.profiler, '_upload_processed_dataset'))
    self.assertTrue(hasattr(self.profiler, '_delete_files'))
    self.assertTrue(hasattr(self.profiler, '_get_presigned_url'))
    self.assertTrue(hasattr(self.profiler, '_send_email'))  # TODO


class TestSetHandler(TestCase):
  def test_set_handler(self):
    profiler = UserProfiler()
    handler = Handler()
    profiler.set_handler(handler)
    self.assertEqual(profiler.handler, handler)
  
  def test_none_handler(self):
    profiler = UserProfiler()
    with self.assertRaises(ValueError):
      profiler.set_handler(None)
  
  def test_instance_type(self):
    profiler = UserProfiler()
    with self.assertRaises(TypeError):
      profiler.set_handler('invalid_type')


class TestSetup(TestCase):
  def test_setup(self):
    profiler = UserProfiler()
    
    self.assertIsNotNone(getattr(profiler, '_expiration', None))
    self.assertIsInstance(profiler._expiration, int)

    self.assertIsNotNone(getattr(profiler, '_ses', None))

class TestValidateKey(TestCase):
  def setUp(self):
    handler = Handler()
    self.profiler = UserProfiler(handler)
  
  def test_valid_key(self):
    file_path = f'{S3_BASE_DIRECTORY}/validate_key.csv'
    result = self.profiler._validate_key(file_path)
    self.assertEqual(result, None)

  def test_invalid_extension(self):
    file_path = 'invalid.doc'
    with self.assertRaises(exceptions.BadExtensionException):
      self.profiler._validate_key(file_path)
  
  def test_file_not_found(self):
    file_path = f'{S3_BASE_DIRECTORY}/non_existing_key.csv'
    with self.assertRaises(FileNotFoundError):
      result = self.profiler._validate_key(file_path)


class TestValidateDateRange(TestCase):
  def setUp(self):
    self.profiler = UserProfiler()
  
  def test_valid_date(self):
    start_date = '2020-04-01'
    end_date = '2020-05-01'
    self.profiler._validate_date_range(start_date, end_date)

  def test_invalid_date(self):
    start_date = '2020-04-01'
    end_date = '2020-03-01'
    with self.assertRaises(exceptions.InvalidDateRangeException):
      self.profiler._validate_date_range(start_date, end_date)


class TestValidateDateFormat(TestCase):
  def setUp(self):
    self.profiler = UserProfiler()
  
  def test_valid_date(self):
    date = '2020-04-01'
    self.profiler._validate_date_format(date)

  def test_invalid_date(self):
    date = '2020-04-at'
    with self.assertRaises(exceptions.InvalidDateFormatException):
      self.profiler._validate_date_format(date)


class TestValidateEmail(TestCase):
  def setUp(self):
    self.profiler = UserProfiler()
  
  def test_valid_email(self):
    email = 'test@citibeats.net'
    self.profiler._validate_email(email)

  def test_invalid_ex(self):
    email = 'invalid_email.com'
    with self.assertRaises(exceptions.InvalidEmailException):
      self.profiler._validate_email(email)


class TestValidate(TestCase):
  def setUp(self):
    handler = Handler()
    self.profiler = UserProfiler(handler)
  
  def test_valid_input(self):
    key = f'{S3_BASE_DIRECTORY}/validate_key.csv'
    email = 'hello@citibeats.com'
    self.profiler._validate(key, email)

  def test_invalid_extension(self):
    key = 'test.doc'
    email = 'hello@citibeats.com'
    with self.assertRaises(exceptions.BadExtensionException):
      self.profiler._validate(key, email)

  def test_invalid_email(self):
    key = f'{S3_BASE_DIRECTORY}/validate_key.csv'
    email = 'hello.com'
    with self.assertRaises(exceptions.InvalidEmailException):
      self.profiler._validate(key, email)


class TestGetGenderClassifier(TestCase):
  def setUp(self):
    self.profiler = UserProfiler()
  
  def test_get_classifier(self):
    classifier = self.profiler._get_gender_classifier()
    self.assertIsInstance(classifier, GenderClassifier)

class TestCheckReportExists(TestCase):
  def setUp(self):
    handler = Handler()
    self.profiler = UserProfiler(handler)

  def test_existing_key(self):
    file_path = f'{S3_BASE_DIRECTORY}/report_exists.csv'
    exists = self.profiler._check_report_exists(file_path)
    self.assertIsInstance(exists, bool)
    self.assertTrue(exists)
  
  def test_invalid_key(self):
    file_path = 'invalid_file.csv'
    exists = self.profiler._check_report_exists(file_path)
    self.assertIsInstance(exists, bool)
    self.assertFalse(exists)


class TestGenerateProcessedFileKey(TestCase):
  def setUp(self):
    self.profiler = UserProfiler()
  
  def test_generate_processed_file_key(self):
    file_path = f'{S3_BASE_DIRECTORY}/report_exists.csv'
    expected_file_path = f'{S3_BASE_DIRECTORY}/user_profiling/report_exists.csv'
    processed_file_key = self.profiler._generate_processed_file_key(file_path)
    self.assertIsInstance(processed_file_key, str)
    self.assertEqual(processed_file_key, expected_file_path)
  
  def test_none_file(self):
    with self.assertRaises(ValueError):
      self.profiler._generate_processed_file_key(None)
  
  def test_empty_filename(self):
    with self.assertRaises(ValueError):
      self.profiler._generate_processed_file_key('')
  
  def test_invalid_type(self):
    with self.assertRaises(TypeError):
      self.profiler._generate_processed_file_key(20)
  

class TestDownloadDataset(TestCase):
  def setUp(self):
    handler = Handler()
    self.profiler = UserProfiler(handler)
    self.file_to_delete = None
  
  def tearDown(self):
    if self.file_to_delete:
      os.remove(self.file_to_delete)

  def test_downloaded_file_exists(self):
    file_path = f'{S3_BASE_DIRECTORY}/download_test.csv'
    downloaded_file_path = self.profiler._download_dataset(file_path)
    self.file_to_delete = downloaded_file_path
    self.assertTrue(os.path.exists(downloaded_file_path))

  def test_file_not_found(self):
    file_path = 'invalid_test_path.csv'
    with self.assertRaises(s3_exceptions.DownloadFileException):
      downloaded_file_path = self.profiler._download_dataset(file_path)
      self.file_to_delete = downloaded_file_path


class TestGetChunkSize(TestCase):
  def setUp(self):
    handler = Handler()
    self.profiler = UserProfiler(handler)
  
  def test_return_type(self):
    path = os.path.join(TEST_DATA_DIRECTORY, 'chunk_size.csv')
    chunk_size = self.profiler._get_chunk_size(path)
    self.assertIsInstance(chunk_size, int)
  
  def test_single_row(self):
    path = os.path.join(TEST_DATA_DIRECTORY, 'single_row.csv')
    chunk_size = self.profiler._get_chunk_size(path)
    self.assertEqual(chunk_size, 1)
  
  def test_empty_dataset(self):
    path = os.path.join(TEST_DATA_DIRECTORY, 'empty_dataset.csv')
    chunk_size = self.profiler._get_chunk_size(path)
    self.assertEqual(chunk_size, 1)


class TestProcessChunk(TestCase):
  def setUp(self):
    self.profiler = UserProfiler()
    path = os.path.join(TEST_DATA_DIRECTORY, 'dataset.csv')
    self.df = pd.read_csv(path, sep=UserProfiler._DEFAULT_SEPARATOR, nrows=100)
  
  def test_process_chunk(self):
    df_processed = self.profiler._process_chunk(self.df)
    columns = df_processed.columns.tolist()
    columns_to_check = ['gender_class']
    for c in columns_to_check:
      self.assertIn(c, columns)


class TestProfileUsers(TestCase):
  def setUp(self):
    handler = Handler()
    self.profiler = UserProfiler(handler)

    # Setup files for profiling
    self.directory = os.path.join(TEST_DATA_DIRECTORY, 'test_profile_users')
    test_file_1 = os.path.join(TEST_DATA_DIRECTORY, 'dataset.csv')
    test_file_2 = os.path.join(TEST_DATA_DIRECTORY, 'dataset_1.csv')
    if not os.path.exists(self.directory):
      os.mkdir(self.directory)
    self.test_file_1 = shutil.copy(test_file_1, self.directory)
    self.test_file_2 = shutil.copy(test_file_2, self.directory)
    self.processed_file = None
  
  def tearDown(self):
    shutil.rmtree(self.directory)
    if self.processed_file:
      os.remove(self.processed_file)

  def test_profile_users_1(self):
    processed_file = self.profiler._profile_users(self.test_file_1)
    self.processed_file = processed_file
    self.assertTrue(os.path.exists(processed_file))

    # Check if file has gender_class column
    df = pd.read_csv(processed_file, nrows=10, sep=UserProfiler._DEFAULT_SEPARATOR)
    columns = df.columns.tolist()
    self.assertIn('gender_class', columns)
  
  def test_profile_users_2(self):
    processed_file = self.profiler._profile_users(self.test_file_2)
    self.processed_file = processed_file
    self.assertTrue(os.path.exists(processed_file))

    # Check if file has gender_class column
    df = pd.read_csv(processed_file, nrows=10, sep=UserProfiler._DEFAULT_SEPARATOR)
    columns = df.columns.tolist()
    self.assertIn('gender_class', columns)


class TestUploadProcessedDataset(TestCase):
  def setUp(self):
    handler = Handler()
    self.profiler = UserProfiler(handler)
  
  def test_file_upload(self):
    path = os.path.join(TEST_DATA_DIRECTORY, 'upload_me.csv')
    s3_key = 'user_profiling/upload_me.csv'
    self.profiler._upload_processed_dataset(s3_key, path)

    exists = s3.file_exists(s3_key)
    self.assertTrue(exists)
    s3.delete_object(s3_key)
  
  def test_file_not_found_locally(self):
    processed_file_path = 'non_existing_dataset_path.csv'
    original_s3_key = 'user_profiling/processed_dataset_path.csv'
    with self.assertRaises(FileNotFoundError):
      self.profiler._upload_processed_dataset(original_s3_key, processed_file_path)
  

class TestDeleteFiles(TestCase):
  def setUp(self):
    handler = Handler()
    self.profiler = UserProfiler(handler)

    # Setup files for deletion
    self.directory = os.path.join(TEST_DATA_DIRECTORY, 'test_delete')
    self.files_to_copy = [
      os.path.join(TEST_DATA_DIRECTORY, 'delete_me_1.txt'),
      os.path.join(TEST_DATA_DIRECTORY, 'delete_me_2.txt'),
    ]
    self.files_to_delete = []
    if not os.path.exists(self.directory):
      os.mkdir(self.directory)
    for f in self.files_to_copy:
      self.files_to_delete.append(shutil.copy(f, self.directory))
  
  def tearDown(self):
    shutil.rmtree(self.directory)

  def test_delete_files(self):
    self.profiler._delete_files(self.files_to_delete)
    for f in self.files_to_delete:
      self.assertFalse(os.path.exists(f))
  

class TestGetPresignedUrl(TestCase):
  def setUp(self):
    handler = Handler()
    self.profiler = UserProfiler(handler)
  
  def test_method(self):
    file_path = 'orgX/handler_test_data/response_twitter1.json'
    url = self.profiler._get_presigned_url(file_path)
    self.assertIsInstance(url, str)


class TestSendEmail(TestCase):
  def setUp(self):
    self.profiler = UserProfiler()
  
  def test_return_type(self):
    presigned_url = 'https://test-url.com'
    email = 'labs@citibeats.net'
    sent = self.profiler._send_email(email, presigned_url)
    self.assertIsInstance(sent, bool)


class TestProfile1(TestCase):
  def setUp(self):
    handler = Handler()
    self.profiler = UserProfiler(handler)
    self.s3_key = f'{S3_BASE_DIRECTORY}/test_profile_method.csv'
    self.processed_object_key = self.profiler._generate_processed_file_key(self.s3_key)
  
  def tearDown(self):
    try:
      s3.delete_object(self.processed_object_key)
    except:
      pass
  
  def test_profile(self):
    email = 'falak.sher@venturedive.com'
    self.profiler.profile(self.s3_key, email)
    self.assertTrue(s3.file_exists(self.processed_object_key))


class TestProfile2(TestCase):
  def setUp(self):
    handler = Handler()
    self.profiler = UserProfiler(handler)
    self.s3_key = f'{S3_BASE_DIRECTORY}/test2.csv'
    self.processed_object_key = self.profiler._generate_processed_file_key(self.s3_key)
  
  def tearDown(self):
    try:
      s3.delete_object(self.processed_object_key)
    except:
      pass
  
  def test_profile(self):
    email = 'falak.sher@venturedive.com'
    self.profiler.profile(self.s3_key, email)
    self.assertTrue(s3.file_exists(self.processed_object_key))
