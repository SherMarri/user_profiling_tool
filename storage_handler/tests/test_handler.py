"""Unit tests for StorageHandlerBase class"""

from unittest import TestCase
import os
import shutil
import uuid
import settings

from storage_handler import exceptions
from storage_handler.handler import Handler
from s3_wrapper import S3Utils, s3_exceptions

TEST_DATA_DIRECTORY = os.path.join(os.path.dirname(__file__), 'data')
S3_BASE_DIRECTORY = 'tests'


s3 = S3Utils()


class TestMethodsExistence(TestCase):
  def setUp(self):
    self.handler = Handler()

  def test_methods_existence(self):
    self.assertTrue(hasattr(self.handler, 'file_exists'))
    self.assertTrue(hasattr(self.handler, 'download_file'))
    self.assertTrue(hasattr(self.handler, 'upload_file'))
    self.assertTrue(hasattr(self.handler, 'delete_local_file'))
    self.assertTrue(hasattr(self.handler, 'get_presigned_url'))


class TestFileExists(TestCase):
  def setUp(self):
    self.handler = Handler()
  
  def test_exists(self):
    file_path = f'{S3_BASE_DIRECTORY}/user_profiling/report_exists.csv'
    exists = self.handler.file_exists(file_path)
    self.assertIsInstance(exists, bool)
    self.assertTrue(exists)

  def test_does_not_exist(self):
    file_path = 'invalid_test_path.csv'
    exists = self.handler.file_exists(file_path)
    self.assertIsInstance(exists, bool)
    self.assertFalse(exists)


class TestDownloadFile(TestCase):
  def setUp(self):
    self.handler = Handler()    
    
  def test_download_file(self):
    file_path = f'{S3_BASE_DIRECTORY}/download_test.csv'
    downloaded_file = self.handler.download_file(file_path)
    self.assertTrue(os.path.exists(downloaded_file))
    os.remove(downloaded_file)

  def test_does_not_exist_on_s3(self):
    file_path = 'invalid_test_path.csv'
    with self.assertRaises(s3_exceptions.DownloadFileException):
      self.handler.download_file(file_path)


class TestUploadFile(TestCase):
  def setUp(self):
    self.handler = Handler()
    self.key = f'{S3_BASE_DIRECTORY}/uploaded_file.csv'
  
  def tearDown(self):
    try:
      s3.delete_object(self.key)
    except:
      pass

  def test_upload_file(self):
    path = os.path.join(TEST_DATA_DIRECTORY, 'upload_me.csv')
    self.handler.upload_file(self.key, path)
    exists = s3.file_exists(self.key)


class TestGetPresignedUrl(TestCase):
  def setUp(self):
    self.handler = Handler()
  
  def test_return_type(self):
    file_path = 'test_path.csv'
    url = self.handler.get_presigned_url(file_path, 2000)
    self.assertIsInstance(url, str)


class TestDeleteLocalFile(TestCase):
  def setUp(self):
    self.handler = Handler()

    # Setup file for deletion
    self.directory = os.path.join(TEST_DATA_DIRECTORY, 'test_delete')
    self.file_to_copy = os.path.join(TEST_DATA_DIRECTORY, 'delete_me.txt')
    if not os.path.exists(self.directory):
      os.mkdir(self.directory)
    
    self.file_to_delete = shutil.copy(self.file_to_copy, self.directory)
  
  def tearDown(self):
    shutil.rmtree(self.directory)

  def test_delete_file(self):
    self.handler.delete_local_file(self.file_to_delete)
    self.assertFalse(os.path.exists(self.file_to_delete))
  
  def test_deleting_non_existing_file(self):
    with self.assertRaises(FileNotFoundError):
      self.handler.delete_local_file('invalid_file.csv')