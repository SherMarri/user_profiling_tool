import os
import pandas as pd
from uuid import uuid1
from unittest import TestCase
from keras.preprocessing.text import Tokenizer
from keras import Model

import settings
from classifiers.gender_classifier import GenderClassifier, exceptions


TEST_DATA_DIRECTORY = os.path.join(os.path.dirname(__file__), 'data')
model_directory = os.getenv('GENDER_CLASSIFIER_MODEL_DIRECTORY')
tokenizer_directory = os.getenv('GENDER_CLASSIFIER_TOKENIZER_DIRECTORY')
classifier = GenderClassifier(model_directory, tokenizer_directory)


class TestValidatePath(TestCase):
  
  def test_valid_path(self):
    self.assertTrue(classifier._validate_path(model_directory))
  
  def test_invalid_path(self):
    with self.assertRaises(IOError):
      self.assertTrue(classifier._validate_path(str(uuid1())))


class TestGetModelName(TestCase):
  def test_method(self):
    name = classifier._get_model_name()
    self.assertIsInstance(name, str)


class TestSetupParams(TestCase):
  def test_method(self):
    self.assertIsInstance(classifier._col_X, list)
    self.assertIsInstance(classifier._maxlen, list)

class TestLoadTokenizer(TestCase):
  def test_method(self):
    tokenizer = classifier._load_tokenizer(tokenizer_directory)
    self.assertIsInstance(tokenizer, Tokenizer)


class TestLoadModel(TestCase):
  def test_method(self):
    model = classifier._load_model(model_directory)
    self.assertIsInstance(model, Model)


class TestPredict(TestCase):
  def test_valid_dataset(self):
    path = os.path.join(TEST_DATA_DIRECTORY, 'dataset.csv')
    df = pd.read_csv(path, sep=';', nrows=100)
    resultant_df = classifier.predict(df)
    columns = resultant_df.columns.tolist()
    self.assertIn('gender_class', columns)
  
  def test_invalid_dataset(self):
    path = os.path.join(TEST_DATA_DIRECTORY, 'dataset_missing_columns.csv')
    df = pd.read_csv(path, sep=';', nrows=100)
    with self.assertRaises(exceptions.PredictionException):
      resultant_df = classifier.predict(df)
