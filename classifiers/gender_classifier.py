import os
from os.path import join
import pandas as pd
import numpy as np
import pickle
from keras.models import load_model
from keras import Model
from keras.preprocessing.sequence import pad_sequences
from keras.preprocessing.text import Tokenizer


from . import exceptions


PathLike = os.PathLike
DataFrame = pd.DataFrame


class GenderClassifier:

  _TAG = 'gender'
  _FEATURES = {
    'name': (0, 50),
    'bio':(2, 160),
    'screenname': (1, 15),
  }
  _LEVEL = 'char'
  _MODEL_FORMAT = 'character_embedding'

  def __init__(self, model_directory: PathLike, tokenizer_directory: PathLike):
    try:
      self._validate_path(model_directory)
      self._validate_path(tokenizer_directory)
      self._setup(model_directory, tokenizer_directory)
    except Exception as e:
      raise exceptions.ClassifierInitException(str(e))
  
  def _validate_path(self, path: PathLike) -> bool:
    if not os.path.isdir(path):
      raise IOError(f'{path}: This is not a valid directory.')
    return True
   
  def _setup(self, model_directory: PathLike, tokenizer_directory: PathLike):
    self._setupParams()
    self._tokenizer = self._load_tokenizer(tokenizer_directory)
    self._model = self._load_model(model_directory)
  
  def _setupParams(self):
    features = self._FEATURES.values()
    self._col_X = [feature[0] for feature in features]
    self._maxlen = [feature[1] for feature in features]
  
  def _get_model_name(self) -> str:
    """
    Objective: gets the name of the prediction model

    Output:
        - model_name, str: name of the prediction model
    """
    name = '_'.join(self._FEATURES.keys())
    model_name = '{}_{}_{}'.format(self._TAG, self._MODEL_FORMAT, name)
    return model_name
  
  def _load_tokenizer(self, directory: PathLike) -> Tokenizer:
    """
    Objective: load an existing tokenizer if exists in the folder directory, else fit one from Keras on X and save it in this folder

    Inputs:
        - directory, PathLike: the path to the tokenizer file
    Outputs:
        - tokenizer, keras.preprocessing.text.Tokenizer: tokenizer for characeter embeddings
    """

    tokenizer = pickle.load(open(join(directory, 'tokenizer_{}.pkl'.format(self._LEVEL)), 'rb'))
    return tokenizer

  def _load_model(self, directory: PathLike) -> Model:
    """
    Objective: from a model check if it exists and load it otherwise create a new one
    
    Inputs:
        - directory, PathLike: the path where lie the models
    Outputs:
        - model, keras.model: a keras model to train from scratch
    """
    model_name = self._get_model_name()
    model = load_model(join(directory, '{}.h5'.format(model_name)))
    return model

  def _preprocess_inputs(self, X: np.array, maxlen: str):
    """
    Objective: preprocess the inputs/features for the model

    Inputs:
        - X, np.array: the features array
        - maxlen, int: the maximum length for each sequence
    Outputs: (generator)
        - X_ppd, np.array: X preprocessed
    """
    try:
      X_ppd = self._tokenizer.texts_to_sequences(X)
      X_ppd = pad_sequences(X_ppd, maxlen=maxlen)

      return X_ppd
    except Exception as e:
      raise exceptions.PreprocessException(str(e))

  def predict(self, dataset: DataFrame) -> DataFrame:
    try:
      # the data we need to apply the model
      X = dataset[['name', 'username', 'bio']].values

      # pre processing of the data before applying the model
      xtest = []
      for _col, _maxlen in zip(self._col_X, self._maxlen):
          _xtest = self._preprocess_inputs(X[:, _col].astype(str), maxlen=_maxlen)
          xtest.append(_xtest)
          
      #apply the model on the pre-processed inputs
      y_probas = self._model.predict(xtest)

      #convert probabilities in classes
      y_preds = y_probas.argmax(axis=1)

      #add the column to the DataFrame
      dataset.loc[:, '{}_class'.format(self._TAG)] = y_preds
      return dataset
    except Exception as e:
      raise exceptions.PredictionException(str(e))
