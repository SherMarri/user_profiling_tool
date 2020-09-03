
class ClassifierInitException(Exception):
  """
  Should be raised when system fails to initialize a classifier.
  """
  def __init__(self, *args):
    if args:
      self.message = args[0]
    else:
      self.message = None
  
  def __str__(self):
    if self.message:
      return f'ClassifierInitException, {self.message}'
    else:
      return 'ClassifierInitException: Failed to initialize the classifier.'


class PredictionException(Exception):
  """
  Should be raised when classifier fails while predicting a dataset.
  """
  def __init__(self, *args):
    if args:
      self.message = args[0]
    else:
      self.message = None
  
  def __str__(self):
    if self.message:
      return f'PredictionException, {self.message}'
    else:
      return 'PredictionException: Failed to predict the dataset.'


class PreprocessException(Exception):
  """
  Should be raised when classifier fails while preprocess a dataset.
  """
  def __init__(self, *args):
    if args:
      self.message = args[0]
    else:
      self.message = None
  
  def __str__(self):
    if self.message:
      return f'PreprocessException, {self.message}'
    else:
      return 'PreprocessException: Failed to preprocess the dataset.'
