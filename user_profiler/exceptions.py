class BadExtensionException(Exception):
  """
  Should be raised when an unrecognized file format is detected.
  """
  def __init__(self, *args):
    if args:
      self.message = args[0]
    else:
      self.message = None
  
  def __str__(self):
    if self.message:
      return f'BadExtensionException, {self.message}'
    else:
      return 'BadExtensionException: This is not a recognized file format.'


class InvalidDateFormatException(Exception):
  """
  Should be raised for an invalid date format.
  """
  def __init__(self, *args):
    if args:
      self.message = args[0]
    else:
      self.message = None
  
  def __str__(self):
    if self.message:
      return f'InvalidDateFormatException, {self.message}'
    else:
      return 'InvalidDateFormatException: This is not a valid date format.'


class InvalidDateRangeException(Exception):
  """
  Should be raised for an invalid date range.
  """
  def __init__(self, *args):
    if args:
      self.message = args[0]
    else:
      self.message = None
  
  def __str__(self):
    if self.message:
      return f'InvalidDateRangeException, {self.message}'
    else:
      return 'InvalidDateRangeException: This is not a valid date range.'


class InvalidEmailException(Exception):
  """
  Should be raised for an invalid email address.
  """
  def __init__(self, *args):
    if args:
      self.message = args[0]
    else:
      self.message = None
  
  def __str__(self):
    if self.message:
      return f'InvalidEmailException, {self.message}'
    else:
      return 'InvalidEmailException: This is not a valid email address.'


class EmailException(Exception):
  """
  Should be raised when system fails to send an email.
  """
  def __init__(self, *args):
    if args:
      self.message = args[0]
    else:
      self.message = None
  
  def __str__(self):
    if self.message:
      return f'EmailException, {self.message}'
    else:
      return 'EmailException: Failed to send an email.'


class SESClientException(Exception):
  """
  Should be raised when system fails to setup SES client.
  """
  def __init__(self, *args):
    if args:
      self.message = args[0]
    else:
      self.message = None
  
  def __str__(self):
    if self.message:
      return f'SESException, {self.message}'
    else:
      return 'SESException: Failed to setup SES client.'
