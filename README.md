# User Profiling Tool #

Celery worker to profiling Twitter users.

## Installation
Install [virtualenv](https://virtualenv.pypa.io/en/latest/index.html)
Set a new environment:
```
virtualenv venv
```
Activate environment:
```
source ./venv/bin/activate
```
Clone this repo:
```
git clone git@bitbucket.org:citibeats/user-profiling-tool.git
```
Navigate to the project directory

Install dependencies:
```
pip install -r requirements.txt
```

## Loading the model
This application loads **GenderClassifier** files from the local storage. It requires a model and tokenizer to
initialize the model. Please set the following environment variables before running the application:
1. ```GENDER_CLASSIFIER_MODEL_DIRECTORY```: The directory containing the model.
2. ```GENDER_CLASSIFIER_TOKENIZER_DIRECTORY```: The directory containing the tokenizer.

## Running unit tests
```
python -m unittest
```

## Running Celery
You can run this application as a celery worker.
### Setup
You need to follow these steps:
1. Install **celery** (already installed through requirements.txt, can be skipped).
2. Set ```CELERY_BROKER_ENDPOINT``` environment variable which is a url of *REDIS* or *rabbitmq* message brokers.
3. You're all set, execute the command below to run *celery worker* from the project's *root* directory:
```
celery -A tasks worker --loglevel=info
```
### Usage Examples
Execute by importing in any other script or from python *shell*.
```
from tasks import profile_users


profiler_users.delay(
  s3_key='s3_test_key',
  email='labs@citibeats.net',
)
```

## Dockerize
### Build
From project's root directory, run:
```
docker build --tag userprofiler .
```
### Run
From project's root directory, run:
```
docker-compose up
```
