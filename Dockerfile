FROM python:3.7-slim-buster

WORKDIR /usr/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
