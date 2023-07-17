FROM python:3.9-slim-buster

WORKDIR /app

# COPY requirements.txt /app


COPY . /app/src/model
RUN pip install -r src/model/requirements.txt

CMD ["python", "-u", "src/model/main.py"]