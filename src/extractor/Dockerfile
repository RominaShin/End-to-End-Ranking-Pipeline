FROM python:3.9-slim-buster

WORKDIR /app

# COPY requirements.txt /app


COPY . /app/src/extractor
RUN pip install -r src/extractor/requirements.txt

CMD ["python", "-u", "src/extractor/main.py"]