FROM python:3.9-slim-buster

WORKDIR /app

# COPY requirements.txt /app


COPY . /app/src/provider
RUN pip install -r src/provider/requirements.txt

CMD ["python", "-u", "src/provider/main.py"]