FROM python:3.9-slim-buster

WORKDIR /app

COPY . /app/src/api
RUN pip install -r src/api/requirements.txt


EXPOSE 5000
# ENV FLASK_APP=/app/src/api/main.py
# ENV FLASK_RUN_HOST=0.0.0.0

# ENTRYPOINT ["python", "-m", "flask", "run"]
# CMD ["--port", "5000"]

CMD ["python", "-u", "src/api/main.py"]