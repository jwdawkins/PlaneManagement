FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    slack-bolt==1.27.0 \
    python-dateutil==2.9.0.post0

COPY app/ .

CMD ["python3", "plane_bot.py"]
