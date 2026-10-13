FROM python:3.12-slim

RUN pip install --no-cache-dir 'kubernetes>=31,<35'

WORKDIR /app
COPY hermes-event-forwarder.py /app/forwarder.py

CMD ["python", "-u", "/app/forwarder.py"]