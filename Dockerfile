FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Europe/Oslo
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY flatten.py resolve.py state.py cdc.py entrypoint.py ./

ENTRYPOINT ["python", "entrypoint.py"]
