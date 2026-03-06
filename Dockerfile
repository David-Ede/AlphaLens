FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY src /app/src
COPY conf /app/conf
COPY tests/fixtures /app/tests/fixtures

RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir .

EXPOSE 8050

CMD ["fg", "run-dashboard"]
