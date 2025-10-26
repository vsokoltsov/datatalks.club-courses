FROM agrigorev/zoomcamp-model:2025

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ curl && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --shell /bin/bash app && chown -R app:app /app

# Copy files and install uv
COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv
ENV PATH="/home/app/.local/bin:${PATH}" 

# Switch to app user and install dependencies
USER app
RUN uv sync --frozen

# Copy application code as app user
USER root
COPY app.py ./app.py
RUN chown -R app:app /app
USER app

EXPOSE 8000

CMD ["uv", "run", "python", "-m", "uvicorn", "app:api", "--host", "0.0.0.0", "--port", "8000"]