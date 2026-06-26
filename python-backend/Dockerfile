# ==========================================
# Python Temporal backend — runs as either the API server or the Worker.
# The docker-compose.yml chooses which by overriding the command at startup.
# ==========================================
FROM python:3.12-slim
WORKDIR /app

# Install dependencies first so the layer is cached across code-only changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application package.
COPY oms ./oms

# No ENTRYPOINT/CMD on purpose: docker-compose decides whether this image boots
# as the API server (oms.api) or the Worker (oms.worker).
