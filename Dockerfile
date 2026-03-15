FROM python:3.12-slim

WORKDIR /app

# Install HTTP server dependencies (includes MCP for StreamableHTTP)
COPY requirements-http.txt .
RUN pip install --no-cache-dir -r requirements-http.txt

# Copy application code and static schemas
COPY auth.py .
COPY http_server.py .
COPY schemas/ schemas/

EXPOSE 8010

CMD ["python", "http_server.py"]
