# Use official Python 3.11 image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend and frontend
COPY backend/ ./backend/
COPY frontend/ ./frontend/

# Create data directory (for local sqlite backup/temp usage)
RUN mkdir -p /app/data

# Set environment variables
ENV PYTHONPATH=/app/backend
ENV PORT=3000
ENV HOST=0.0.0.0

# Expose port
EXPOSE 3000

# Start the server
CMD ["python", "backend/proxy_server.py"]
