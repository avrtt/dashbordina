FROM python:3.10-slim

WORKDIR /app

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker caching
COPY requirements.txt .

# Install specific numpy version before other packages
RUN pip install --no-cache-dir numpy==1.24.3 
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire app directory
COPY app/ /app/

# Make sure Python can find our module
ENV PYTHONPATH="/app:${PYTHONPATH}"

EXPOSE 8050

# This runs the frontend index file
CMD ["python", "-m", "frontend.index"] 