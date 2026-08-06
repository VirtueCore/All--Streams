FROM python:3.11-slim

WORKDIR /app

# Copy the requirements file first to leverage Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright's Chromium browser and all required system dependencies
RUN playwright install --with-deps chromium

# Copy the rest of your application code
COPY . .

# Set environment variables matching your script's Config class
ENV HOST=0.0.0.0
ENV PORT=7000

# Run the FastAPI application using Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7000"]
