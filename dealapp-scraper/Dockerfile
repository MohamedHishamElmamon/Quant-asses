FROM mcr.microsoft.com/playwright/python:v1.40.0-focal

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create directories and non-root user
RUN useradd -m -u 1000 scraper && \
    mkdir -p /app/data /app/logs && \
    chown -R scraper:scraper /app

# Copy application code
COPY --chown=scraper:scraper . .

# Switch to non-root user
USER scraper

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV HEADLESS=true
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Run the scraper
CMD ["python", "main.py"]