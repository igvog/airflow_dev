# Use official Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Expose port
EXPOSE 8000

# Command to run FastAPI with uvicorn
CMD ["uvicorn", "fastapi_post:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
