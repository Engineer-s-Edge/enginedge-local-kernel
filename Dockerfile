# Use the official Wolfram Engine image
FROM docker.io/wolframresearch/wolframengine:latest

# Switch to root to install dependencies
USER root

# Set environment variables to non-interactive to avoid prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary dependencies
# - build-essential: for compiling software
# - python3, python3-pip: for the Python web service
RUN apt-get update && apt-get install -y \
    build-essential \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Set up a working directory
WORKDIR /app

# Copy the Python web service application files
COPY app.py .
COPY requirements.txt .

# Install Python dependencies
# Use --break-system-packages to allow installing packages system-wide in this container
# This is necessary because newer Debian/Ubuntu versions enforce PEP 668
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

# Create a non-root user and set up home directory (if not already present)
# The base image might already have a user, but let's ensure we have our appuser
RUN useradd --create-home --shell /bin/bash appuser || true
RUN chown -R appuser:appuser /app

# Switch to the non-root user
USER appuser
WORKDIR /app
ENV PATH=/home/appuser/.local/bin:$PATH
ENV WOLFRAM_KERNEL_PATH=/usr/local/bin/WolframKernel

# Expose the port the Flask app will run on
EXPOSE 5000

# Define the command to run the application
CMD ["python3", "app.py"]
