# Use a recent Ubuntu LTS as a base image
FROM ubuntu:22.04

# Set environment variables to non-interactive to avoid prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary dependencies
# - build-essential: for compiling software
# - python3, python3-pip: for the Python web service
# - sudo: The installer may need it
# - wget: To download the installer
RUN apt-get update && apt-get install -y \
    build-essential \
    python3 \
    python3-pip \
    sudo \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set up a working directory
WORKDIR /app

# Copy the Python web service application files
COPY app.py .
COPY requirements.txt .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# --- Wolfram Engine Installation ---

# Define build arguments for version and (optional) download URL override
ARG WOLFRAM_VERSION=14.3
ARG WOLFRAM_DOWNLOAD_URL
ENV WOLFRAM_VERSION=${WOLFRAM_VERSION}

# Download the Wolfram Engine installer
# If WOLFRAM_DOWNLOAD_URL is not provided, construct one from WOLFRAM_VERSION
RUN wget -O /tmp/installer.sh "${WOLFRAM_DOWNLOAD_URL:-https://account.wolfram.com/dl/WolframEngine?version=$WOLFRAM_VERSION&platform=Linux&downloadManager=false&includesDocumentation=false}"

# Make the installer executable
RUN chmod +x /tmp/installer.sh

# Run the Wolfram Engine installer in non-interactive mode to a fixed location
RUN sudo /tmp/installer.sh -- -auto -silent -createdir=y -overwrite=y -execdir=/usr/local/bin -targetdir=/opt/Wolfram/WolframEngine/${WOLFRAM_VERSION}

# Provide the kernel path via ENV for the app
ENV WOLFRAM_KERNEL_PATH=/opt/Wolfram/WolframEngine/${WOLFRAM_VERSION}/Executables/WolframKernel

# Clean up the installer to save space
RUN rm /tmp/installer.sh

# Create a non-root user and set up home directory
RUN useradd --create-home --shell /bin/bash appuser
RUN chown -R appuser:appuser /app

# Switch to the non-root user and run the app from /app
USER appuser
WORKDIR /app
ENV PATH=/home/appuser/.local/bin:$PATH

# Expose the port the Flask app will run on
EXPOSE 5000

# Define the command to run the application
# This will start the Python web service that listens for computation requests
CMD ["python3", "app.py"]
