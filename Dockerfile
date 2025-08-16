# Dockerfile
# Start from the full spark-master image, which already contains the correct entrypoint script
FROM bde2020/spark-master:3.1.1-hadoop3.2

# Switch to the root user to install system packages
USER root

# The bde2020 images are based on Alpine Linux, so use 'apk'
# Install build tools and Python 3 development headers
RUN apk update && apk add --no-cache gcc g++ python3-dev

# Upgrade pip and install the required Python libraries
COPY requirements.txt /
RUN pip3 install --upgrade pip
RUN pip3 install -r /requirements.txt