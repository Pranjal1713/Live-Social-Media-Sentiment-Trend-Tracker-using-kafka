
FROM bde2020/spark-master:3.1.1-hadoop3.2

# Switch to the root user to install system packages
USER root

RUN apk update && apk add --no-cache gcc g++ python3-dev

# Upgrade pip and install the required Python libraries
COPY requirements.txt /
RUN pip3 install --upgrade pip
RUN pip3 install -r /requirements.txt