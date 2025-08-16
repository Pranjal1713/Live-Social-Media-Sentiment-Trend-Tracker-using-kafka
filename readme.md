# Live Social Media Sentiment & Trend Tracker

## 1. Overview

This project is a real-time data pipeline designed to ingest, process, and visualize social media data to track public sentiment and identify emerging trends. It leverages a modern, scalable tech stack to provide actionable insights for marketing teams, analysts, and researchers.

The system is built around a core pipeline:

- **Data Ingestion:** A Python script simulates a live social media feed by producing messages to an **Apache Kafka** topic.
- **Data Processing:** An **Apache Spark Streaming** job consumes the data from Kafka in real-time, performs sentiment analysis, and extracts key information like hashtags and locations.
- **Data Visualization:** A **Streamlit** dashboard reads the processed data and presents it through a series of live-updating charts, metrics, and tables.

The entire infrastructure is containerized using **Docker** and **Docker Compose** for easy setup and consistent deployment across different environments.

## 2. Technology Stack

- **Containerization:** Docker & Docker Compose
- **Data Ingestion:** Apache Kafka
- **Data Processing:** Apache Spark Streaming
- **Dashboard:** Streamlit
- **Programming Language:** Python
- **Core Python Libraries:** PySpark, Kafka-Python, Pandas, VaderSentiment

## 3. Project Structure

*(Add your directory/file tree here as appropriate.)*

## 4. Prerequisites

Before you begin, ensure you have the following installed on your local machine:

- **Docker Desktop:** The core engine for running containers. Make sure it is running before you start.
- **Python 3.8+:** For running the producer and dashboard scripts locally.

## 5. Setup and Installation

Follow these steps in order from the root directory of the project (`social-media-sentiment-tracker/`).

### Step 1: Build and Start the Docker Environment

This command will build your custom Spark image (installing necessary Python libraries) and start all the services (Kafka, Zookeeper, Spark Master, Spark Worker) in the background.

> **Note:** This may take several minutes on the first run as it downloads base images and installs dependencies.

### Step 2: Create the Kafka Topic

The Kafka service is running, but you need to create a "topic" (a message channel) for the data.

### Step 3: Set Up a Local Python Environment

It is highly recommended to use a virtual environment to manage dependencies for the producer and dashboard.

### Step 4: Start the Kafka Producer

This script reads from `data/mock_social_media.json` and sends messages to the Kafka topic.

You should see `Sending message: ...` logs appearing in this terminal.

### Step 5: Submit the Spark Streaming Job

This command sends your Python script to the Spark cluster to begin processing the data stream.

This terminal will show the Spark job logs. Leave it running to monitor the job.

### Step 6: Launch the Streamlit Dashboard

Finally, start the web application to visualize the results.

This will provide a local URL (usually `http://localhost:8501`). Open it in your browser to see the live dashboard.

## 6. Troubleshooting and Common Commands

### Full Restart Checklist (The "Golden Rule")

If you change any code or encounter issues, the most reliable way to ensure a clean state is to perform a full restart.

**Shut down and remove all containers, volumes, and networks:**

*(Add your commands here)*

**Rebuild and restart the services:**

*(Add your commands here)*

**Follow Steps 2-6** from the installation guide (create topic, start producer, etc.).

### Checking Container Status

To see which containers are running and which have exited.

*(Add your commands here)*

### Viewing Logs

If a container fails to start or is behaving unexpectedly, its logs are the best place to find the error.

*(Add your commands here)*

### Common Errors

- **`container ... is not running`:** This means the container has crashed or exited. Use `docker logs <container_name>` to find out why.
- **`exec format error` in logs:** This is a line-ending issue. It happens if you create a script (like `entrypoint.sh`) on Windows and try to run it in a Linux container. The solution is to not use local scripts and rely on those built into the base Docker image.
- **`ModuleNotFoundError` in Spark logs:** The Python library is missing inside the Spark container. Ensure your `Dockerfile` is correctly installing libraries from `requirements.txt`.
- **Dashboard is empty but Spark job is running:** This usually means Spark is not writing output files. The most common cause is a micro-batching issue. Adding a `.trigger(processingTime='10 seconds')` to your `writeStream` query in the Spark job is the standard fix.

## 7. How to Contribute

Contributions are welcome! If you'd like to improve the project, please follow these steps:

1. **Fork** the repository.
2. Create a new **branch** for your feature or bug fix.
3. Make your changes and **commit** them with clear, descriptive messages.
4. Push your branch to your fork.
5. Create a **Pull Request** to the main repository's `main` branch.
