# Use the official Python image as a base
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python script
COPY bird_history.py ./

# Install cron utility
RUN apt-get update && apt-get install -y cron

# Copy the cron job definition file
COPY cronjob.txt /etc/cron.d/birdjob

# Give execution rights on the cron job file
RUN chmod 0644 /etc/cron.d/birdjob

# Apply the cron job
RUN crontab /etc/cron.d/birdjob

# Create a log file for cron
RUN touch /var/log/cron.log

# Start cron service and keep the container running
CMD ["sh", "-c", "cron && tail -f /var/log/cron.log"]
