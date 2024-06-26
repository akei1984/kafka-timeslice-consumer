# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY kafka-consumer.py /app
COPY requirements.txt /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 5100 available to the world outside this container
EXPOSE 5100

# Define environment variable
ENV FLASK_APP=kafka-consumer.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5100

# Run the Flask app
CMD ["flask", "run"]