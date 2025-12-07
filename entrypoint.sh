#!/bin/bash
set -e

# Create the airflow_home directory if it doesn't exist
mkdir -p /opt/airflow/airflow_home

# Run the database migration
airflow db migrate

# Create the admin user
airflow standalone


# Install Python dependencies
pip install -r /requirements.txt

echo "Airflow initialization complete."
