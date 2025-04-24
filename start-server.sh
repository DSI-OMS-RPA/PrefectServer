#!/bin/bash
# Load environment variables from .env file manually
# Instead of sourcing the entire file, we'll read specific variables

# Load the other variables normally
source /opt/PrefectServer/.env

# But for the database URL, we'll use the password directly
# This avoids issues with special characters being interpreted by the shell
PREFECT_SERVER_DATABASE_CONNECTION_URL='postgresql+asyncpg://postgres:P$23fg#98@localhost:5432/prefect'

# Activate virtual environment
source /opt/PrefectServer/.venv/bin/activate

# Start Prefect server
exec prefect server start --host ${PREFECT_SERVER_API_HOST} --port ${PREFECT_SERVER_API_PORT}