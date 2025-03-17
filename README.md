# Prefect 3.x Server Setup

This README provides instructions for setting up a Prefect 3.x server on a Windows development environment using a dedicated virtual environment and a PostgreSQL database running in Docker.

## Prerequisites

- Windows 10/11
- Python 3.9+ installed
- uv package manager installed
- Docker Desktop running with PostgreSQL container

## Directory Structure

```
D:\Prefect\
├── .venv\          # Virtual environment for Prefect server
├── server\              # Server configuration
│   ├── .env             # Environment variables
│   └── start-server.bat # Script to start the server
└── profiles\            # Prefect profiles directory (created automatically)
```

## Step 1: Create Virtual Environment

```powershell
# Create the main directory
mkdir -p D:\1.PROJECTS\CVTelecom\PrefectServer

# Create and activate virtual environment
cd D:\Prefect
uv venv .venv
.\.venv\Scripts\activate
```

## Step 2: Install Prefect 3.x

```powershell
# Make sure you're in the activated environment
uv pip install prefect==3.0.0
```

## Step 3: Configure PostgreSQL Connection

Create a `.env` file in the server directory:

```
# D:\1.PROJECTS\CVTelecom\PrefectServer\.env
PREFECT_SERVER_DATABASE_CONNECTION_URL=postgresql+asyncpg://postgres:Pass123@postgres_db:5432/prefect
```

If your PostgreSQL isn't exposed directly, you might need to specify your Docker network settings or ensure host file mapping is configured to resolve `postgres_db`.

## Step 4: Create a Startup Script

Create a `start-server.bat` file in the server directory:

```batch
@echo off
REM D:\1.PROJECTS\CVTelecom\PrefectServer\start-server.bat
set PREFECT_HOME=D:\1.PROJECTS\CVTelecom\PrefectServer
set PREFECT_SERVER_DATABASE_CONNECTION_URL=postgresql+asyncpg://postgres:Pass123@postgres_db:5432/prefect

echo Starting Prefect 3.x server...
prefect server start
```

## Step 5: Initialize the Database

Before starting the server, initialize the PostgreSQL database:

```powershell
# Activate the server environment
cd D:\Prefect
.\.venv\Scripts\activate

# Initialize the database
prefect server database init
```

## Step 6: Start the Prefect Server

```powershell
# Navigate to server directory
cd D:\1.PROJECTS\CVTelecom\PrefectServer

# Run the startup script
.\start-server.bat
```

The Prefect UI should now be accessible at http://127.0.0.1:4200

## Step 7: Create a Default Profile

Create a profile to connect to your Prefect server:

```powershell
# Activate the server environment
cd D:\Prefect
.\.venv\Scripts\activate

# Create a default profile
prefect profile create default
prefect profile use default
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

## Maintenance

### Updating Prefect

To update Prefect to the latest version:

```powershell
cd D:\Prefect
.\.venv\Scripts\activate
uv pip install --upgrade prefect
```

### Database Management

You can manage the database schema using the CLI:

```powershell
# Check database status
prefect server database status

# Run migrations
prefect server database upgrade
```

### Environment Configuration

Prefect 3.x uses a hierarchical configuration system. You can set additional environment variables in the `.env` file or in the startup script.

Common configuration options include:

```
PREFECT_HOME=D:\1.PROJECTS\CVTelecom\PrefectServer
PREFECT_SERVER_DATABASE_CONNECTION_URL=postgresql+asyncpg://postgres:Pass123@postgres_db:5432/prefect
PREFECT_UI_URL=http://127.0.0.1:4200
PREFECT_SERVER_API_HOST=0.0.0.0
PREFECT_SERVER_API_PORT=4200
PREFECT_LOGGING_LEVEL=INFO
```

## Troubleshooting

### Database Connection Issues

If you're having trouble connecting to the PostgreSQL database:

1. Ensure PostgreSQL is running:
   ```powershell
   docker ps
   ```

2. Test the connection directly:
   ```powershell
   docker exec -it postgres_db psql -U postgres -c "SELECT 1"
   ```

3. Check that your connection string is correct:
   - For Docker container name resolution, ensure networking is configured correctly
   - Try using IP address instead of hostname if name resolution fails

### Server Won't Start

If the server won't start:

1. Check the logs for errors:
   ```powershell
   prefect server start --verbose
   ```

2. Verify that the PostgreSQL database exists:
   ```powershell
   docker exec -it postgres_db psql -U postgres -c "SELECT datname FROM pg_database"
   ```

3. Ensure the `prefect` database is created:
   ```powershell
   docker exec -it postgres_db psql -U postgres -c "CREATE DATABASE prefect"
   ```

## Resources

- [Prefect 3.x Documentation](https://docs.prefect.io/)
- [Prefect GitHub Repository](https://github.com/PrefectHQ/prefect)
- [Prefect Community Slack](https://prefect-community.slack.com/)
- [Prefect Discourse Forum](https://discourse.prefect.io/)