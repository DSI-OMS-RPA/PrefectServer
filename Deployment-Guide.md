# Prefect Server Configuration Guide

This guide provides instructions for setting up and maintaining the Prefect server environment for ETL projects.

## Installation Process

### Install Prefect Server

```bash
# Create a dedicated virtual environment
python -m venv prefect-env
source prefect-env/bin/activate

# Install Prefect
pip install prefect

# Configure database connection (assuming PostgreSQL is already set up)
prefect config set PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://prefect:your_secure_password@localhost:5432/prefect"
```

### Configure Server Settings

```bash
# Set API URL (replace with your actual server IP/hostname)
prefect config set PREFECT_API_URL="http://192.168.87.59:8507/api"

# Set logging level
prefect config set PREFECT_LOGGING_LEVEL="INFO"

# Configure server settings
prefect config set PREFECT_SERVER_API_HOST="0.0.0.0"  # Listen on all interfaces
prefect config set PREFECT_SERVER_API_PORT="8507"     # Custom port for API

# (Optional) Configure TLS for production
# prefect config set PREFECT_API_SSL_CERT_PATH="/path/to/cert.pem"
# prefect config set PREFECT_API_SSL_KEY_PATH="/path/to/key.pem"
```

## Server Deployment Options

### Option 1: Systemd Service (Recommended for Production)

Create a systemd service file:

```bash
sudo nano /etc/systemd/system/prefect-server.service
```

Add the following content:

```ini
[Unit]
Description=Prefect Server
After=network.target postgresql.service

[Service]
WorkingDirectory=/path/to/prefect
Environment="PATH=/path/to/prefect-env/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=/path/to/prefect-env/bin/prefect server start
Restart=always
RestartSec=5
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=prefect-server

[Install]
WantedBy=multi-user.target
```

Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable prefect-server
sudo systemctl start prefect-server
```

### Option 2: Docker Deployment

```bash
# Pull the official image
docker pull prefecthq/prefect:latest

# Run with PostgreSQL connection
docker run -d \
  --name prefect-server \
  -p 8507:8507 \
  -e PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://prefect:your_secure_password@db-host:5432/prefect" \
  -e PREFECT_API_URL="http://192.168.87.59:8507/api" \
  -e PREFECT_SERVER_API_HOST="0.0.0.0" \
  -e PREFECT_SERVER_API_PORT="8507" \
  prefecthq/prefect:latest prefect server start
```

## Worker Configuration

Workers need to be properly configured to connect to the server:

```bash
# Create worker environment
python -m venv worker-env
source worker-env/bin/activate

# Install Prefect and project dependencies
pip install prefect
pip install -e /path/to/etl-project

# Configure worker to use the server
prefect config set PREFECT_API_URL="http://192.168.87.59:8507/api"

# Create and start worker
prefect work-pool create --type process production-pool
prefect worker start --pool production-pool
```

Create a systemd service for the worker:

```bash
sudo nano /etc/systemd/system/prefect-worker.service
```

Add the following content:

```ini
[Unit]
Description=Prefect Worker
After=network.target prefect-server.service

[Service]
WorkingDirectory=/path/to/work/directory
Environment="PATH=/path/to/worker-env/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=/path/to/worker-env/bin/prefect worker start --pool production-pool
Restart=always
RestartSec=10s

[Install]
WantedBy=multi-user.target
```

Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable prefect-worker
sudo systemctl start prefect-worker
```

## Backup and Maintenance

### Log Rotation

Configure log rotation for Prefect logs:

```bash
sudo nano /etc/logrotate.d/prefect
```

Add the following:

```
/path/to/prefect/logs/*.log {
    daily
    missingok
    rotate 14
    compress
    delaycompress
    notifempty
    create 0640 prefect prefect
}
```

## Upgrading Prefect

Follow these steps for safe upgrades:

```bash
# Stop the server
sudo systemctl stop prefect-server

# Backup the database (assuming you have backup procedures in place)

# Activate virtual environment
source /path/to/prefect-env/bin/activate

# Upgrade Prefect
pip install -U prefect

# Restart the server
sudo systemctl start prefect-server

# Check logs for any issues
sudo journalctl -u prefect-server -f
```

## Troubleshooting Common Issues

### Server Won't Start

If the server fails to start:

1. Check system resources:
   ```bash
   free -m
   df -h
   ```

2. Review logs:
   ```bash
   sudo journalctl -u prefect-server -n 100
   ```

3. Verify database connection:
   ```bash
   source /path/to/prefect-env/bin/activate
   python -c "from sqlalchemy import create_engine; engine = create_engine('postgresql+psycopg2://prefect:your_password@localhost:5432/prefect'); print(engine.connect())"
   ```

### Worker Connection Issues

If workers can't connect to the server:

1. Check network connectivity:
   ```bash
   curl -v http://192.168.87.59:8507/api/health
   ```

2. Verify worker configuration:
   ```bash
   prefect config view PREFECT_API_URL
   ```

## Additional Resources

- [Official Prefect Documentation](https://docs.prefect.io/)