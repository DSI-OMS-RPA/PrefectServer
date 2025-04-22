# Prefect 3.x Production Server Deployment Guide for Ubuntu 24.04 LTS

This guide provides comprehensive instructions for deploying a production Prefect 3.x server on Ubuntu 24.04 LTS. It covers server setup, database configuration, environment preparation, systemd service configuration, and maintenance procedures.

## Prerequisites

- Ubuntu 24.04 LTS server with sudo access
- External PostgreSQL server already set up and accessible
- Python 3.9+ installed on the Ubuntu server
- Port 8506 available on the Ubuntu server
- Access to GitHub (for private repository access)

## Deployment Overview

1. Server preparation
2. Python environment setup with uv
3. Prefect installation and configuration
4. Database connection configuration
5. Systemd service setup for automatic startup
6. Firewall configuration
7. Testing the deployment
8. Connecting from ETL projects
9. Maintenance procedures

## 1. Server Preparation

### Update the System

```bash
sudo apt update && sudo apt upgrade -y
```

### Install Required Dependencies

```bash
sudo apt install -y python3-pip python3-venv git curl
```

### Install uv Package Manager

```bash
curl -sSf https://install.ultraviolet.dev | python3 -
```

### Create Directory Structure

```bash
# Create main directory
sudo mkdir -p /opt/prefect-server
sudo chown $(whoami):$(whoami) /opt/prefect-server

# Create subdirectories
mkdir -p /opt/prefect-server/{env,logs,config}
```

## 2. Python Environment Setup

### Create Virtual Environment using uv

```bash
cd /opt/prefect-server
uv venv env
```

### Activate Virtual Environment

```bash
source /opt/prefect-server/env/bin/activate
```

## 3. Prefect Installation and Configuration

### Install Prefect

```bash
uv pip install prefect==3.0.0 psycopg2-binary
```

### Set Environment Variables

Create a `.env` file in the config directory:

```bash
cat > /opt/prefect-server/config/.env << EOL
# Prefect server configuration
PREFECT_HOME=/opt/prefect-server
PREFECT_SERVER_API_HOST=0.0.0.0
PREFECT_SERVER_API_PORT=8506
PREFECT_UI_URL=http://your-server-ip:8506
PREFECT_LOGGING_LEVEL=INFO
PREFECT_API_URL=http://127.0.0.1:8506/api

# PostgreSQL configuration - Update these values
PREFECT_SERVER_DATABASE_CONNECTION_URL=postgresql+asyncpg://username:password@your-db-host:5432/prefect
EOL
```

Replace `your-server-ip`, `username`, `password`, and `your-db-host` with your actual server IP and database credentials.

### Create a Prefect Server Start Script

```bash
cat > /opt/prefect-server/start-server.sh << EOL
#!/bin/bash
# Load environment variables
set -a
source /opt/prefect-server/config/.env
set +a

# Activate virtual environment
source /opt/prefect-server/env/bin/activate

# Start Prefect server
exec prefect server start --host \${PREFECT_SERVER_API_HOST} --port \${PREFECT_SERVER_API_PORT}
EOL

chmod +x /opt/prefect-server/start-server.sh
```

## 4. Initialize the Database

```bash
# Make sure virtual environment is activated
source /opt/prefect-server/env/bin/activate

# Load environment variables
set -a
source /opt/prefect-server/config/.env
set +a

# Initialize the database
prefect server database init
```

## 5. Systemd Service Configuration

Create a systemd service file for automatic startup:

```bash
sudo tee /etc/systemd/system/prefect-server.service << EOL
[Unit]
Description=Prefect 3.x Server
After=network.target

[Service]
User=$(whoami)
WorkingDirectory=/opt/prefect-server
ExecStart=/opt/prefect-server/start-server.sh
Restart=always
RestartSec=5
StandardOutput=append:/opt/prefect-server/logs/prefect-server.log
StandardError=append:/opt/prefect-server/logs/prefect-server-error.log
Environment="PATH=/opt/prefect-server/env/bin:/usr/local/bin:/usr/bin:/bin"

# Environment variables will be loaded from the script

[Install]
WantedBy=multi-user.target
EOL
```

### Enable and Start the Service

```bash
sudo systemctl daemon-reload
sudo systemctl enable prefect-server
sudo systemctl start prefect-server
```

### Check Service Status

```bash
sudo systemctl status prefect-server
```

## 6. Firewall Configuration

If UFW firewall is enabled, open the port:

```bash
sudo ufw allow 8506/tcp
sudo ufw status
```

## 7. Testing the Deployment

### Check Server Logs

```bash
tail -f /opt/prefect-server/logs/prefect-server.log
```

### Test API Access

```bash
curl http://localhost:8506/api/health
```

### Access Web UI

Open a web browser and navigate to:
```
http://your-server-ip:8506
```

## 8. Connecting Your ETL Projects

### Create a Production Profile

For each ETL project, create a production profile:

```bash
prefect profile create etl-project-prod
prefect profile use etl-project-prod
prefect config set PREFECT_API_URL=http://your-server-ip:8506/api
```

Update your ETL project's `prefect.yaml` files to use the production profile as needed:

```yaml
# Example prefect.yaml section
profile:
  name: etl-project-prod
  # Other profile settings
```

### Setting Up GitHub Access with Personal Access Token

To enable CI/CD with your GitHub repository:

1. Configure Git on the server:
   ```bash
   git config --global user.name "Your Name"
   git config --global user.email "your-email@example.com"
   ```

2. Store your GitHub token credentials:
   ```bash
   git config --global credential.helper store
   # The following command will prompt for your GitHub username and token
   # Use your token as the password
   git clone https://github.com/yourusername/your-repo-name.git
   ```

3. Create a simple update script:
   ```bash
   cat > /opt/prefect-server/update-from-github.sh << EOL
   #!/bin/bash
   # Script to pull latest changes from GitHub
   
   cd /opt/prefect-server/your-repo-name
   git pull origin main  # or your branch name
   
   # Restart Prefect server to apply changes
   sudo systemctl restart prefect-server
   
   echo "Updated from GitHub and restarted Prefect server at \$(date)"
   EOL
   
   chmod +x /opt/prefect-server/update-from-github.sh
   ```

4. To make changes on the production server and push back:
   ```bash
   cd /opt/prefect-server/your-repo-name
   # Make your changes
   git add .
   git commit -m "Description of changes made on production"
   git push origin main  # or your branch name
   ```

## 9. Maintenance Procedures

### Updating Prefect

```bash
# Activate virtual environment
source /opt/prefect-server/env/bin/activate

# Update Prefect
uv pip install --upgrade prefect

# Restart service
sudo systemctl restart prefect-server
```

### Database Migrations

```bash
# Activate environment and load variables
source /opt/prefect-server/env/bin/activate
set -a
source /opt/prefect-server/config/.env
set +a

# Check database status
prefect server database status

# Apply migrations
prefect server database upgrade
```

### Backup Configuration

Regularly backup your configuration files:

```bash
# Create backup directory
mkdir -p ~/prefect-backups

# Backup configuration
cp /opt/prefect-server/config/.env ~/prefect-backups/prefect-env-$(date +%Y%m%d).bak
```

### Monitoring

Set up basic monitoring for the Prefect service:

```bash
# Check service status
sudo systemctl status prefect-server

# Check logs for errors
grep ERROR /opt/prefect-server/logs/prefect-server.log
```

### Service Rotation

Configure log rotation to prevent log files from growing too large:

```bash
sudo tee /etc/logrotate.d/prefect-server << EOL
/opt/prefect-server/logs/*.log {
    daily
    missingok
    rotate 7
    compress
    delaycompress
    notifempty
    create 0640 $(whoami) $(whoami)
    sharedscripts
    postrotate
        systemctl reload prefect-server.service > /dev/null 2>/dev/null || true
    endscript
}
EOL
```

## Troubleshooting

### Database Connection Issues

If you encounter database connection problems:

1. Verify the connection string:
   ```bash
   # Test PostgreSQL connection 
   PGPASSWORD=your_password psql -h your-db-host -U username -d prefect -c "SELECT 1"
   ```

2. Check if the database exists:
   ```bash
   PGPASSWORD=your_password psql -h your-db-host -U username -l
   ```

3. Create the database if needed:
   ```bash
   PGPASSWORD=your_password psql -h your-db-host -U username -c "CREATE DATABASE prefect"
   ```

### Service Won't Start

If the service fails to start:

1. Check systemd logs:
   ```bash
   sudo journalctl -u prefect-server.service -n 50
   ```

2. Test starting manually:
   ```bash
   source /opt/prefect-server/env/bin/activate
   set -a
   source /opt/prefect-server/config/.env
   set +a
   prefect server start --host 0.0.0.0 --port 8506 --verbose
   ```

3. Verify permissions:
   ```bash
   ls -la /opt/prefect-server
   sudo chown -R $(whoami):$(whoami) /opt/prefect-server
   ```

### Web UI Access Issues

If you can't access the web UI:

1. Check if the server is running:
   ```bash
   netstat -tulpn | grep 8506
   ```

2. Test local access:
   ```bash
   curl http://localhost:8506
   ```

3. Check firewall status:
   ```bash
   sudo ufw status
   ```

## Security Considerations

1. **SSL/TLS Configuration**: For production, consider setting up an NGINX reverse proxy with SSL:

```bash
sudo apt install -y nginx certbot python3-certbot-nginx

# Configure NGINX as reverse proxy with SSL
sudo tee /etc/nginx/sites-available/prefect-server << EOL
server {
    listen 443 ssl;
    server_name prefect.your-domain.com;

    ssl_certificate /etc/letsencrypt/live/prefect.your-domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/prefect.your-domain.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8506;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}

server {
    listen 80;
    server_name prefect.your-domain.com;
    return 301 https://\$host\$request_uri;
}
EOL

sudo ln -s /etc/nginx/sites-available/prefect-server /etc/nginx/sites-enabled/
sudo certbot --nginx -d prefect.your-domain.com
sudo systemctl restart nginx
```

1. **Database Security**: Ensure your PostgreSQL connection uses SSL and that the database is properly secured.

2. **Authentication**: Consider enabling authentication for your Prefect server.

## Migration from Development

If migrating ETL projects from development to this production server:

1. Export existing deployments:
   ```bash
   # On development server
   prefect deployment export --name "deployment-name" --file deployment-export.yaml
   ```

2. Import deployments to production:
   ```bash
   # On production server
   prefect deployment import --file deployment-export.yaml
   ```

## Resources

- [Prefect 3.x Documentation](https://docs.prefect.io/)
- [Prefect GitHub Repository](https://github.com/PrefectHQ/prefect)
- [Prefect Community Slack](https://prefect-community.slack.com/)
- [Prefect Discourse Forum](https://discourse.prefect.io/)