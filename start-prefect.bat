@echo off
REM D:\1.PROJECTS\CVTelecom\PrefectServer\start-server.bat
set PREFECT_HOME=D:\1.PROJECTS\CVTelecom\PrefectServer
set PREFECT_SERVER_DATABASE_CONNECTION_URL=postgresql+asyncpg://postgres:Pass123@localhost:5432/prefect

echo Starting Prefect 3.x server...
prefect server start