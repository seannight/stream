@echo off
echo Starting Kafka environment...

docker-compose -p kafka-env up -d

if %errorlevel% neq 0 (
    echo Failed to start Docker environment
    pause
    exit /b %errorlevel%
)

echo Kafka environment started successfully!
pause 