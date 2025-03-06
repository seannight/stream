@echo off
echo Starting environment setup...

REM 安装Python依赖
python scripts/setup.py
if %ERRORLEVEL% NEQ 0 (
    echo Failed to install dependencies
    exit /b 1
)

REM 启动Docker环境
docker-compose up -d
if %ERRORLEVEL% NEQ 0 (
    echo Failed to start Docker environment
    exit /b 1
)

REM 等待服务启动
echo Waiting for services to start...
timeout /t 15

REM 测试Kafka连接
python scripts/test_kafka_connection.py
if %ERRORLEVEL% NEQ 0 (
    echo Kafka connection test failed
    exit /b 1
)

echo Environment setup completed successfully! 