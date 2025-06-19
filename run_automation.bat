@echo off
REM Job Recommendation System Automation Script for Windows
REM This script:
REM 1. Runs Kafka producer continuously in background
REM 2. Runs Spark job every 10 minutes
REM 3. Monitors both processes

setlocal enabledelayedexpansion

REM Configuration
set PRODUCER_SCRIPT=kafka\producer.py
set SPARK_SCRIPT=spark.py
set LOG_DIR=logs
set AUTOMATION_LOG=%LOG_DIR%\automation.log
set PRODUCER_PID_FILE=%LOG_DIR%\producer.pid
set SPARK_INTERVAL=600

REM Colors for output (Windows)
set GREEN=[92m
set RED=[91m
set YELLOW=[93m
set BLUE=[94m
set NC=[0m

REM Ensure log directory exists
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Initialize log file
echo [%date% %time%] [INFO] Automation script started >> "%AUTOMATION_LOG%"

echo %GREEN%=========================================%NC%
echo %GREEN%Job Recommendation System - Automation%NC%
echo %GREEN%=========================================%NC%

:check_dependencies
echo [INFO] Checking dependencies...

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo %RED%[ERROR] Python is not installed or not in PATH%NC%
    pause
    exit /b 1
)

REM Check required scripts
if not exist "%SPARK_SCRIPT%" (
    echo %RED%[ERROR] Spark script not found: %SPARK_SCRIPT%%NC%
    pause
    exit /b 1
)

if not exist "%PRODUCER_SCRIPT%" (
    echo %YELLOW%[WARN] Producer script not found: %PRODUCER_SCRIPT%%NC%
    echo %YELLOW%[WARN] Producer monitoring will be disabled%NC%
)

echo [INFO] Dependencies check completed

:start_producer
echo [INFO] Starting Kafka producer...
echo [%date% %time%] [INFO] Starting Kafka producer >> "%AUTOMATION_LOG%"

REM Kill any existing Python processes running producer
taskkill /f /im python.exe /fi "windowtitle eq producer*" >nul 2>&1

REM Start producer in background
start "producer" /min python "%PRODUCER_SCRIPT%"

REM Wait a moment for producer to start
timeout /t 2 /nobreak >nul

echo %GREEN%[INFO] Producer started successfully%NC%
echo [%date% %time%] [INFO] Producer started successfully >> "%AUTOMATION_LOG%"

:main_loop
set /a spark_executions=0
set /a last_spark_time=0
call :get_current_time start_time

echo [INFO] Automation started. Producer running, Spark will run every 10 minutes.
echo [INFO] Press Ctrl+C to stop.

:monitor_loop
call :get_current_time current_time
set /a time_diff=!current_time! - !last_spark_time!

REM Check if it's time for Spark job (every 10 minutes = 600 seconds)
if !time_diff! geq %SPARK_INTERVAL% (
    call :run_spark
    set /a spark_executions+=1
    set last_spark_time=!current_time!
    
    REM Log statistics
    set /a uptime=!current_time! - !start_time!
    set /a uptime_minutes=!uptime! / 60
    echo [INFO] Statistics: Uptime: !uptime_minutes! minutes, Spark executions: !spark_executions!
    echo [%date% %time%] [INFO] Statistics: Uptime: !uptime_minutes! minutes, Spark executions: !spark_executions! >> "%AUTOMATION_LOG%"
)

REM Monitor producer every 30 seconds
set /a monitor_check=!current_time! %% 30
if !monitor_check! equ 0 (
    call :monitor_producer
)

REM Sleep for 5 seconds
timeout /t 5 /nobreak >nul
goto monitor_loop

:run_spark
echo %BLUE%[INFO] Starting Spark job execution...%NC%
echo [%date% %time%] [INFO] Starting Spark job execution >> "%AUTOMATION_LOG%"

if not exist "%SPARK_SCRIPT%" (
    echo %RED%[ERROR] Spark script not found: %SPARK_SCRIPT%%NC%
    echo [%date% %time%] [ERROR] Spark script not found: %SPARK_SCRIPT% >> "%AUTOMATION_LOG%"
    goto :eof
)

call :get_current_time spark_start_time

REM Run Spark job
python "%SPARK_SCRIPT%" >> "%LOG_DIR%\spark.log" 2>&1

call :get_current_time spark_end_time
set /a spark_duration=!spark_end_time! - !spark_start_time!

if errorlevel 1 (
    echo %RED%[ERROR] Spark job failed%NC%
    echo [%date% %time%] [ERROR] Spark job failed >> "%AUTOMATION_LOG%"
) else (
    echo %GREEN%[INFO] Spark job completed successfully in !spark_duration! seconds%NC%
    echo [%date% %time%] [INFO] Spark job completed successfully in !spark_duration! seconds >> "%AUTOMATION_LOG%"
)
goto :eof

:monitor_producer
REM Check if producer is still running
tasklist /fi "imagename eq python.exe" /fi "windowtitle eq producer*" 2>nul | find /i "python.exe" >nul
if errorlevel 1 (
    echo %YELLOW%[WARN] Producer is not running, attempting to restart...%NC%
    echo [%date% %time%] [WARN] Producer is not running, attempting to restart >> "%AUTOMATION_LOG%"
    call :start_producer
) else (
    echo [DEBUG] Producer is running
)
goto :eof

:get_current_time
REM Get current time in seconds (approximation)
for /f "tokens=1-4 delims=:.," %%a in ("%time%") do (
    set /a "%~1=(((%%a*60)+1%%b %% 100)*60+1%%c %% 100)*60+1%%d %% 100"
)
goto :eof

:cleanup
echo [INFO] Cleaning up...
echo [%date% %time%] [INFO] Cleaning up >> "%AUTOMATION_LOG%"

REM Kill producer
taskkill /f /im python.exe /fi "windowtitle eq producer*" >nul 2>&1

echo [INFO] Automation stopped
echo [%date% %time%] [INFO] Automation stopped >> "%AUTOMATION_LOG%"
exit /b 0

REM Handle Ctrl+C
:handle_ctrl_c
call :cleanup
exit /b 0
