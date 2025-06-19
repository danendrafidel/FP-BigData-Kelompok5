#!/bin/bash

# Job Recommendation System Automation Script
# This script:
# 1. Runs Kafka producer continuously in background
# 2. Runs Spark job every 10 minutes
# 3. Monitors both processes
# 4. Logs everything

# Configuration
PRODUCER_SCRIPT="./kafka/producer.py"
SPARK_SCRIPT="./spark.py"
LOG_DIR="./logs"
AUTOMATION_LOG="$LOG_DIR/automation.log"
PRODUCER_PID_FILE="$LOG_DIR/producer.pid"
SPARK_INTERVAL=600  # 10 minutes in seconds

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Logging function
log() {
    local level=$1
    shift
    local message="$@"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a "$AUTOMATION_LOG"
}

# Colored logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $@"
    log "INFO" "$@"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $@"
    log "WARN" "$@"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $@"
    log "ERROR" "$@"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $@"
    log "DEBUG" "$@"
}

# Check if producer is running
is_producer_running() {
    if [ -f "$PRODUCER_PID_FILE" ]; then
        local pid=$(cat "$PRODUCER_PID_FILE")
        if ps -p "$pid" > /dev/null 2>&1; then
            return 0  # Producer is running
        else
            rm -f "$PRODUCER_PID_FILE"
            return 1  # Producer is not running
        fi
    else
        return 1  # No PID file
    fi
}

# Start Kafka producer
start_producer() {
    if is_producer_running; then
        log_info "Producer is already running"
        return 0
    fi

    log_info "Starting Kafka producer..."
    
    # Check if producer script exists
    if [ ! -f "$PRODUCER_SCRIPT" ]; then
        log_error "Producer script not found: $PRODUCER_SCRIPT"
        return 1
    fi

    # Start producer in background
    nohup python3 "$PRODUCER_SCRIPT" >> "$LOG_DIR/producer.log" 2>&1 &
    local producer_pid=$!
    
    # Save PID
    echo "$producer_pid" > "$PRODUCER_PID_FILE"
    
    # Wait a moment and check if it's still running
    sleep 2
    if ps -p "$producer_pid" > /dev/null 2>&1; then
        log_info "Producer started successfully with PID: $producer_pid"
        return 0
    else
        log_error "Producer failed to start"
        rm -f "$PRODUCER_PID_FILE"
        return 1
    fi
}

# Stop producer
stop_producer() {
    if [ -f "$PRODUCER_PID_FILE" ]; then
        local pid=$(cat "$PRODUCER_PID_FILE")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_info "Stopping producer (PID: $pid)..."
            kill "$pid"
            sleep 2
            
            # Force kill if still running
            if ps -p "$pid" > /dev/null 2>&1; then
                log_warn "Force killing producer..."
                kill -9 "$pid"
            fi
        fi
        rm -f "$PRODUCER_PID_FILE"
        log_info "Producer stopped"
    else
        log_info "Producer is not running"
    fi
}

# Run Spark job
run_spark() {
    log_info "Starting Spark job execution..."
    
    # Check if Spark script exists
    if [ ! -f "$SPARK_SCRIPT" ]; then
        log_error "Spark script not found: $SPARK_SCRIPT"
        return 1
    fi

    local start_time=$(date +%s)
    
    # Run Spark job with timeout (30 minutes)
    timeout 1800 python3 "$SPARK_SCRIPT" >> "$LOG_DIR/spark.log" 2>&1
    local exit_code=$?
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    if [ $exit_code -eq 0 ]; then
        log_info "Spark job completed successfully in ${duration} seconds"
    elif [ $exit_code -eq 124 ]; then
        log_error "Spark job timed out after 30 minutes"
    else
        log_error "Spark job failed with exit code: $exit_code"
    fi
    
    return $exit_code
}

# Monitor producer health
monitor_producer() {
    if ! is_producer_running; then
        log_warn "Producer is not running, attempting to restart..."
        start_producer
    else
        local pid=$(cat "$PRODUCER_PID_FILE")
        log_debug "Producer is running (PID: $pid)"
    fi
}

# Monitor system resources
monitor_system() {
    # Get system info using available commands
    if command -v free > /dev/null; then
        local memory_info=$(free -m | awk 'NR==2{printf "Memory: %.1f%% (%s/%s MB)", $3*100/$2, $3, $2}')
        log_debug "$memory_info"
    fi
    
    if command -v df > /dev/null; then
        local disk_info=$(df -h . | awk 'NR==2{printf "Disk: %s used (%s available)", $5, $4}')
        log_debug "$disk_info"
    fi
}

# Cleanup function
cleanup() {
    log_info "Cleaning up..."
    stop_producer
    log_info "Automation stopped"
    exit 0
}

# Main function
main() {
    log_info "=========================================="
    log_info "Job Recommendation System - Automation"
    log_info "=========================================="
    
    # Set up signal handlers
    trap cleanup SIGINT SIGTERM
    
    # Start producer
    start_producer
    
    # Initialize counters
    local spark_executions=0
    local last_spark_time=0
    local start_time=$(date +%s)
    
    log_info "Automation started. Producer running, Spark will run every 10 minutes."
    log_info "Press Ctrl+C to stop."
    
    # Main monitoring loop
    while true; do
        local current_time=$(date +%s)
        
        # Monitor producer every 30 seconds
        if [ $((current_time % 30)) -eq 0 ]; then
            monitor_producer
            monitor_system
        fi
        
        # Run Spark job every 10 minutes
        if [ $((current_time - last_spark_time)) -ge $SPARK_INTERVAL ]; then
            log_info "Time for Spark job execution..."
            run_spark
            spark_executions=$((spark_executions + 1))
            last_spark_time=$current_time
            
            # Log statistics
            local uptime=$((current_time - start_time))
            local uptime_minutes=$((uptime / 60))
            log_info "Statistics: Uptime: ${uptime_minutes} minutes, Spark executions: $spark_executions"
        fi
        
        # Sleep for 1 second
        sleep 1
    done
}

# Check dependencies
check_dependencies() {
    log_info "Checking dependencies..."
    
    # Check Python
    if ! command -v python3 > /dev/null; then
        log_error "Python3 is not installed or not in PATH"
        exit 1
    fi
    
    # Check required scripts
    if [ ! -f "$SPARK_SCRIPT" ]; then
        log_error "Spark script not found: $SPARK_SCRIPT"
        exit 1
    fi
    
    if [ ! -f "$PRODUCER_SCRIPT" ]; then
        log_warn "Producer script not found: $PRODUCER_SCRIPT"
        log_warn "Producer monitoring will be disabled"
    fi
    
    log_info "Dependencies check completed"
}

# Help function
show_help() {
    echo "Usage: $0 [OPTION]"
    echo "Job Recommendation System Automation Script"
    echo ""
    echo "Options:"
    echo "  start       Start the automation (default)"
    echo "  stop        Stop all processes"
    echo "  status      Show status of running processes"
    echo "  restart     Restart the automation"
    echo "  help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0              # Start automation"
    echo "  $0 start        # Start automation"
    echo "  $0 stop         # Stop all processes"
    echo "  $0 status       # Check status"
}

# Status function
show_status() {
    echo "Job Recommendation System Status:"
    echo "=================================="
    
    if is_producer_running; then
        local pid=$(cat "$PRODUCER_PID_FILE")
        echo -e "${GREEN}Producer: RUNNING${NC} (PID: $pid)"
    else
        echo -e "${RED}Producer: STOPPED${NC}"
    fi
    
    # Check if automation script is running
    local automation_pids=$(pgrep -f "run_automation.sh")
    if [ -n "$automation_pids" ]; then
        echo -e "${GREEN}Automation: RUNNING${NC} (PIDs: $automation_pids)"
    else
        echo -e "${RED}Automation: STOPPED${NC}"
    fi
    
    # Show recent log entries
    if [ -f "$AUTOMATION_LOG" ]; then
        echo ""
        echo "Recent log entries:"
        echo "==================="
        tail -5 "$AUTOMATION_LOG"
    fi
}

# Parse command line arguments
case "${1:-start}" in
    start)
        check_dependencies
        main
        ;;
    stop)
        stop_producer
        ;;
    status)
        show_status
        ;;
    restart)
        stop_producer
        sleep 2
        check_dependencies
        main
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo "Unknown option: $1"
        show_help
        exit 1
        ;;
esac
