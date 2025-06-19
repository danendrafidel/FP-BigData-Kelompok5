#!/usr/bin/env python3
"""
Simple Automation - Run producer once, Spark every 5 minutes
"""
import os
import sys
import subprocess
import logging
import signal
import schedule
import time

# Configuration  
SPARK_SCRIPT = "./spark.py"
PRODUCER_SCRIPT = "./kafka/producer.py"
SPARK_INTERVAL_MINUTES = 5

shutdown_requested = False

def setup_logging():
    os.makedirs("./logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("./logs/automation.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def run_producer_once():
    """Run producer once to send all data"""
    logger.info("Running Kafka producer (one-time data send)...")
    
    try:
        result = subprocess.run([sys.executable, PRODUCER_SCRIPT], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("✓ Producer completed successfully")
        else:
            logger.error(f"Producer failed: {result.stderr}")
    except Exception as e:
        logger.error(f"Error running producer: {e}")

def run_spark():
    """Run Spark job"""
    logger.info("Running Spark job...")
    
    try:
        result = subprocess.run([sys.executable, SPARK_SCRIPT], 
                              capture_output=True, text=True, timeout=1800)
        if result.returncode == 0:
            logger.info("✓ Spark job completed")
        else:
            logger.error(f"Spark failed: {result.stderr}")
    except Exception as e:
        logger.error(f"Error running Spark: {e}")

def main():
    global logger, shutdown_requested
    
    logger = setup_logging()
    logger.info("=== Simple Automation Started ===")
    
    # Run producer once at startup
    run_producer_once()
    
    # Schedule only Spark job
    schedule.every(SPARK_INTERVAL_MINUTES).minutes.do(run_spark)
    
    logger.info("Automation running. Only Spark will run periodically.")
    
    try:
        while not shutdown_requested:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested")

if __name__ == "__main__":
    main()