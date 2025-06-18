#!/usr/bin/env python3
"""
Setup script for Job Recommendation System
"""

import os
import subprocess
import sys

def install_requirements():
    """Install required Python packages"""
    print("Installing required packages...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✓ Requirements installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Error installing requirements: {e}")
        return False

def check_spark_installation():
    """Check if Spark is properly installed"""
    try:
        import pyspark
        print(f"✓ PySpark found (version: {pyspark.__version__})")
        return True
    except ImportError:
        print("✗ PySpark not found. Please install it using 'pip install pyspark'")
        return False

def check_java_installation():
    """Check if Java is installed (required for Spark)"""
    try:
        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        if result.returncode == 0:
            print("✓ Java is installed")
            return True
        else:
            print("✗ Java not found. Please install Java 8 or higher")
            return False
    except FileNotFoundError:
        print("✗ Java not found. Please install Java 8 or higher")
        return False

def create_directories():
    """Create necessary directories"""
    directories = ["models", "logs", "output"]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✓ Created directory: {directory}")

def check_dataset():
    """Check if dataset exists"""
    dataset_path = "kafka/dataset.csv"
    if os.path.exists(dataset_path):
        file_size = os.path.getsize(dataset_path) / (1024 * 1024)  # MB
        print(f"✓ Dataset found: {dataset_path} ({file_size:.1f} MB)")
        return True
    else:
        print(f"✗ Dataset not found: {dataset_path}")
        print("Please ensure the dataset is available at the correct location.")
        return False

def main():
    """Main setup function"""
    print("=" * 60)
    print("Job Recommendation System - Setup")
    print("=" * 60)
    
    success = True
    
    # Check Java installation
    if not check_java_installation():
        success = False
    
    # Install requirements
    if not install_requirements():
        success = False
    
    # Check Spark installation
    if not check_spark_installation():
        success = False
    
    # Create directories
    create_directories()
    
    # Check dataset
    if not check_dataset():
        print("\nWarning: Dataset not found. Some functions may not work.")
    
    print("\n" + "=" * 60)
    if success:
        print("✓ Setup completed successfully!")
        print("\nUsage:")
        print("1. Train model: python spark.py")
        print("2. Quick demo: python demo.py")
        print("3. Interactive demo: python demo.py --interactive")
    else:
        print("✗ Setup completed with errors. Please fix the issues above.")
    print("=" * 60)

if __name__ == "__main__":
    main()
