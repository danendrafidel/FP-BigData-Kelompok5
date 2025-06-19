#!/usr/bin/env python3
"""
Real-time monitoring dashboard for Job Recommendation System
Run this in a separate terminal to monitor the automation
"""

import os
import time
import subprocess
import json
from datetime import datetime
import psutil

class AutomationMonitor:
    def __init__(self):
        self.log_file = "./logs/automation.log"
        self.producer_log = "./logs/producer.log"
        self.spark_log = "./logs/spark.log"
        
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def get_system_info(self):
        """Get current system information"""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('.')
            
            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_used_gb': memory.used / (1024**3),
                'memory_total_gb': memory.total / (1024**3),
                'disk_percent': disk.percent,
                'disk_used_gb': disk.used / (1024**3),
                'disk_total_gb': disk.total / (1024**3)
            }
        except Exception as e:
            return {'error': str(e)}
    
    def check_processes(self):
        """Check if processes are running"""
        processes = {
            'producer': False,
            'automation_script': False,
            'python_processes': 0
        }
        
        try:
            # Check for Python processes
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] and 'python' in proc.info['name'].lower():
                        processes['python_processes'] += 1
                        
                        if proc.info['cmdline']:
                            cmdline = ' '.join(proc.info['cmdline'])
                            if 'producer.py' in cmdline:
                                processes['producer'] = True
                            if 'run_automation' in cmdline:
                                processes['automation_script'] = True
                                
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
                    
        except Exception as e:
            processes['error'] = str(e)
        
        return processes
    
    def get_log_tail(self, log_file, lines=5):
        """Get last few lines from log file"""
        try:
            if not os.path.exists(log_file):
                return [f"Log file not found: {log_file}"]
                
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                all_lines = f.readlines()
                return [line.strip() for line in all_lines[-lines:] if line.strip()]
        except Exception as e:
            return [f"Error reading {log_file}: {e}"]
    
    def count_log_entries(self, log_file, search_term):
        """Count occurrences of search term in log file"""
        try:
            if not os.path.exists(log_file):
                return 0
                
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                return content.count(search_term)
        except Exception:
            return 0
    
    def display_dashboard(self):
        """Display the monitoring dashboard"""
        self.clear_screen()
        
        print("=" * 80)
        print("🚀 JOB RECOMMENDATION SYSTEM - MONITORING DASHBOARD")
        print("=" * 80)
        print(f"📅 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # System Information
        print("💻 SYSTEM RESOURCES")
        print("-" * 40)
        sys_info = self.get_system_info()
        if 'error' not in sys_info:
            print(f"🔴 CPU Usage:    {sys_info['cpu_percent']:.1f}%")
            print(f"🟡 Memory Usage: {sys_info['memory_percent']:.1f}% "
                  f"({sys_info['memory_used_gb']:.1f}GB / {sys_info['memory_total_gb']:.1f}GB)")
            print(f"🟢 Disk Usage:   {sys_info['disk_percent']:.1f}% "
                  f"({sys_info['disk_used_gb']:.1f}GB / {sys_info['disk_total_gb']:.1f}GB)")
        else:
            print(f"❌ Error: {sys_info['error']}")
        print()
        
        # Process Status
        print("⚙️  PROCESS STATUS")
        print("-" * 40)
        processes = self.check_processes()
        
        producer_status = "🟢 RUNNING" if processes['producer'] else "🔴 STOPPED"
        automation_status = "🟢 RUNNING" if processes['automation_script'] else "🔴 STOPPED"
        
        print(f"🏭 Producer:          {producer_status}")
        print(f"🤖 Automation Script: {automation_status}")
        print(f"🐍 Python Processes:  {processes['python_processes']}")
        print()
        
        # Log Statistics
        print("📊 LOG STATISTICS")
        print("-" * 40)
        spark_executions = self.count_log_entries(self.log_file, "Spark job completed")
        producer_starts = self.count_log_entries(self.log_file, "Producer started")
        errors = self.count_log_entries(self.log_file, "[ERROR]")
        warnings = self.count_log_entries(self.log_file, "[WARN]")
        
        print(f"✅ Spark Executions: {spark_executions}")
        print(f"🔄 Producer Starts:   {producer_starts}")
        print(f"⚠️  Warnings:         {warnings}")
        print(f"❌ Errors:           {errors}")
        print()
        
        # Recent Automation Logs
        print("📝 RECENT AUTOMATION LOGS")
        print("-" * 40)
        recent_logs = self.get_log_tail(self.log_file, 6)
        for log_line in recent_logs:
            # Color code based on log level
            if "[ERROR]" in log_line:
                print(f"❌ {log_line}")
            elif "[WARN]" in log_line:
                print(f"⚠️  {log_line}")
            elif "[INFO]" in log_line:
                print(f"ℹ️  {log_line}")
            else:
                print(f"   {log_line}")
        print()
        
        # Recent Spark Logs
        print("🔥 RECENT SPARK LOGS")
        print("-" * 40)
        spark_logs = self.get_log_tail(self.spark_log, 4)
        for log_line in spark_logs:
            if "[ERROR]" in log_line or "Error" in log_line:
                print(f"❌ {log_line}")
            elif "[OK]" in log_line or "successfully" in log_line:
                print(f"✅ {log_line}")
            else:
                print(f"   {log_line}")
        print()
        
        # Instructions
        print("🎮 CONTROLS")
        print("-" * 40)
        print("Press Ctrl+C to exit monitor")
        print("Monitor refreshes every 5 seconds")
        print()
        
    def run(self):
        """Run the monitoring dashboard"""
        print("Starting Job Recommendation System Monitor...")
        print("Press Ctrl+C to exit")
        time.sleep(2)
        
        try:
            while True:
                self.display_dashboard()
                time.sleep(5)  # Refresh every 5 seconds
                
        except KeyboardInterrupt:
            print("\n\n👋 Monitor stopped by user")
        except Exception as e:
            print(f"\n\n❌ Monitor error: {e}")

def main():
    monitor = AutomationMonitor()
    monitor.run()

if __name__ == "__main__":
    main()
