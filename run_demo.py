#!/usr/bin/env python3
"""
Apache Hudi Demo Runner
Executes the complete Hudi demo workflow with progress tracking
"""

import subprocess
import sys
import time
from datetime import datetime

def run_command(command, description):
    """Run a command and track its execution"""
    print(f"\n{'='*60}")
    print(f"🚀 {description}")
    print(f"{'='*60}")
    print(f"Command: {command}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    start_time = time.time()
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        end_time = time.time()
        duration = end_time - start_time
        
        if result.returncode == 0:
            print(f"✅ SUCCESS - Completed in {duration:.2f} seconds")
            if result.stdout:
                print(f"\nOutput:\n{result.stdout}")
        else:
            print(f"❌ FAILED - Exit code: {result.returncode}")
            if result.stderr:
                print(f"\nError:\n{result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ EXCEPTION: {str(e)}")
        return False
    
    return True

def main():
    """Run the complete Hudi demo"""
    print("🎯 Apache Hudi Transactions Demo")
    print("=" * 60)
    
    # Check if virtual environment is activated
    if sys.prefix == sys.base_prefix:
        print("⚠️  Warning: Virtual environment not detected")
        print("Please run: source venv/bin/activate")
        return
    
    # Demo steps
    steps = [
        ("python spark/create_hudi_table.py", "Step 1: Creating Hudi Table"),
        ("python spark/upsert_transactions.py", "Step 2: Upserting New Data"),
        ("python spark/query_hudi_table.py", "Step 3: Querying Hudi Table"),
        ("python visualize_hudi_demo.py", "Step 4: Generating Visualizations")
    ]
    
    success_count = 0
    total_start = time.time()
    
    for command, description in steps:
        if run_command(command, description):
            success_count += 1
        else:
            print(f"\n❌ Demo stopped due to failure in: {description}")
            break
    
    total_duration = time.time() - total_start
    
    print(f"\n{'='*60}")
    print(f"📊 DEMO SUMMARY")
    print(f"{'='*60}")
    print(f"✅ Successful steps: {success_count}/{len(steps)}")
    print(f"⏱️  Total duration: {total_duration:.2f} seconds")
    
    if success_count == len(steps):
        print(f"🎉 Demo completed successfully!")
        print(f"\n📁 Check the following outputs:")
        print(f"   • warehouse/hudi_transactions/ - Hudi table data")
        print(f"   • images/ - Generated visualizations")
        print(f"   • Playbook.md - Updated documentation")
    else:
        print(f"⚠️  Demo completed with {len(steps) - success_count} failures")

if __name__ == "__main__":
    main()