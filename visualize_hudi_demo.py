#!/usr/bin/env python3
"""
Hudi Transactions Demo Visualization
Creates visual diagrams and charts for the Apache Hudi playbook
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, Rectangle
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import seaborn as sns

# Set style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def create_architecture_diagram():
    """Create Hudi architecture diagram"""
    fig, ax = plt.subplots(1, 1, figsize=(14, 10))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 8)
    ax.axis('off')
    
    # Title
    ax.text(5, 7.5, 'Apache Hudi Architecture & Workflow', 
            fontsize=20, fontweight='bold', ha='center')
    
    # Data Source
    source_box = FancyBboxPatch((0.5, 6), 2, 0.8, 
                               boxstyle="round,pad=0.1", 
                               facecolor='lightblue', 
                               edgecolor='navy', linewidth=2)
    ax.add_patch(source_box)
    ax.text(1.5, 6.4, 'CSV Data\nSource', ha='center', va='center', fontweight='bold')
    
    # Spark Processing
    spark_box = FancyBboxPatch((4, 6), 2, 0.8, 
                              boxstyle="round,pad=0.1", 
                              facecolor='orange', 
                              edgecolor='darkorange', linewidth=2)
    ax.add_patch(spark_box)
    ax.text(5, 6.4, 'Apache Spark\nProcessing', ha='center', va='center', fontweight='bold')
    
    # Hudi Table
    hudi_box = FancyBboxPatch((7.5, 6), 2, 0.8, 
                             boxstyle="round,pad=0.1", 
                             facecolor='lightgreen', 
                             edgecolor='darkgreen', linewidth=2)
    ax.add_patch(hudi_box)
    ax.text(8.5, 6.4, 'Hudi Table\n(Parquet + Metadata)', ha='center', va='center', fontweight='bold')
    
    # Operations
    operations = [
        ('CREATE', 1, 4.5, 'lightcoral'),
        ('UPSERT', 3, 4.5, 'lightyellow'),
        ('QUERY', 5, 4.5, 'lightcyan'),
        ('CLEAN', 7, 4.5, 'lavender')
    ]
    
    for op, x, y, color in operations:
        op_box = FancyBboxPatch((x-0.5, y-0.3), 1, 0.6, 
                               boxstyle="round,pad=0.05", 
                               facecolor=color, 
                               edgecolor='black', linewidth=1)
        ax.add_patch(op_box)
        ax.text(x, y, op, ha='center', va='center', fontweight='bold', fontsize=10)
    
    # Arrows
    arrow_props = dict(arrowstyle='->', lw=2, color='black')
    ax.annotate('', xy=(3.8, 6.4), xytext=(2.7, 6.4), arrowprops=arrow_props)
    ax.annotate('', xy=(7.3, 6.4), xytext=(6.2, 6.4), arrowprops=arrow_props)
    
    # Features box
    features_box = FancyBboxPatch((1, 1.5), 8, 2, 
                                 boxstyle="round,pad=0.1", 
                                 facecolor='whitesmoke', 
                                 edgecolor='gray', linewidth=2)
    ax.add_patch(features_box)
    
    ax.text(5, 3, 'Apache Hudi Key Features', ha='center', fontweight='bold', fontsize=14)
    features_text = """
    • ACID Transactions: Ensures data consistency and reliability
    • Time Travel: Query data at different points in time
    • Incremental Processing: Efficient updates and deletes
    • Schema Evolution: Handle schema changes gracefully
    • Compaction: Optimize storage and query performance
    • Rollback: Revert to previous table states
    """
    ax.text(5, 2.2, features_text, ha='center', va='center', fontsize=11)
    
    plt.tight_layout()
    plt.savefig('images/hudi_architecture.png', dpi=300, bbox_inches='tight')
    plt.close()

def create_workflow_diagram():
    """Create step-by-step workflow diagram"""
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 8)
    ax.axis('off')
    
    # Title
    ax.text(5, 7.5, 'Hudi Demo Workflow Steps', 
            fontsize=18, fontweight='bold', ha='center')
    
    steps = [
        ('1. CREATE TABLE', 2, 6, 'lightblue', 'create_hudi_table.py'),
        ('2. UPSERT DATA', 8, 6, 'lightgreen', 'upsert_transactions.py'),
        ('3. QUERY TABLE', 2, 3.5, 'lightyellow', 'query_hudi_table.py'),
        ('4. CLEAN VERSIONS', 8, 3.5, 'lightcoral', 'clean_old_versions.py')
    ]
    
    for step, x, y, color, script in steps:
        # Step box
        step_box = FancyBboxPatch((x-1.5, y-0.5), 3, 1, 
                                 boxstyle="round,pad=0.1", 
                                 facecolor=color, 
                                 edgecolor='black', linewidth=2)
        ax.add_patch(step_box)
        ax.text(x, y+0.1, step, ha='center', va='center', fontweight='bold', fontsize=12)
        ax.text(x, y-0.2, script, ha='center', va='center', fontsize=9, style='italic')
    
    # Arrows
    arrow_props = dict(arrowstyle='->', lw=3, color='darkblue')
    ax.annotate('', xy=(6.5, 6), xytext=(3.5, 6), arrowprops=arrow_props)
    ax.annotate('', xy=(2, 5), xytext=(2, 4.5), arrowprops=arrow_props)
    ax.annotate('', xy=(6.5, 3.5), xytext=(3.5, 3.5), arrowprops=arrow_props)
    
    # Add descriptions
    descriptions = [
        (2, 1.5, "• Load CSV data\n• Create Hudi table\n• Set record key & precombine field"),
        (8, 1.5, "• Add new records\n• Update existing records\n• Maintain ACID properties"),
    ]
    
    for x, y, desc in descriptions:
        ax.text(x, y, desc, ha='center', va='center', fontsize=10, 
                bbox=dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    plt.savefig('images/hudi_workflow.png', dpi=300, bbox_inches='tight')
    plt.close()

def create_data_visualization():
    """Create data visualization showing before/after upsert"""
    # Sample data
    original_data = {
        'txn_id': [1, 2, 3, 4],
        'customer_id': [101, 102, 103, 101],
        'amount': [120.50, 310.20, 50.00, 200.00],
        'currency': ['USD', 'USD', 'USD', 'USD'],
        'timestamp': ['2025-11-01T09:00:00', '2025-11-01T09:05:00', 
                     '2025-11-01T09:10:00', '2025-11-01T09:15:00']
    }
    
    upserted_data = {
        'txn_id': [1, 2, 3, 4, 5],
        'customer_id': [101, 102, 103, 101, 106],
        'amount': [120.50, 310.20, 50.00, 250.00, 1200.00],  # txn_id 4 updated
        'currency': ['USD', 'USD', 'USD', 'USD', 'USD'],
        'timestamp': ['2025-11-01T09:00:00', '2025-11-01T09:05:00', 
                     '2025-11-01T09:10:00', '2025-11-14T12:03:16', '2025-11-14T12:03:16']
    }
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Original data
    df1 = pd.DataFrame(original_data)
    colors1 = ['lightblue'] * len(df1)
    bars1 = ax1.bar(df1['txn_id'], df1['amount'], color=colors1, edgecolor='navy')
    ax1.set_title('Original Data (After CREATE)', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Transaction ID')
    ax1.set_ylabel('Amount (USD)')
    ax1.grid(True, alpha=0.3)
    
    # Add value labels
    for bar in bars1:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 5,
                f'${height:.2f}', ha='center', va='bottom')
    
    # Upserted data
    df2 = pd.DataFrame(upserted_data)
    colors2 = ['lightblue', 'lightblue', 'lightblue', 'orange', 'lightgreen']  # Updated and new
    bars2 = ax2.bar(df2['txn_id'], df2['amount'], color=colors2, edgecolor='navy')
    ax2.set_title('Data After UPSERT', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Transaction ID')
    ax2.set_ylabel('Amount (USD)')
    ax2.grid(True, alpha=0.3)
    
    # Add value labels
    for bar in bars2:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 20,
                f'${height:.2f}', ha='center', va='bottom')
    
    # Legend
    legend_elements = [
        mpatches.Patch(color='lightblue', label='Original Records'),
        mpatches.Patch(color='orange', label='Updated Record'),
        mpatches.Patch(color='lightgreen', label='New Record')
    ]
    ax2.legend(handles=legend_elements, loc='upper left')
    
    plt.tight_layout()
    plt.savefig('images/data_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()

def create_performance_chart():
    """Create performance comparison chart"""
    fig, ax = plt.subplots(1, 1, figsize=(10, 6))
    
    # Sample performance data
    operations = ['Initial Load', 'Upsert 1K', 'Upsert 10K', 'Query All', 'Time Travel Query']
    traditional = [45, 120, 850, 15, 180]  # seconds
    hudi = [50, 25, 95, 12, 8]  # seconds
    
    x = np.arange(len(operations))
    width = 0.35
    
    bars1 = ax.bar(x - width/2, traditional, width, label='Traditional Approach', 
                   color='lightcoral', edgecolor='darkred')
    bars2 = ax.bar(x + width/2, hudi, width, label='Apache Hudi', 
                   color='lightgreen', edgecolor='darkgreen')
    
    ax.set_xlabel('Operations')
    ax.set_ylabel('Time (seconds)')
    ax.set_title('Performance Comparison: Traditional vs Apache Hudi', fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(operations, rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Add value labels
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 2,
                    f'{height}s', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    plt.savefig('images/performance_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()

def main():
    """Generate all visualization images"""
    import os
    
    # Create images directory
    os.makedirs('images', exist_ok=True)
    
    print("Generating Hudi demo visualizations...")
    
    create_architecture_diagram()
    print("✓ Architecture diagram created")
    
    create_workflow_diagram()
    print("✓ Workflow diagram created")
    
    create_data_visualization()
    print("✓ Data comparison chart created")
    
    create_performance_chart()
    print("✓ Performance comparison chart created")
    
    print("\nAll visualizations saved to 'images/' directory!")

if __name__ == "__main__":
    main()