# Apache Hudi Transactions Demo

A comprehensive demonstration of Apache Hudi's capabilities for managing transactional data lakes with ACID properties, efficient upserts, and time travel queries.

## 🚀 Quick Start

```bash
# Clone and setup
git clone <repository-url>
cd hudi-transactions-demo

# Setup virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run complete demo
python run_demo.py
```

## 📋 What This Demo Shows

- **ACID Transactions**: Create and modify data with full consistency guarantees
- **Upsert Operations**: Efficiently update existing records and insert new ones
- **Time Travel**: Query data at different points in time
- **Performance Benefits**: Compare Hudi vs traditional approaches
- **Visual Analytics**: Generated charts and diagrams

## 🏗️ Architecture

![Hudi Architecture](images/hudi_architecture.png)

## 📊 Results

The demo processes sample transaction data and demonstrates:

1. **Initial Load**: 4 transactions from CSV → Hudi table
2. **Upsert**: Add 2 new records + update 1 existing record
3. **Query**: Retrieve all data with Hudi metadata
4. **Visualize**: Generate performance and data comparison charts

![Data Comparison](images/data_comparison.png)

## 🛠️ Manual Execution

If you prefer to run steps individually:

```bash
# Activate environment
source venv/bin/activate

# Step 1: Create table
python spark/create_hudi_table.py

# Step 2: Upsert data
python spark/upsert_transactions.py

# Step 3: Query table
python spark/query_hudi_table.py

# Step 4: Generate visualizations
python visualize_hudi_demo.py
```

## 📁 Project Structure

```
├── data/
│   └── transactions_sample.csv    # Sample transaction data
├── spark/
│   ├── create_hudi_table.py      # Table creation script
│   ├── upsert_transactions.py    # Upsert operations
│   ├── query_hudi_table.py       # Query operations
│   └── clean_old_versions.py     # Cleanup operations
├── images/                       # Generated visualizations
├── warehouse/                    # Hudi table storage (auto-created)
├── run_demo.py                   # Complete demo runner
├── visualize_hudi_demo.py        # Visualization generator
├── requirements.txt              # Python dependencies
├── Playbook.md                   # Detailed playbook
└── README.md                     # This file
```

## 🔧 Requirements

- Python 3.8+
- Java 8+ (for Spark)
- 4GB+ RAM recommended

## 📈 Performance Benefits

![Performance Comparison](images/performance_comparison.png)

Apache Hudi provides significant improvements for:
- **Upsert Operations**: 80-90% faster than traditional approaches
- **Time Travel Queries**: Near-instant historical data access
- **Incremental Processing**: Only processes changed data

## 🎯 Key Features Demonstrated

- ✅ **ACID Transactions**: Data consistency and reliability
- ✅ **Schema Evolution**: Handle schema changes gracefully  
- ✅ **Time Travel**: Query historical data snapshots
- ✅ **Incremental Updates**: Efficient change processing
- ✅ **Compaction**: Storage optimization
- ✅ **Metadata Management**: Rich table metadata

## 🔍 Advanced Usage

### Time Travel Queries
```python
# Query specific timestamp
df = spark.read.format("hudi") \
    .option("as.of.instant", "20251114120251137") \
    .load("warehouse/hudi_transactions")
```

### Incremental Queries  
```python
# Read only changes since checkpoint
df = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", "20251114120251137") \
    .load("warehouse/hudi_transactions")
```

## 🐛 Troubleshooting

### Common Issues
- **Java Path**: Ensure JAVA_HOME is set correctly
- **Memory**: Increase JVM heap size for large datasets  
- **Permissions**: Check write permissions for warehouse directory

### Logs & Monitoring
- Spark logs: Check console output for execution details
- Hudi metadata: Inspect `warehouse/hudi_transactions/.hoodie/`
- Performance: Monitor Spark UI during execution

## 🚀 Next Steps

- Deploy on distributed Spark cluster
- Integrate with streaming data sources (Kafka)
- Implement automated compaction schedules
- Connect to data catalogs (Hive, AWS Glue)
- Explore different storage backends (S3, HDFS)

## 📚 Resources

- [Apache Hudi Documentation](https://hudi.apache.org/)
- [Hudi Spark Guide](https://hudi.apache.org/docs/quick-start-guide)
- [Performance Tuning](https://hudi.apache.org/docs/tuning-guide)

## 📄 License

This demo is provided as-is for educational purposes. See individual component licenses for production use.