#!/usr/bin/env python3
"""
Problem 1: Log Level Distribution
---------------------------------
This script analyzes Spark cluster log files stored in S3 and computes
the distribution of log levels (INFO, WARN, ERROR, DEBUG).

Outputs (all saved under data/output/):
1. problem1_counts.csv   — counts per log level
2. problem1_sample.csv   — 10 random sample log entries
3. problem1_summary.txt  — summary statistics
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, count, rand

# ------------------------------------------------------------
# Argument parsing
# ------------------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("master_url", help="Spark master URL (e.g., spark://10.0.0.1:7077)")
parser.add_argument("--net-id", required=True, help="Your NetID for S3 path")
args = parser.parse_args()

master_url = args.master_url
netid = args.net_id

# ------------------------------------------------------------
# Spark Session
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("Problem1_Log_Level_Distribution") \
    .master(master_url) \
    .getOrCreate()

# ------------------------------------------------------------
# Load log files from your personal S3 bucket
# ------------------------------------------------------------
s3_path = f"s3a://{netid}-assignment-spark-cluster-logs/data/"
logs_df = spark.read.text(s3_path)

# ------------------------------------------------------------
# Extract log levels using regex
# ------------------------------------------------------------
parsed_df = logs_df.select(
    regexp_extract("value", r"(INFO|WARN|ERROR|DEBUG)", 1).alias("log_level"),
    col("value").alias("log_entry")
).filter(col("log_level") != "")

# ------------------------------------------------------------
# Compute counts by log level
# ------------------------------------------------------------
counts_df = parsed_df.groupBy("log_level").agg(count("*").alias("count")).orderBy(col("count").desc())

# ------------------------------------------------------------
# Take a 10-row random sample
# ------------------------------------------------------------
sample_df = parsed_df.orderBy(rand()).limit(10)

# ------------------------------------------------------------
# Collect summary statistics
# ------------------------------------------------------------
total_lines = logs_df.count()
lines_with_levels = parsed_df.count()
unique_levels = counts_df.count()

summary = [
    f"Total log lines processed: {total_lines}",
    f"Total lines with log levels: {lines_with_levels}",
    f"Unique log levels found: {unique_levels}",
    "",
    "Log level distribution:"
]

for row in counts_df.collect():
    pct = (row['count'] / lines_with_levels) * 100
    summary.append(f"  {row['log_level']:6s}: {row['count']:>10,d} ({pct:5.2f}%)")

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
output_dir = "data/output/"
counts_df.coalesce(1).write.csv(output_dir + "problem1_counts.csv", header=True, mode="overwrite")
sample_df.coalesce(1).write.csv(output_dir + "problem1_sample.csv", header=True, mode="overwrite")

with open(output_dir + "problem1_summary.txt", "w") as f:
    f.write("\n".join(summary))

spark.stop()
print("[SUCCESS] Problem 1 completed successfully.")
