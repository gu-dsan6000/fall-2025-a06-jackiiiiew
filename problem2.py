#!/usr/bin/env python3
"""
Problem 2: Cluster Usage Analysis

Outputs (saved to data/output/):
1) problem2_timeline.csv
2) problem2_cluster_summary.csv
3) problem2_stats.txt
4) problem2_bar_chart.png
5) problem2_density_plot.png

Usage:
  uv run python problem2.py spark://<MASTER_PRIVATE_IP>:7077 --net-id <your_netid>
  uv run python problem2.py --skip-spark   # Only re-draw figures from existing CSVs
"""

import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# -------------------------------
# Utilities for plotting
# -------------------------------
def save_fig(path: str) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()

def regenerate_viz_from_csv(outdir: str) -> None:
    timeline_path = os.path.join(outdir, "problem2_timeline.csv")
    summary_path = os.path.join(outdir, "problem2_cluster_summary.csv")

    if not (os.path.exists(timeline_path) and os.path.exists(summary_path)):
        raise SystemExit("CSV files not found. Run Spark mode first or ensure CSVs exist.")

    timeline = pd.read_csv(timeline_path)
    cluster_summary = pd.read_csv(summary_path)

    # Bar chart: applications per cluster
    plt.figure()
    if len(cluster_summary):
        ax = sns.barplot(
            data=cluster_summary.sort_values("num_applications", ascending=False),
            x="cluster_id",
            y="num_applications",
        )
        ax.bar_label(ax.containers[0])
        plt.xticks(rotation=45, ha="right")
        plt.xlabel("Cluster ID")
        plt.ylabel("# Applications")
        plt.title("Applications per Cluster")
    else:
        plt.title("No data")
    save_fig(os.path.join(outdir, "problem2_bar_chart.png"))

    # Density/Histogram: duration distribution for the largest cluster
    plt.figure()
    if len(cluster_summary):
        largest = (
            cluster_summary.sort_values("num_applications", ascending=False)["cluster_id"]
            .astype(str)
            .iloc[0]
        )
        t_largest = timeline.loc[timeline["cluster_id"].astype(str) == largest].copy()
        if len(t_largest):
            ax = sns.histplot(t_largest["duration_minutes"], bins=50, kde=True)
            plt.xscale("log")
            plt.xlabel("Job Duration (minutes, log scale)")
            plt.title(f"Duration Distribution — Cluster {largest} (n={len(t_largest)})")
        else:
            plt.title("No durations available")
    else:
        plt.title("No data")
    save_fig(os.path.join(outdir, "problem2_density_plot.png"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("master_url", nargs="?", help="spark://<MASTER_PRIVATE_IP>:7077")
    parser.add_argument("--net-id", required=False, help="Your NetID for S3 path")
    parser.add_argument(
        "--skip-spark",
        action="store_true",
        help="Skip Spark compute; only (re)draw figures from existing CSVs",
    )
    args = parser.parse_args()

    outdir = "data/output"
    os.makedirs(outdir, exist_ok=True)

    # Re-draw only
    if args.skip_spark:
        regenerate_viz_from_csv(outdir)
        return

    # Spark compute path
    if not (args.master_url and args.net_id):
        raise SystemExit("Provide master_url and --net-id, or use --skip-spark.")

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        regexp_extract,
        input_file_name,
        expr,
        min as smin,
        max as smax,
    )

    spark = (
        SparkSession.builder.appName("Problem2_Cluster_Usage")
        .master(args.master_url)
        .getOrCreate()
    )

    # Read all log lines recursively from your bucket
    s3_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/"
    logs_df = spark.read.option("recursiveFileLookup", "true").text(s3_path)

    # Add file path to extract IDs first
    df = logs_df.withColumn("file_path", input_file_name())

    # Extract application_id and cluster_id from file path
    # Path contains .../application_<clusterid>_<appnum>/...
    df = df.withColumn(
        "application_id",
        regexp_extract(col("file_path"), r"(application_\d+_\d+)", 1),
    ).withColumn(
        "cluster_id",
        regexp_extract(col("file_path"), r"application_(\d+)_\d+", 1),
    )

    # Extract timestamp from the beginning of the log line, e.g. "17/03/29 10:04:41 ..."
    # Use try_to_timestamp to tolerate invalid lines (returns NULL) and filter them out.
    df = df.withColumn(
        "ts_str",
        regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2})", 1),
    ).withColumn(
        "timestamp",
        expr("try_to_timestamp(ts_str, 'yy/MM/dd HH:mm:ss')"),
    ).filter(
        (col("application_id") != "") & col("timestamp").isNotNull()
    )

    # Per-application start/end time
    app_times = (
        df.groupBy("cluster_id", "application_id")
        .agg(
            smin("timestamp").alias("start_time"),
            smax("timestamp").alias("end_time"),
        )
        .filter(col("start_time").isNotNull() & col("end_time").isNotNull())
    )

    # Add app_number for readability (last numeric part of application_id)
    app_times = app_times.withColumn(
        "app_number", regexp_extract(col("application_id"), r"_(\d+)$", 1)
    )

    # Convert to pandas and compute durations
    pdf = app_times.toPandas()
    if not pdf.empty:
        pdf["duration_minutes"] = (
            (pdf["end_time"] - pdf["start_time"]).dt.total_seconds() / 60.0
        )
    else:
        pdf["duration_minutes"] = []

    # ----- Output 1: timeline
    timeline_cols = [
        "cluster_id",
        "application_id",
        "app_number",
        "start_time",
        "end_time",
        "duration_minutes",
    ]
    timeline = pdf[timeline_cols].sort_values(["cluster_id", "start_time"])
    timeline_path = os.path.join(outdir, "problem2_timeline.csv")
    timeline.to_csv(timeline_path, index=False)

    # ----- Output 2: cluster summary
    if len(timeline):
        summary = (
            timeline.groupby("cluster_id")
            .agg(
                num_applications=("application_id", "nunique"),
                cluster_first_app=("start_time", "min"),
                cluster_last_app=("end_time", "max"),
            )
            .reset_index()
        )
    else:
        summary = pd.DataFrame(
            columns=[
                "cluster_id",
                "num_applications",
                "cluster_first_app",
                "cluster_last_app",
            ]
        )
    summary_path = os.path.join(outdir, "problem2_cluster_summary.csv")
    summary.to_csv(summary_path, index=False)

    # ----- Output 3: stats.txt
    total_clusters = summary["cluster_id"].nunique() if len(summary) else 0
    total_apps = timeline["application_id"].nunique() if len(timeline) else 0
    avg_apps = summary["num_applications"].mean() if len(summary) else 0.0
    lines = [
        f"Total unique clusters: {total_clusters}",
        f"Total applications: {total_apps}",
        f"Average applications per cluster: {avg_apps:.2f}",
        "",
        "Most heavily used clusters (top 5):",
    ]
    if len(summary):
        for _, r in (
            summary.sort_values("num_applications", ascending=False).head(5).iterrows()
        ):
            lines.append(
                f"  Cluster {str(r['cluster_id'])}: {int(r['num_applications'])} applications"
            )
    with open(os.path.join(outdir, "problem2_stats.txt"), "w") as f:
        f.write("\n".join(lines))

    # ----- Output 4 & 5: figures
    sns.set_theme()

    # Bar chart
    plt.figure()
    if len(summary):
        ax = sns.barplot(
            data=summary.sort_values("num_applications", ascending=False),
            x="cluster_id",
            y="num_applications",
        )
        ax.bar_label(ax.containers[0])
        plt.xticks(rotation=45, ha="right")
        plt.xlabel("Cluster ID")
        plt.ylabel("# Applications")
        plt.title("Applications per Cluster")
    else:
        plt.title("No data")
    save_fig(os.path.join(outdir, "problem2_bar_chart.png"))

    # Density / histogram for largest cluster (log x)
    plt.figure()
    if len(summary):
        largest = (
            summary.sort_values("num_applications", ascending=False)["cluster_id"]
            .astype(str)
            .iloc[0]
        )
        t_largest = timeline.loc[timeline["cluster_id"].astype(str) == largest]
        if len(t_largest):
            ax = sns.histplot(t_largest["duration_minutes"], bins=50, kde=True)
            plt.xscale("log")
            plt.xlabel("Job Duration (minutes, log scale)")
            plt.title(
                f"Duration Distribution — Cluster {largest} (n={len(t_largest)})"
            )
        else:
            plt.title("No durations available")
    else:
        plt.title("No data")
    save_fig(os.path.join(outdir, "problem2_density_plot.png"))

    spark.stop()
    print("[SUCCESS] Problem 2 completed.")


if __name__ == "__main__":
    main()
