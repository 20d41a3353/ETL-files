from pyspark.sql import SparkSession

# Define your NameNode hosts
nn_hosts = {
    "nn1": "host1:8020",
    "nn2": "host2:8020",
    "nn3": "host3:8020",
    "nn4": "host4:8020",
    "nn5": "host5:8020"
}

builder = SparkSession.builder \
    .appName("FincoreResilientApp") \
    .config("fs.defaultFS", "hdfs://fincore") \
    .config("dfs.nameservices", "fincore") \
    .config("dfs.ha.namenodes.fincore", ",".join(nn_hosts.keys()))

# Dynamically set the RPC addresses for all 5 nodes
for nn_id, address in nn_hosts.items():
    builder.config(f"dfs.namenode.rpc-address.fincore.{nn_id}", address)

# Use the ObserverReadProxyProvider for better load balancing
# This allows reads from the Observer and writes to the Active node
builder.config(
    "dfs.client.failover.proxy.provider.fincore", 
    "org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider"
)

# Optional: Set automatic retries for more stability
builder.config("dfs.client.failover.max.attempts", "15")
builder.config("dfs.client.failover.sleep.base.millis", "1000")

spark = builder.getOrCreate()

# Example Usage:
# Even if nn1 fails during this action, Spark will retry on nn2, nn3, etc.
# df = spark.read.csv("hdfs://fincore/path/to/data.csv")
