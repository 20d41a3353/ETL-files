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












import os
from pyspark.sql import SparkSession

def get_spark_ha_session(cluster_name="fincore"):
    # 1. Fetch IPs from Environment Variables (injected from K8s Secrets)
    # Mapping expected: NN_HOST_1, NN_HOST_2, etc.
    nn_hosts = {
        f"nn{i}": os.getenv(f"NN_HOST_{i}") 
        for i in range(1, 6)
    }
    
    # Validation: Ensure we actually got the secrets
    if not all(nn_hosts.values()):
        missing = [k for k, v in nn_hosts.items() if not v]
        raise EnvironmentError(f"Missing K8s Secrets for: {missing}")

    builder = SparkSession.builder.appName(f"{cluster_name}ResilientApp")

    # 2. Base HA Config
    builder.config("fs.defaultFS", f"hdfs://{cluster_name}")
    builder.config("dfs.nameservices", cluster_name)
    builder.config(f"dfs.ha.namenodes.{cluster_name}", ",".join(nn_hosts.keys()))

    # 3. Dynamic RPC Mapping
    for nn_id, host_ip in nn_hosts.items():
        # Using 8020 as default, or use another env var if ports change
        builder.config(f"dfs.namenode.rpc-address.{cluster_name}.{nn_id}", f"{host_ip}:8020")

    # 4. The "Safe" Failover Logic
    # ObserverReadProxyProvider is best because it utilizes your 5th node for reads
    builder.config(
        f"dfs.client.failover.proxy.provider.{cluster_name}", 
        "org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider"
    )

    # 5. Stability Tuning (The "Don't Crash" settings)
    builder.config("dfs.client.retry.policy.enabled", "true")
    builder.config("dfs.client.failover.max.attempts", "15") # Gives cluster time to elect new leader
    builder.config("ipc.client.connect.max.retries", "10")    # Retries on network blips

    return builder.getOrCreate()

# Usage
spark = get_spark_ha_session()
