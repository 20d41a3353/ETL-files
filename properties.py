import requests
from pyspark.sql import SparkSession

import os

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

kafkalistener = os.getenv("kafkalistener") 
oracle_url = os.getenv("oracle_url") 
oracle_user = os.getenv("oracle_user") 
oracle_password = os.getenv("oracle_password") 
oracle_driver = os.getenv("oracle_driver") 
# environment = os.getenv("fincore_environment")

print(f"kafkalistener--------> {kafkalistener}")
print(f"oracle_url--------> {oracle_url}")
print(f"oracle_user--------> {oracle_user}")
print(f"oracle_password--------> {oracle_password}")
print(f"oracle_driver--------> {oracle_driver}")
# print(f"environment--------> {environment}")

# region = "DEV"
environment = "dev"
# oracle_user = "fincore"
# oracle_password = "Password#1234"
# oracle_driver = "oracle.jdbc.driver.OracleDriver"

# if(region == "ST"):     
#      oracle_url = "jdbc:oracle:thin:@//10.177.179.46:1523/fincorepdb1" #ST DB
#      kafkalistener = "kafka.cbops.svc.cluster.local:9092"
# if(region == "DEV"):    
#     oracle_url = "jdbc:oracle:thin:@//10.177.103.192:1523/fincorepdb1" # Dev
#     kafkalistener = "kafka.be-test.svc.cluster.local:9092"
# if(region == "UAT"):    
#     oracle_url = "jdbc:oracle:thin:@//10.177.179.85:1523/fincorepdb1"
#     kafkalistener =  "kafka.uat-cbops1.svc.cluster.local:9092"



builder = SparkSession.builder \
    .appName("DeltaLakeOperation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")


spark = configure_spark_with_delta_pip(builder).getOrCreate()




def get_hdfs_properties(environment):

    hdfs_properties = {
        "dev": {
            "nameservice": "fincoredev",
            "nodes": "10.177.103.199,10.177.103.192,10.177.103.196",
        },
        "sit": {
            "nameservice": "fincorest",
            "nodes": "10.177.179.59 ,10.177.179.60,10.177.179.61",
        },
        "uat": {
            "nameservice": "fincoreuat",
            "nodes": "10.177.179.99 ,10.177.179.100,10.177.179.101",
        },
        "preprod": {
            "nameservice": "fincorePP",
            "nodes": "10.177.179.158,10.177.179.159,10.177.179.160",
        },
        "prod": {
            "nameservice": "fincore",
            "nodes": "10.177.224.102,10.177.224.103,10.177.224.104,10.177.224.105,10.177.224.106",
        },
    }
    return (
        hdfs_properties[environment]["nameservice"],
        hdfs_properties[environment]["nodes"],
    )



  
    
 

def setup_dynamic_hdfs(spark,environment):
    ns, nodes_str = get_hdfs_properties(environment)

    port = "8022"
    node_list = [n.strip() for n in nodes_str.split(",") if n.strip()]
    if not node_list:
        print("Error: No valid nodes found")
        return None
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()

    nn_ids = [f"nn{i+1}" for i in range(len(node_list))]

    hadoop_conf.set("dfs.nameservices", ns)
    hadoop_conf.set(f"dfs.ha.namenodes.{ns}", ",".join(nn_ids))

    for nn_id, addr in zip(nn_ids, node_list):
        full_addr = f"{addr}:{port}"
        print(f"Setting {nn_id} to {full_addr}")
        hadoop_conf.set(f"dfs.namenode.rpc-address.{ns}.{nn_id}", full_addr)

    hadoop_conf.set(
        f"dfs.client.failover.proxy.provider.{ns}",
        "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
    )

    return ns


def get_oracle_properties():
    oracle_properties={
        "url":oracle_url,
        "user":oracle_user,
        "password":oracle_password,
        "driver":oracle_driver
    }    
    return oracle_properties

def get_active_nn_RPC(nn_list):
   conf = spark.sparkContext._jsc.hadoopConfiguration()
   gateway = spark.sparkContext._gateway
   
   for address in nn_list:
       try:
           uri = f"hdfs://{address}:8022"
           
           fs = gateway.jvm.org.apache.hadoop.fs.FileSystem.get(
               gateway.jvm.java.net.URI(uri),
               conf
           )
          
           fs.exists(gateway.jvm.org.apache.hadoop.fs.Path("/"))

           return address  

       except Exception:
           continue

   raise Exception("No Active NameNode found")



def get_jdbc_url():
    return oracle_url

def get_hdfs_base():
    
    nameservice = setup_dynamic_hdfs(spark,environment)
    HDFS_BASE = f"hdfs://{nameservice}"    
    # if nameservice is None:
    #     properties = get_hdfs_properties(environment)
    #     print("properties",properties)
    #     properties = list(properties)
    #     namenodes = properties[1].split(",")
    #     active_nn = get_active_nn_RPC(namenodes)
    #     HDFS_BASE = f"hdfs://{active_nn}:8022"
    # else:
    #     HDFS_BASE = f"hdfs://{nameservice}"
    return HDFS_BASE

def get_kafka_ip():
    if(kafkalistener):
        return kafkalistener
 




# import os

# # kafkalistener = os.getenv("kafkalistener") 
# # oracle_url = os.getenv("oracle_url") 
# # oracle_user = os.getenv("oracle_user") 
# # oracle_password = os.getenv("oracle_password") 
# # oracle_driver = os.getenv("oracle_driver") 

# # HDFS_BASE = "hdfs://10.177.179.99:8022" # UAT

# region = "DEV"
# oracle_user = "fincore"
# oracle_password = "Password#1234"
# oracle_driver = "oracle.jdbc.driver.OracleDriver"

# if(region == "ST"):
#      HDFS_BASE = "hdfs://10.177.179.61:8022" # ST
#      oracle_url = "jdbc:oracle:thin:@//10.177.179.46:1523/fincorepdb1" #ST DB
#      kafkalistener = "kafka.cbops.svc.cluster.local:9092"
# if(region == "DEV"):
#     HDFS_BASE = "hdfs://10.177.103.199:8022" #Dev
#     oracle_url = "jdbc:oracle:thin:@//10.177.103.192:1523/fincorepdb1" # Dev
#     kafkalistener = "kafka.be-test.svc.cluster.local:9092"
# if(region == "UAT"):
#     HDFS_BASE = "hdfs://10.177.179.99:8022" # UAT
#     oracle_url = "jdbc:oracle:thin:@//10.177.179.85:1523/fincorepdb1"
#     kafkalistener =  "kafka.uat-cbops1.svc.cluster.local:9092"


# def get_oracle_properties():
#     oracle_properties={
#         "url":oracle_url,
#         "user":oracle_user,
#         "password":oracle_password,
#         "driver":oracle_driver
#     }    
#     return oracle_properties

# def get_jdbc_url():
#     return oracle_url

# def get_hdfs_base():
#     return HDFS_BASE

# def get_kafka_ip():
#     if(kafkalistener):
#         return kafkalistener
 
# dsfd = get_kafka_ip()
# print(dsfd)

