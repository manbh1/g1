def ingest_oracle_table_automatically(spark, jdbc_url, table_name, num_partitions, user, pwd):
    
    max_bucket = num_partitions - 1
    
    # 1. The Universal Query (Works on any table!)
    oracle_query = f"""
        (SELECT 
            t.*, 
            ORA_HASH(ROWID, {max_bucket}) as spark_hash_bucket 
        FROM {table_name} t) as tmp
    """
    
    # 2. The Universal Reader
    df = (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", oracle_query) 
        .option("user", user)
        .option("password", pwd)
        .option("partitionColumn", "spark_hash_bucket") 
        .option("lowerBound", "0")              
        .option("upperBound", str(num_partitions)) 
        .option("numPartitions", str(num_partitions))
        .option("fetchsize", "50000")
        .load()
    )
    
    return df









---


def ingest_oracle_with_predicates(spark, jdbc_url, table_name, num_partitions, user, pwd):
    
    # The Oracle ORA_HASH max bucket parameter is always (partitions - 1)
    max_bucket = num_partitions - 1
    
    # 1. Generate the exact list of SQL WHERE clauses using a Python list comprehension
    # This automatically creates a list like:
    # ["ORA_HASH(ROWID, 19) = 0", "ORA_HASH(ROWID, 19) = 1", ..., "ORA_HASH(ROWID, 19) = 19"]
    hash_predicates = [
        f"ORA_HASH(ROWID, {max_bucket}) = {i}" 
        for i in range(num_partitions)
    ]
    
    # 2. Define the connection properties (including the fetchsize for memory safety)
    connection_properties = {
        "user": user,
        "password": pwd,
        "driver": "oracle.jdbc.driver.OracleDriver",
        "fetchsize": "50000"
    }
    
    # 3. Execute the read using the native .jdbc() method
    df = spark.read.jdbc(
        url=jdbc_url,
        table=table_name, # We just pass the clean table name here (no subquery needed!)
        predicates=hash_predicates, # Spark will run 1 partition per item in this list
        properties=connection_properties
    )
    
    return df



---

from datetime import datetime

def ingest_live_oracle_table(spark, jdbc_url, table_name, num_partitions, user, pwd):
    
    # 1. Lock in the exact current time in Python
    # Format it exactly how Oracle expects it (YYYY-MM-DD HH24:MI:SS)
    snapshot_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    max_bucket = num_partitions - 1
    
    # 2. We inject "AS OF TIMESTAMP" directly into the table name or subquery.
    # This forces EVERY single Spark partition to ask Oracle for the exact same snapshot in time.
    oracle_query = f"""
        (SELECT 
            t.*, 
            ORA_HASH(ROWID, {max_bucket}) as spark_hash_bucket 
        FROM {table_name} AS OF TIMESTAMP TO_TIMESTAMP('{snapshot_time}', 'YYYY-MM-DD HH24:MI:SS') t) as tmp
    """
    
    # 3. Execute the read using the perfectly immune query
    df = (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", oracle_query) 
        .option("user", user)
        .option("password", pwd)
        .option("partitionColumn", "spark_hash_bucket") 
        .option("lowerBound", "0")              
        .option("upperBound", str(num_partitions)) 
        .option("numPartitions", str(num_partitions))
        .option("fetchsize", "50000")
        .load()
    )
    
    return df

---

def ingest_live_oracle_bulletproof(spark, jdbc_url, table_name, num_partitions, user, pwd):
    
    # 1. Ask Oracle for its CURRENT internal time (already formatted)
    # Using SYSTIMESTAMP guarantees we are using the database's own clock and timezone
    time_query = "(SELECT TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF') as db_time FROM DUAL) as tmp"
    
    time_df = (spark.read.format("jdbc")
               .option("url", jdbc_url)
               .option("dbtable", time_query)
               .option("user", user)
               .option("password", pwd)
               .load())
               
    # Extract the exact string from Oracle
    exact_db_time = time_df.collect()[0]["db_time"]
    
    # 2. Inject Oracle's OWN time back into the AS OF TIMESTAMP clause
    max_bucket = num_partitions - 1
    oracle_query = f"""
        (SELECT 
            t.*, 
            ORA_HASH(ROWID, {max_bucket}) as spark_hash_bucket 
        FROM {table_name} AS OF TIMESTAMP TO_TIMESTAMP('{exact_db_time}', 'YYYY-MM-DD HH24:MI:SS.FF') t) as tmp
    """
    
    # 3. Execute the 20 parallel reads...
    # (Rest of your PySpark read logic goes here)

---

# 1. The Read: Uses exactly 1 connection, 1 worker core, and results in 1 partition.
df_single = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "your_table")
    .option("fetchsize", "100000")
    .load()
)

# 2. The Write: Force Spark to shuffle the data across the cluster before writing.
(df_single
    .repartition(4) # Pushes the data from 1 worker core out to 4 worker cores
    .write
    .format("parquet")
    .mode("overwrite")
    .save("abfss://.../raw_parquet_folder/")
)
