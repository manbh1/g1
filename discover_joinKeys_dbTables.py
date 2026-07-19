import os
from pyspark.sql import SparkSession
 
def discover_pyspark_join_keys(parent_dir, table_names, overlap_threshold=0.80):
    # Initialize a local Spark session utilizing all available CPU cores
    spark = SparkSession.builder \
        .appName("NaiveJoinKeyDiscovery") \
        .master("local[*]") \
        .config("spark.sql.analyzer.failOnHint", "false") \
        .getOrCreate()
    
    # 1. Register all parquet subfolders as temporary raw SQL views
    for table in table_names:
        table_path = os.path.join(parent_dir, table)
        # Read the parquet directory and create a queryable SQL view name matching the folder name
        spark.read.parquet(table_path).createOrReplaceTempView(table)
    
    # 2. Extract column metadata for each table using raw Spark SQL commands
    table_columns = {}
    for table in table_names:
        try:
            # Run pure SQL to inspect column names from the registered views
            columns_info = spark.sql(f"SHOW COLUMNS IN {table}").collect()
            table_columns[table] = [row['col_name'] for row in columns_info]
        except Exception:
            continue
 
    print(f"{'Source Table.Col':<40} -> {'Target Table.Col':<40} | Overlap %")
    print("-" * 95)
 
    # 3. Naive brute-force cross-table loops using pure dynamically generated Spark SQL
    for table_a in table_names:
        for col_a in table_columns.get(table_a, []):
            for table_b in table_names:
                # Only check columns across different tables
                if table_a == table_b:
                    continue
                    
                for col_b in table_columns.get(table_b, []):
                    # Construct the pure cross-table SQL string
                    query = f"""
                        SELECT 
                            COUNT(DISTINCT `{col_a}`) AS total_left,
                            COUNT(DISTINCT CASE WHEN `{col_a}` IN (SELECT `{col_b}` FROM {table_b}) THEN `{col_a}` END) AS overlap
                        FROM {table_a}
                    """
                    
                    try:
                        # Execute the query using Spark SQL engine
                        result = spark.sql(query).collect()[0]
                        total_left = result['total_left']
                        overlap = result['overlap']
                        
                        # Process overlap ratio if data exists on the left side
                        if total_left and total_left > 0:
                            overlap_ratio = overlap / total_left
                            if overlap_ratio >= overlap_threshold:
                                print(f"{table_a}.{col_a:<30} -> {table_b}.{col_b:<30} | {overlap_ratio:.1%}")
                                
                    except Exception:
                        # Catch-all block for incompatible data types or unexpected SQL analysis syntax issues
                        continue
                        
    spark.stop()
 
# Example usage:
# discover_pyspark_join_keys(parent_dir="/path/to/parent_folder", table_names=["orders", "customers", "items"])
 
