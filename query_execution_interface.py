import trino
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd

class QueryExecutionInterface:
    def __init__(self):
        self.trino_connection = None
        self.spark_session = None
    
    def initialize_trino(self, host="localhost", port=8080, user="trino", catalog="default", schema="default"):
        """Initialize Trino connection"""
        self.trino_connection = trino.dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema
        )
        return self.trino_connection
    
    def initialize_spark(self, app_name="SQL Assistant", configs=None):
        """Initialize Spark session with optional configurations
        
        Args:
            app_name (str): Name of the Spark application
            configs (dict): Dictionary of Spark configuration options
        """
        # Start with the basic builder
        self.spark_session = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .getOrCreate()

        
        # Add any custom configurations
        # if configs:
        #     for key, value in configs.items():
        #         builder = builder.config(key, value)
        
        # Create the session with proper error handling
        try:
            # self.spark_session = builder.getOrCreate()
            # Test the session with a simple query
            self.spark_session.sql("SELECT 1").collect()
            return self.spark_session
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Spark session: {str(e)}")
    
    def execute_trino_query(self, query):
        """Execute a query using Trino"""
        if not self.trino_connection:
            raise ValueError("Trino connection not initialized")
        
        cursor = self.trino_connection.cursor()
        cursor.execute(query)
        
        # Get column names
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch results
            rows = cursor.fetchall()
            
            # Create a list of dictionaries for the results
            results = []
            for row in rows:
                result = dict(zip(columns, row))
                results.append(result)
            
            return {"columns": columns, "rows": rows, "results": results}
        else:
            # For DDL/DML statements that don't return results
            return {"message": "Query executed successfully"}
    
    def execute_spark_query(self, query):
        """Execute a query using Spark SQL with improved error handling and data conversion"""
        if not self.spark_session:
            raise ValueError("Spark session not initialized")
        
        try:
            # Execute the query
            result_df = self.spark_session.sql(query)
            
            # Convert to a pandas DataFrame for easier processing
            pdf = result_df.toPandas()
            
            # Get columns
            columns = pdf.columns.tolist()
            
            # Convert rows to list of tuples
            row_tuples = [tuple(row) for row in pdf.values]
            
            # Create a list of dictionaries for the results
            results = pdf.to_dict('records')
            
            return {"columns": columns, "rows": row_tuples, "results": results}
        
        except Exception as e:
            # Provide detailed error information
            error_msg = str(e)
            if "AnalysisException" in error_msg:
                if "Table or view not found" in error_msg:
                    return {"error": f"Table not found: {error_msg}"}
                else:
                    return {"error": f"SQL analysis error: {error_msg}"}
            elif "ParseException" in error_msg:
                return {"error": f"SQL parsing error: {error_msg}"}
            else:
                return {"error": f"Spark execution error: {error_msg}"}
    
    def execute_query(self, query, engine="trino"):
        """Execute a query with the specified engine"""
        if engine.lower() == "trino":
            return self.execute_trino_query(query)
        elif engine.lower() in ["spark", "spark_sql"]:
            return self.execute_spark_query(query)
        else:
            raise ValueError(f"Unsupported engine: {engine}")
    
    def create_temp_table_from_data(self, data, table_name, schema=None):
        """Create a temporary Spark table from a data source
        
        Args:
            data: List of dictionaries, pandas DataFrame, or path to file
            table_name: Name for the temporary table
            schema: Optional Spark schema for the data
        """
        if not self.spark_session:
            raise ValueError("Spark session not initialized")
        
        try:
            # Different handling based on data type
            if isinstance(data, pd.DataFrame):
                df = self.spark_session.createDataFrame(data)
            elif isinstance(data, list) and all(isinstance(item, dict) for item in data):
                df = self.spark_session.createDataFrame(data)
            elif isinstance(data, str):
                # Assume it's a file path - try to infer format
                if data.endswith('.csv'):
                    df = self.spark_session.read.csv(data, header=True, inferSchema=True)
                elif data.endswith('.json'):
                    df = self.spark_session.read.json(data)
                elif data.endswith('.parquet'):
                    df = self.spark_session.read.parquet(data)
                else:
                    raise ValueError(f"Unsupported file format: {data}")
            else:
                raise ValueError("Unsupported data format")
            
            # Create or replace temp view
            df.createOrReplaceTempView(table_name)
            return {"message": f"Temporary table '{table_name}' created successfully with {df.count()} rows"}
        
        except Exception as e:
            return {"error": f"Failed to create temporary table: {str(e)}"}
    
    def list_tables(self, engine="spark"):
        """List available tables in the current session"""
        if engine.lower() == "spark":
            if not self.spark_session:
                raise ValueError("Spark session not initialized")
            tables = self.spark_session.catalog.listTables()
            return [table.name for table in tables]
        elif engine.lower() == "trino":
            if not self.trino_connection:
                raise ValueError("Trino connection not initialized")
            cursor = self.trino_connection.cursor()
            cursor.execute("SHOW TABLES")
            return [row[0] for row in cursor.fetchall()]
        else:
            raise ValueError(f"Unsupported engine: {engine}")
        
if __name__ == "__main__":
    # Initialize with more configuration options
    interface = QueryExecutionInterface()
    interface.initialize_spark(
        app_name="MySparkApp",
        configs={
            "spark.driver.memory": "4g",
            "spark.executor.memory": "4g",
            "spark.sql.shuffle.partitions": "10"
        }
    )

    # Load some test data
    test_data = [
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "Bob", "value": 200},
        {"id": 3, "name": "Charlie", "value": 300}
    ]
    interface.create_temp_table_from_data(test_data, "test_table")

    # Run a query
    result = interface.execute_query("SELECT * FROM test_table WHERE value > 150", engine="spark")
    print(result)