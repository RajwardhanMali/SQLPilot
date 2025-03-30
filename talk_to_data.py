from langchain_community.agent_toolkits import SparkSQLToolkit, create_spark_sql_agent
from langchain_community.utilities.spark_sql import SparkSQL
from langchain_google_genai import ChatGoogleGenerativeAI
from pyspark.sql import SparkSession
from langchain_experimental.agents import create_spark_dataframe_agent
import pandas as pd
import os
# import pyspark.pandas as ps

def get_df(file_path, spark: SparkSession):
    extension = os.path.splitext(file_path)[1].lower()

    if extension in ['.csv']:
        return spark.read.csv(file_path, header=True, inferSchema=True)
    elif extension in ['.parquet']:
        return spark.read.parquet(file_path)
    elif extension in ['.orc']:
        return spark.read.orc(file_path)
    elif extension in ['.json']:
        csv_output_path = os.path.splitext(file_path)[0] + ".csv"
        df = pd.read_json(file_path)
        df.to_csv(csv_output_path, index=False)
        df = spark.read.csv(csv_output_path, header=True, inferSchema=True)
        return df
    else:
        return 'unknown'

# Initialize Spark Session
# spark = SparkSession.builder.getOrCreate()


# schema = "langchain_example"
# spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
# spark.sql(f"USE {schema}")

# Load Titanic dataset
# csv_file_path = "datasets/titanic.csv"
# table = "temp_table"
# df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
# print(type(df))
# edited_df = df.write.saveAsTable(table)
# print(type(edited_df))
# spark.table(table).show()


# csv_file_path = "datasets/titanic.parquet"
# table = "temp_table"
# df = spark.read.parquet(csv_file_path)
# # df = spark.read.option("mode", "DROPMALFORMED").json(csv_file_path)  # Removes corrupt records
# print(type(df))
# print(df)

# csv_file_path = "datasets/iris.orc"
# df = spark.read.orc(csv_file_path)
# print(type(df))
# print(df)



# csv_file_path = "datasets/titanic.json"
# table = "temp_table"
# df = pd.read_json(csv_file_path)
# print(type(df))
# print(df)
# psdf = ps.from_pandas(df)
# print(type(psdf))
# print(psdf)

'''
-------Handeled Json --------------
'''

#
# csv_file_path = "datasets/titanic.json"
# table = "temp_table"
# df = pd.read_json(csv_file_path)
# df.to_csv("output.csv", index=False)
# df = spark.read.csv("output.csv", header=True, inferSchema=True)


# Initialize SparkSQL and Gemini LLM
# spark_sql = SparkSQL(schema=schema)

def talk_to_data(path,query,spark: SparkSession):
    """Interact with data using the LLM"""
    # spark = SparkSession.builder.getOrCreate()
    df = get_df(path,spark=spark)
    llm = ChatGoogleGenerativeAI(model="models/gemini-2.0-flash", temperature=0)  # Use Gemini-Pro

    agent = create_spark_dataframe_agent(
        llm=llm, df=df, verbose=True, allow_dangerous_code=True , max_iterations = 5
    )

    ans = agent.invoke(query)
    print(ans)
    return ans

# # Create SparkSQL Agent
# toolkit = SparkSQLToolkit(db=spark_sql, llm=llm)
# agent_executor = create_spark_sql_agent(llm=llm, toolkit=toolkit, verbose=True)

# # Run Agent Query
# ans = agent_executor.invoke("What is the average age of passengers?")
# # print(ans)

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    talk_to_data("uploads/titanic.csv", "what is the average age of men who survived",spark)