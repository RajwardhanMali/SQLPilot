from langchain_community.agent_toolkits import SparkSQLToolkit, create_spark_sql_agent
from langchain_community.utilities.spark_sql import SparkSQL
from langchain_google_genai import ChatGoogleGenerativeAI
from pyspark.sql import SparkSession
from langchain_experimental.agents import create_spark_dataframe_agent
import pandas as pd
import os

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

def talk_to_data(path,query,spark: SparkSession):
    """Interact with data using the LLM"""

    df = get_df(path,spark=spark)
    llm = ChatGoogleGenerativeAI(model="models/gemini-2.0-flash", temperature=0)  # Use Gemini-Pro

    agent = create_spark_dataframe_agent(
        llm=llm, df=df, verbose=True, allow_dangerous_code=True , max_iterations = 5
    )

    ans = agent.invoke(query)
    print(ans)
    return ans

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    talk_to_data("uploads/titanic.csv", "what is the average age of men who survived",spark)