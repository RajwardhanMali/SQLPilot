from typing import Dict
from langchain.tools import Tool
from query_execution_interface import QueryExecutionInterface
import pandas as pd
import re

## SOLVE ERRORS WHICH COME IN BETWEEN AS MIGHT HURT THE CALL CHAIN WHEN GIVEN TO AGENT

def get_tables(catalog: str, schema: str) -> Dict:
    """Fetches information/schema of  tables with columns."""
    interface = QueryExecutionInterface()
    interface.initialize_trino()
    
    query =  f"SELECT table_name, column_name, data_type, ordinal_position FROM {catalog}.information_schema.columns WHERE table_schema = '{schema}' ORDER BY table_name, ordinal_position"
    
    return  interface.execute_trino_query(query, schema=schema).get("rows")

get_table = Tool(
    name="get_schema",
    func=get_tables,
    description="Fetches information/schema of  tables with columns from a given specific schema."
)

def get_schemas(catalog: str) -> Dict:
    """Fetches information about schemas in the given catalog."""
    interface = QueryExecutionInterface()
    interface.initialize_trino()
    
    query = f"SELECT schema_name FROM {catalog}.information_schema.schemata"
    
    return interface.execute_trino_query(query,schema="information_schema").get("rows")

get_schemas = Tool(
    name="get_schemas",
    func=get_schemas,
    description="Fetches information about schemas in the given catalog."
)

def _parse_data_type(data_type):
        """Parse data type and extract length/precision if specified"""
        match = re.match(r'(\w+)(?:\((\d+(?:,\d+)?)\))?', data_type.lower())
        if not match:
            raise ValueError(f"Invalid data type format: {data_type}")
        base_type, length_spec = match.groups()
        return base_type, length_spec

def _map_data_type( data_type, dialect ="trino"):
        """
        Maps a generic data type to a SQL dialect-specific data type.
        Now handles length specifications like VARCHAR(255)
        """
        base_type, length_spec = _parse_data_type(data_type)
        
        if dialect.lower() == "trino":
            # Mapping for Trino
            type_mapping = {
                "int": "INTEGER",
                "integer": "INTEGER",
                "text": "VARCHAR",
                "string": "VARCHAR",
                "varchar": "VARCHAR",
                "boolean": "BOOLEAN",
                "bool": "BOOLEAN",
                "float": "DOUBLE",
                "double": "DOUBLE",
                "decimal": "DECIMAL",
                "date": "DATE",
                "timestamp": "TIMESTAMP",
            }
            return True
        else:
            return False

check_datatype_for_trino = Tool(
    name="check_datatype_for_trino",
    func=_map_data_type,
    description="Takes datatype and dialect as input and checks if the data type is supported by the dialect. Currently only trino is supported."
)

if __name__ == "__main__":
    print("schema info",get_schemas("memory"))
    print("table info",get_tables("memory", "default"))
