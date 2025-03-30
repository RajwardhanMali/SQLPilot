import re
from typing import Dict, List, Tuple
import json

class SchemaDesignEngine:
    def __init__(self, nlu_module):
        self.nlu_module = nlu_module
        
    def generate_schema(self, business_description):
        """Generate a schema based on business description"""
        schema_design = self.nlu_module.design_schema(business_description)
        return schema_design
    
    def generate_ddl(self, schema_design, dialect="Trino"):
        """Generate DDL statements for the schema"""
        if isinstance(schema_design, str):
            schema_design = json.loads(schema_design)  # Deserialize if schema_design is a string
        
        ddl_statements = []
        
        # Create dimension tables first
        dimension_tables = [table for table in schema_design["tables"] 
                          if table["type"].lower() == "dimension"]
        
        for table in dimension_tables:
            ddl = self._generate_table_ddl(table, dialect)
            ddl_statements.append(ddl)
        
        # Then create fact tables (to handle foreign keys)
        fact_tables = [table for table in schema_design["tables"] 
                      if table["type"].lower() == "fact"]
        
        for table in fact_tables:
            ddl = self._generate_table_ddl(table, dialect)
            ddl_statements.append(ddl)
            
        return ddl_statements
    
    def _generate_table_ddl(self, table_def, dialect):
        """Generate DDL for a specific table"""
        table_name = table_def["name"]
        columns = table_def["columns"]
        
        # Start building the DDL statement
        if dialect.lower() == "trino":
            ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        else:  # Spark SQL
            ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        
        # Add columns
        column_defs = []
        for col in columns:
            col_name = col["name"]
            # Change from data_type to dataType to match the schema
            col_type = self._map_data_type(col["dataType"].lower(), dialect)
            nullable = col.get("nullable", True)
            
            if nullable:
                column_defs.append(f"  {col_name} {col_type}")
            else:
                column_defs.append(f"  {col_name} {col_type} NOT NULL")
        
        # Add primary key constraints
        if "primary_key" in table_def:
            pk_cols = ", ".join(table_def["primary_key"])
            if dialect.lower() == "trino":
                column_defs.append(f"  PRIMARY KEY ({pk_cols})")
            else:  # Spark SQL doesn't support PRIMARY KEY in CREATE TABLE
                pass  # We'll handle this differently for Spark
        
        # Add foreign key constraints
        if "foreign_keys" in table_def:
            for fk in table_def["foreign_keys"]:
                fk_cols = ", ".join(fk["columns"])
                ref_table = fk["references"]["table"]
                ref_cols = ", ".join(fk["references"]["columns"])
                
                if dialect.lower() == "trino":
                    column_defs.append(
                        f"  FOREIGN KEY ({fk_cols}) REFERENCES {ref_table}({ref_cols})"
                    )
                else:  # Spark SQL doesn't support FOREIGN KEY in CREATE TABLE
                    pass  # We'll handle this differently for Spark
        
        ddl += ",\n".join(column_defs)
        ddl += "\n)"
        
        # Add table properties based on dialect
        if dialect.lower() == "spark sql":
            ddl += "\nUSING PARQUET"  # Default to Parquet format for Spark SQL
        
        return ddl

    def _parse_data_type(self, data_type):
        """Parse data type and extract length/precision if specified"""
        match = re.match(r'(\w+)(?:\((\d+(?:,\d+)?)\))?', data_type.lower())
        if not match:
            raise ValueError(f"Invalid data type format: {data_type}")
        base_type, length_spec = match.groups()
        return base_type, length_spec

    def _map_data_type(self, data_type, dialect):
        """
        Maps a generic data type to a SQL dialect-specific data type.
        Now handles length specifications like VARCHAR(255)
        """
        try:
            base_type, length_spec = self._parse_data_type(data_type)
            
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
            elif dialect.lower() == "spark_sql":
                # Mapping for Spark SQL
                type_mapping = {
                    "int": "INT",
                    "integer": "INT",
                    "text": "STRING",
                    "string": "STRING",
                    "varchar": "STRING",
                    "boolean": "BOOLEAN",
                    "bool": "BOOLEAN",
                    "float": "FLOAT",
                    "double": "DOUBLE",
                    "decimal": "DECIMAL",
                    "date": "DATE",
                    "timestamp": "TIMESTAMP",
                }
            else:
                raise ValueError(f"Unsupported SQL dialect: {dialect}")

            if base_type not in type_mapping:
                raise ValueError(f"Unsupported data type '{base_type}' for dialect '{dialect}'")

            mapped_type = type_mapping[base_type]
            
            # Append length specification for types that support it
            if length_spec:
                if mapped_type in ["VARCHAR", "DECIMAL"] and dialect.lower() == "trino":
                    return f"{mapped_type}({length_spec})"
                elif mapped_type in ["STRING", "DECIMAL"] and dialect.lower() == "spark_sql":
                    if mapped_type == "DECIMAL":
                        return f"{mapped_type}({length_spec})"
                    # Spark SQL doesn't need length for STRING type
                    return mapped_type
            
            return mapped_type

        except Exception as e:
            raise ValueError(f"Error mapping data type '{data_type}' for dialect '{dialect}': {str(e)}")