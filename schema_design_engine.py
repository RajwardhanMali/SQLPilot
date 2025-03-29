import re
from typing import Dict, List, Tuple

class SchemaDesignEngine:
    def __init__(self, nlu_module):
        self.nlu_module = nlu_module
        
    def generate_schema(self, business_description):
        """Generate a schema based on business description"""
        schema_design = self.nlu_module.design_schema(business_description)
        return schema_design
    
    def generate_ddl(self, schema_design, dialect="Trino"):
        """Generate DDL statements for the schema"""
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
            col_type = self._map_data_type(col["data_type"], dialect)
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
    
    def _map_data_type(self, data_type, dialect):
        """Map generic data types to dialect-specific types"""
        data_type = data_type.lower()
        
        if dialect.lower() == "trino":
            type_mapping = {
                "int": "INTEGER",
                "integer": "INTEGER",
                "bigint": "BIGINT",
                "string": "VARCHAR",
                "varchar": "VARCHAR",
                "text": "VARCHAR",
                "float": "REAL",
                "double": "DOUBLE",
                "decimal": "DECIMAL(18, 2)",
                "boolean": "BOOLEAN",
                "date": "DATE",
                "timestamp": "TIMESTAMP",
                "datetime": "TIMESTAMP"
            }
        else:  # Spark SQL
            type_mapping = {
                "int": "INT",
                "integer": "INT",
                "bigint": "BIGINT",
                "string": "STRING",
                "varchar": "STRING",
                "text": "STRING",
                "float": "FLOAT",
                "double": "DOUBLE",
                "decimal": "DECIMAL(18, 2)",
                "boolean": "BOOLEAN",
                "date": "DATE",
                "timestamp": "TIMESTAMP",
                "datetime": "TIMESTAMP"
            }
        
        # Handle dimensions like VARCHAR(255)
        if "varchar" in data_type and "(" in data_type:
            size = re.search(r'\((\d+)\)', data_type)
            if size and dialect.lower() == "trino":
                return f"VARCHAR({size.group(1)})"
            else:  # Spark SQL doesn't need size for STRING
                return "STRING"
        
        # Handle decimal precision and scale
        if "decimal" in data_type and "(" in data_type:
            size = re.search(r'\((\d+),\s*(\d+)\)', data_type)
            if size:
                precision, scale = size.group(1), size.group(2)
                return f"DECIMAL({precision}, {scale})"
        
        return type_mapping.get(data_type, "VARCHAR" if dialect.lower() == "trino" else "STRING")