import json
import re

class SQLGenerationEngine:
    def __init__(self, nlu_module):
        self.nlu_module = nlu_module
    
    def clean_json_string(self,json_string):
        """Removes Markdown-style triple backticks from a JSON string"""
        return re.sub(r"^```json\n|\n```$", "", json_string.strip())
    
    def translate_to_sql(self, natural_query, schema, dialect="Trino"):
        """Translate natural language to SQL in the specified dialect"""
        return self.nlu_module.translate_to_sql(natural_query, schema, dialect)

    def generate_dml(self, operation, table_name, schema, dialect="Trino"):
        """Generate a general DML template (INSERT, UPDATE, DELETE) with placeholders for execution"""

        dml_prompt = f"""
        You are an expert SQL assistant. Generate a structured, optimized {operation} statement template for the table `{table_name}` in the `{dialect}` SQL dialect.

        - **Schema:** {json.dumps(schema, indent=2)}

        The template should use placeholders (e.g., `?`, `:param`) instead of actual values to allow the user to insert their own values at execution time.

        Output format:
        {{
            "sql_template": "<Generated SQL Template with placeholders>",
            "explanation": "<Brief explanation of the query structure>"
        }}
        """

        response = self.nlu_module.llm.invoke(dml_prompt)
        clean_content = self.clean_json_string(response.content)
    
        try:
            parsed_response = json.loads(clean_content)
            return parsed_response.get("sql_template", "")
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            return None

    def complete_sql(self, partial_query, schema, dialect="Trino"):
        """Provide SQL completion for a partial query with structured response"""
        completion_prompt = f"""
        You are an SQL query completion assistant. Complete the given `{dialect}` SQL query using the provided schema.

        - **Schema:** {json.dumps(schema, indent=2)}
        - **Partial Query:** `{partial_query}`

        Output format:
        {{
            "completed_sql": "<Fully completed SQL query>"
        }}
        """
        
        response = self.nlu_module.llm.invoke(completion_prompt)
        clean_content = self.clean_json_string(response.content)
    
        try:
            parsed_response = json.loads(clean_content)
            return parsed_response.get("completed_sql", "")
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            return None

    def optimize_query(self, query, schema, dialect="Trino"):
        """Optimize a SQL query for better performance with structured response"""
        optimization_prompt = f"""
        You are an SQL performance tuning expert. Optimize the following `{dialect}` SQL query for **maximum efficiency**.

        - **Schema:** {json.dumps(schema, indent=2)}
        - **Original Query:** `{query}`

        Provide:
        1. The optimized SQL query.
        2. A concise explanation of performance improvements.

        Output format:
        {{
            "optimized_sql": "<Optimized SQL query>",
            "explanation": "<Key optimizations applied>"
        }}
        """
        
        response = self.nlu_module.llm.invoke(optimization_prompt)
        clean_content = self.clean_json_string(response.content)
        try:
            parsed_response = json.loads(clean_content)
            return parsed_response.get("completed_sql", "")
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            return None
    
