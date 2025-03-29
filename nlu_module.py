import os
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

class NLUModule:
    def __init__(self, model_name="models/gemini-1.5-pro"):
        # Initialize with Google Gemini model using direct API key
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY environment variable not set")
            
        self.llm = ChatGoogleGenerativeAI(
            model=model_name,
            temperature=0,
            google_api_key=api_key  # Pass API key directly
        )
        
        # Set up prompt templates for different tasks
        self.schema_design_prompt = ChatPromptTemplate.from_template(
            """You are an expert database architect. Based on the following business description, 
            design an optimal OLAP schema with fact and dimension tables.
            
            Business Description: {business_description}
            
            Respond with a JSON object that contains:
            1. A list of tables
            2. For each table, specify if it's a fact or dimension table
            3. For each table, provide column names, data types, and descriptions
            4. Primary keys and foreign key relationships
            """
        )
        
        self.sql_translation_prompt = ChatPromptTemplate.from_template(
            """
            You are an expert SQL assistant. Convert the following natural language query into an optimized SQL query.
            
            - Ensure the syntax follows the {dialect} dialect.
            - Use proper table and column names based on the provided schema.
            - Ensure correct joins, aggregations, and filters.
            - If the query requires a date range, handle it dynamically using {dialect}-compatible functions.
            - Give the answer in JSON format given below :
                Output format:
                {{
                "sql": "<Generated SQL Query>",
                "explanation": "<Explanation of the SQL Query>"
                }}


            Schema:
            {schema}

            Natural Language Query:
            "{natural_query}"

            SQL Query:
            """
        )
        
        # Output parser
        self.json_parser = JsonOutputParser()
    
    def design_schema(self, business_description):
        """Generate database schema design from business description"""
        chain = self.schema_design_prompt | self.llm | self.json_parser
        return chain.invoke({"business_description": business_description})
    
    def translate_to_sql(self, natural_query, schema, dialect="Trino"):
        """Translate natural language to SQL in specified dialect"""
        chain = self.sql_translation_prompt | self.llm | self.json_parser
        ans = chain.invoke({
            "natural_query": natural_query,
            "schema": schema,
            "dialect": dialect
        })
        print(ans['sql'])
        return ans['sql'].strip()
    
    def understand_intent(self, user_input):
        """Identify the user's intent from their input"""
        intent_prompt = ChatPromptTemplate.from_template(
            """Determine the user's intent from the following input:
            
            User Input: {user_input}
            
            Possible intents:
            - SCHEMA_DESIGN: User wants to design a database schema
            - DDL_GENERATION: User wants to generate DDL statements
            - DML_GENERATION: User wants to generate DML statements
            - SQL_COMPLETION: User wants help completing a SQL query
            - SQL_TRANSLATION: User wants to translate natural language to SQL
            - SQL_EXECUTION: User wants to execute a SQL query
            
            Respond with just the intent name.
            """
        )
        
        chain = intent_prompt | self.llm 
        return chain.invoke({"user_input": user_input}).content.strip()