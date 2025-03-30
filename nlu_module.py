import os
from typing import List, Optional
import json
from dotenv import load_dotenv
from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from pydantic import BaseModel
from tools import check_datatype_for_trino

class Column(BaseModel):
    name: str
    dataType: str
    description: str
    primaryKey: Optional[bool] = False
    foreignKey: Optional[bool] = False
    references: Optional[str] = None

class Table(BaseModel):
    name: str
    type: str  # "fact" or "dimension"
    columns: List[Column]

class SchemaDesignResponse(BaseModel):
    tables: List[Table]
    mermaid_code : str

class SqlTranslationResponse(BaseModel):
    sql: str
    explanation: str


class NLUModule:
    def __init__(self, model_name="models/gemini-2.0-flash"):
        api_key = os.environ.get("GOOGLE_API_KEY")
        
        if not api_key:
            raise ValueError("GOOGLE_API_KEY environment variable not set")
        
        mermaid_code = """
        erDiagram
        classDef usersStyle fill:#000,stroke:#1f77b4,stroke-width:2px;
        classDef postsStyle fill:#000,stroke:#ff7f0e,stroke-width:2px;
        classDef commentsStyle fill:#000,stroke:#2ca02c,stroke-width:2px;
        classDef tagsStyle fill:#000,stroke:#d62728,stroke-width:2px;
        classDef interactionsStyle fill:#000,stroke:#9467bd,stroke-width:2px;
        classDef bridgeStyle fill:#000,stroke:#8c564b,stroke-width:2px;

        dim_users {
            INT user_id PK
            VARCHAR(50) username
            DATE registration_date
            VARCHAR(100) email
        }
        class dim_users usersStyle

        dim_posts {
            INT post_id PK
            INT user_id FK
            VARCHAR(255) post_title
            DATE post_date
            TEXT post_content
        }
        class dim_posts postsStyle

        dim_comments {
            INT comment_id PK
            INT user_id FK
            INT post_id FK
            TEXT comment_text
            TIMESTAMP comment_date
        }
        class dim_comments commentsStyle

        dim_tags {
            INT tag_id PK
            VARCHAR(50) tag_name
        }
        class dim_tags tagsStyle

        fact_interactions {
            INT interaction_id PK
            INT user_id FK
            INT post_id FK
            INT comment_id FK
            TIMESTAMP interaction_timestamp
            VARCHAR(50) interaction_type
        }
        class fact_interactions interactionsStyle

        bridge_post_tags {
            INT post_id FK
            INT tag_id FK
        }
        class bridge_post_tags bridgeStyle

        dim_users ||--o{ dim_posts : "has"
        dim_users ||--o{ dim_comments : "writes"
        dim_posts ||--o{ dim_comments : "has"
        dim_posts ||--o{ fact_interactions : "involves"
        dim_comments ||--o{ fact_interactions : "involves"
        dim_users ||--o{ fact_interactions : "performs"
        dim_posts ||--o{ bridge_post_tags : "tagged with"
        dim_tags ||--o{ bridge_post_tags : "applies to"
        """
        self.llm = ChatGoogleGenerativeAI(
            model=model_name,
            temperature=0,
            google_api_key=api_key
        )

        # Schema Design Agent Setup
        self.schema_parser = PydanticOutputParser(pydantic_object=SchemaDesignResponse)
        self.schema_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are an expert database architect specialized in **Trino (Presto) SQL**. 
             Your goal is to **design a highly optimized, accurate OLAP schema** using Trino best practices.

            ## **Schema Design Guidelines**
            1. **Strictly follow Trino SQL syntax**:
            - **Use `INTEGER` instead of `BIGINT`**.
            - **Use `TIMESTAMP` instead of `DATETIME`**.
            - **Use `FLOAT` instead of `DECIMAL(10,2)`** for compatibility.
            2. **Ensure all column types are validated** using the `check_datatype_for_trino` tool.
            3. **Use explicit Primary Keys (PK) and Foreign Keys (FK)** to define relationships.
            
            ## **Mermaid Diagram Styling Rules**
            - **Fact tables:** Blue (`fill:#3498db, stroke:#000`).
            - **Dimension tables:** Orange (`fill:#f39c12, stroke:#000`).
            - **Ensure proper relationships with clear PK-FK references**.
            - **Use a light background with black stroke (`fill:#000, text:#fff`)**.
            - **Since `DECIMAL(10,2)` does not render correctly in Mermaid, replace it with `FLOAT`**.

            ## **Example Mermaid Code**
            ```
            {mermaid_code}
            ```

            **Ensure that the generated schema is free from syntax errors.**  
            Use the provided tools when necessary and strictly adhere to the format instructions.  

            {format_instructions}
            """),
            ("human", "{business_description}"),
            ("placeholder", "{agent_scratchpad}")
        ]).partial(format_instructions=self.schema_parser.get_format_instructions(), mermaid_code=mermaid_code)



        # SQL Translation Agent Setup
        self.sql_parser = PydanticOutputParser(pydantic_object=SqlTranslationResponse)
        self.sql_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are an expert SQL assistant. Convert natural language queries into optimized SQL.
            Use the provided tools when necessary and format the output as specified.
            Try to use joins and subqueries where appropriate.
            Try to use window functions and common table expressions where appropriate.
            Try to use aggregate functions and grouping where appropriate.
            Try to use filtering and sorting where appropriate.
            Try to use case statements and conditional logic where appropriate.
            Try to implement everything dialect specific features. 
            Try to  Give Optimized queries
            Dialect: {dialect}
            Schema: {schema}
            {format_instructions}"""),
            ("human", "{natural_query}"),
            ("placeholder", "{agent_scratchpad}")
        ]).partial(format_instructions=self.sql_parser.get_format_instructions())

    def design_schema(self, business_description, tools =[check_datatype_for_trino]):
        """Generate database schema design using an agent with tools"""
        agent = create_tool_calling_agent(
            llm=self.llm,
            prompt=self.schema_prompt,
            tools=tools
        )
        executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

        response = executor.invoke({
            "business_description": business_description
        })
        parsed_response = self.schema_parser.parse(response["output"])

        return json.loads(parsed_response.model_dump_json())


    def translate_to_sql(self, natural_query, schema, dialect="Trino", tools=[]):
            """Translate natural language to SQL using an agent with tools"""
            # print("schema:", schema)
            # schema = schema.join("\n")
            agent = create_tool_calling_agent(
                llm=self.llm,
                prompt=self.sql_prompt,
                tools=tools
            )
            executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

            response = executor.invoke({
                "natural_query": natural_query,
                "schema": schema,
                "dialect": dialect
            })
            return json.loads(self.sql_parser.parse(response["output"]).model_dump_json())

    def understand_intent(self, user_input, tools=[]):
        """Identify user's intent using an agent with tools"""
        intent_prompt = ChatPromptTemplate.from_messages([
            ("system", """Determine the user's intent from their input. Use available tools if needed.
            Possible intents:
            - SCHEMA_DESIGN: User wants to design a database schema
            - DDL_GENERATION: User wants to generate DDL statements
            - DML_GENERATION: User wants to generate DML statements
            - SQL_COMPLETION: User wants help completing a SQL query
            - SQL_TRANSLATION: User wants to translate natural language to SQL
            - SQL_EXECUTION: User wants to execute a SQL query"""),
            ("human", "{user_input}"),
            ("placeholder", "{agent_scratchpad}")
        ])

        agent = create_tool_calling_agent(
            llm=self.llm,
            prompt=intent_prompt,
            tools=tools
        )
        executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

        response = executor.invoke({"user_input": user_input})
        return response["output"].strip()


if __name__ == "__main__":
    nlu = NLUModule()
    schema = nlu.design_schema("Design a database schema for a social media platform that includes users, posts, comments, and tags.")
    translated_sql = nlu.translate_to_sql("show me total activities of users on wednesday", schema, "Trino", [])
    print("sql:",translated_sql.get("sql"))
    print("explaination:",translated_sql.get("explanation"))


   