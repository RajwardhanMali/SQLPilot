import streamlit as st
import subprocess
import json
import pandas as pd
from nlu_module import NLUModule
from schema_design_engine import SchemaDesignEngine
from sql_generation_engine import SQLGenerationEngine
from query_execution_interface import QueryExecutionInterface
from feedback_loop_system import FeedbackLoopSystem

# Initialize components
@st.cache_resource
def initialize_components():
    nlu = NLUModule()
    schema_engine = SchemaDesignEngine(nlu)
    sql_engine = SQLGenerationEngine(nlu)
    query_executor = QueryExecutionInterface()
    feedback_system = FeedbackLoopSystem()
    st.session_state.schema_description = None
    
    return {
        "nlu": nlu,
        "schema_engine": schema_engine,
        "sql_engine": sql_engine,
        "query_executor": query_executor,
        "feedback_system": feedback_system
    }

components = initialize_components()

# Set up the Streamlit app
st.title("SQLPilot: Your AI-Powered SQL Assistant")

# Create sidebar for configuration
with st.sidebar:
    st.header("Configuration")
    
    # SQL dialect selection
    dialect = st.selectbox(
        "SQL Dialect",
        ["Trino", "Spark SQL"],
        index=0
    )
    
    # Connection settings for query execution
    st.subheader("Connection Settings")
    
    if dialect == "Trino":
        trino_host = st.text_input("Trino Host", "localhost")
        trino_port = st.number_input("Trino Port", min_value=1, max_value=65535, value=8080)
        trino_user = st.text_input("Trino User", "trino")
        trino_catalog = st.text_input("Catalog", "default")
        trino_schema = st.text_input("Schema", "default")
        
        if st.button("Connect to Trino"):
            try:
                if components["query_executor"].initialize_trino(
                    host=trino_host,
                    port=trino_port,
                    user=trino_user,
                    catalog=trino_catalog,
                    schema=trino_schema
                ):
                    st.success("Connected to Trino successfully!")
            except Exception as e:
                st.error(f"Failed to connect to Trino: {str(e)}")
    else:
        spark_app_name = st.text_input("Spark App Name", "SQL Assistant")
        
        if st.button("Initialize Spark"):
            try:
                components["query_executor"].initialize_spark(app_name=spark_app_name)
                st.success("Spark session initialized successfully!")
            except Exception as e:
                st.error(f"Failed to initialize Spark: {str(e)}")
    
    # Show feedback statistics
    st.subheader("Feedback Stats")
    if st.button("Show Feedback Statistics"):
        stats = components["feedback_system"].get_feedback_statistics()
        st.write(stats)

# Create tabs for different functionalities
tabs = st.tabs([
    "Schema Design", 
    "SQL Translation", 
    "SQL Completion", 
    "Query Execution",
    "DML Generation"
])

# Tab 1: Schema Design
with tabs[0]:
    st.header("Database Schema Design")
    
    business_description = st.text_area(
        "Describe your business requirements",
        height=200,
        placeholder="e.g., I need a database for an e-commerce system that tracks products, orders, customers, and sales...",
        key="schema_input"
    )
    
    if st.button("Generate Schema"):
        if business_description:
            with st.spinner("Generating optimal schema..."):
                schema_design = components["schema_engine"].generate_schema(business_description)
                st.session_state.schema_design = schema_design
                
                # Display the schema design
                st.subheader("Generated Schema")
                st.json(schema_design)
                
                # Store schema description for other tabs
                schema_description = json.dumps(schema_design, indent=2)
                st.session_state.schema_description = schema_description
        else:
            st.warning("Please enter a business description")
    
    if "schema_design" in st.session_state:
        st.subheader("Generate DDL Statements")
        if st.button("Generate DDL"):
            with st.spinner("Generating DDL statements..."):
                ddl_statements = components["schema_engine"].generate_ddl(
                    st.session_state.schema_design,
                    dialect=dialect
                )
                
                # Display DDL statements
                for i, ddl in enumerate(ddl_statements):
                    st.code(ddl, language="sql")
                    
                    # Add option to execute DDL
                    if st.button(f"Execute DDL {i+1}", key=f"exec_ddl_{i}"):
                        try:
                            result = components["query_executor"].execute_query(
                                ddl,
                                engine=dialect.lower().replace(" ", "_")
                            )
                            st.success("DDL executed successfully")
                        except Exception as e:
                            st.error(f"Error executing DDL: {str(e)}")
        
        # Add schema feedback section
        st.subheader("Provide Feedback")
        schema_feedback = st.slider(
            "Rate the schema quality (1-5)",
            min_value=1,
            max_value=5,
            value=3
        )
        schema_comments = st.text_area("Comments or corrections",key="schema_comments")
        
        if st.button("Submit Schema Feedback"):
            components["feedback_system"].record_schema_feedback(
                business_description,
                st.session_state.schema_design,
                feedback_score=schema_feedback,
                user_comments=schema_comments
            )
            st.success("Thank you for your feedback!")

# Tab 2: SQL Translation
with tabs[1]:
    st.header("Natural Language to SQL Translation")
    
    if "schema_description" not in st.session_state:
        st.info("Please generate a schema in the Schema Design tab first, or enter schema details below")
        user_schema = st.text_area(
            "Enter your schema details",
            height=200,
            placeholder="e.g., Table: orders(id, customer_id, order_date, total_amount)...",
            key="user_schema_input"
        )
        if user_schema:
            st.session_state.user_schema = user_schema
    
    natural_query = st.text_area(
        "Enter your query in natural language",
        height=100,
        placeholder="e.g., Show me the total sales by product category for the last month",
        key="natural_query"
    )
    
    if st.button("Translate to SQL"):
        if natural_query:
            with st.spinner("Translating to SQL..."):
                schema_info = st.session_state.get("schema_description", st.session_state.get("user_schema", ""))
                
                sql_query = components["sql_engine"].translate_to_sql(
                    natural_query,
                    schema_info,
                    dialect=dialect
                )
                
                st.subheader("Generated SQL Query")
                st.code(sql_query, language="sql")
                
                # Store for execution
                st.session_state.current_sql = sql_query
                
                # Store for feedback
                st.session_state.current_natural_query = natural_query
        else:
            st.warning("Please enter a natural language query")
    
    # Add translation feedback section
    if "current_sql" in st.session_state and "current_natural_query" in st.session_state:
        st.subheader("Provide Feedback")
        
        corrected_sql = st.text_area(
            "Corrected SQL (if needed)",
            st.session_state.current_sql,
            height=150,
            key="corrected_sql"
        )
        
        translation_feedback = st.slider(
            "Rate the translation quality (1-5)",
            min_value=1,
            max_value=5,
            value=3
        )
        
        translation_comments = st.text_area("Comments",key="translation_comments")
        
        if st.button("Submit Translation Feedback"):
            components["feedback_system"].record_query_feedback(
                "translation",
                st.session_state.current_natural_query,
                st.session_state.current_sql,
                corrected_sql=corrected_sql if corrected_sql != st.session_state.current_sql else None,
                feedback_score=translation_feedback,
                user_comments=translation_comments
            )
            st.success("Thank you for your feedback!")

# Tab 3: SQL Completion
with tabs[2]:
    st.header("SQL Completion Assistant")
    
    if "schema_description" not in st.session_state:
        st.info("Please generate a schema in the Schema Design tab first, or enter schema details below")
        completion_schema = st.text_area(
            "Enter your schema details",
            height=200,
            placeholder="e.g., Table: orders(id, customer_id, order_date, total_amount)...",
            key="schema_completion"
        )
        if completion_schema:
            st.session_state.completion_schema = completion_schema
    
    partial_query = st.text_area(
        "Enter a partial SQL query",
        height=100,
        placeholder="e.g., SELECT * FROM orders WHERE",
        key="partial_query"
    )
    
    if st.button("Complete SQL"):
        if partial_query:
            with st.spinner("Completing SQL query..."):
                schema_info = st.session_state.get("schema_description", st.session_state.get("completion_schema", ""))
                
                completed_sql = components["sql_engine"].complete_sql(
                    partial_query,
                    schema_info,
                    dialect=dialect
                )
                
                st.subheader("Completed SQL Query")
                st.code(completed_sql, language="sql")
                
                # Store for execution
                st.session_state.current_sql = completed_sql
                
                # Store for feedback
                st.session_state.current_partial_query = partial_query
        else:
            st.warning("Please enter a partial SQL query")
    
    # Add completion feedback section
    if "current_sql" in st.session_state and "current_partial_query" in st.session_state:
        st.subheader("Provide Feedback")
        
        corrected_completion = st.text_area(
            "Corrected SQL (if needed)",
            st.session_state.current_sql,
            height=150,
            key="corrected_completion"
        )
        
        completion_feedback = st.slider(
            "Rate the completion quality (1-5)",
            min_value=1,
            max_value=5,
            value=3
        )
        
        completion_comments = st.text_area("Comments",key="completion_comments")
        
        if st.button("Submit Completion Feedback"):
            components["feedback_system"].record_query_feedback(
                "completion",
                st.session_state.current_partial_query,
                st.session_state.current_sql,
                corrected_sql=corrected_completion if corrected_completion != st.session_state.current_sql else None,
                feedback_score=completion_feedback,
                user_comments=completion_comments
            )
            st.success("Thank you for your feedback!")

# Tab 4: Query Execution
with tabs[3]:
    st.header("Query Execution")
    
    # Option to use query from other tabs
    if "current_sql" in st.session_state:
        st.info("You can use the SQL query generated in another tab")
        use_current = st.checkbox("Use current query")
        
        if use_current:
            query_to_execute = st.session_state.current_sql
        else:
            query_to_execute = st.text_area("Enter SQL query to execute", height=150,key="query_to_execute")
    else:
        query_to_execute = st.text_area("Enter SQL query to execute", height=150,key="query_to_execute")
    
    if st.button("Execute Query"):
        if query_to_execute:
            with st.spinner("Executing query..."):
                try:
                    result = components["query_executor"].execute_query(
                        query_to_execute,
                        engine=dialect.lower().replace(" ", "_")
                    )
                    
                    st.subheader("Query Results")
                    
                    if "rows" in result and "columns" in result:
                        # Create a DataFrame from the results
                        df = pd.DataFrame(result["rows"], columns=result["columns"])
                        st.dataframe(df)
                        
                        # Option to download results
                        csv = df.to_csv(index=False)
                        st.download_button(
                            "Download Results as CSV",
                            csv,
                            "query_results.csv",
                            "text/csv",
                            key="download-csv"
                        )
                    else:
                        st.success(result.get("message", "Query executed successfully"))
                        
                except Exception as e:
                    st.error(f"Error executing query: {str(e)}")
        else:
            st.warning("Please enter a SQL query to execute")

# Tab 5: DML Generation
with tabs[4]:
    st.header("DML Statement Generation")
    
    if "schema_description" not in st.session_state:
        st.info("Please generate a schema in the Schema Design tab first, or enter schema details below")
        dml_schema = st.text_area(
            "Enter your schema details",
            height=200,
            placeholder="e.g., Table: orders(id, customer_id, order_date, total_amount)...",
            key="dml_schema"
        )
        if dml_schema:
            st.session_state.dml_schema = dml_schema
    
    dml_operation = st.selectbox(
        "Select DML Operation",
        ["INSERT", "UPDATE", "DELETE"]
    )
    
    if st.session_state.schema_description :
        _schema = json.loads(st.session_state.schema_description)
        table_names = [table['name'] for table in _schema['tables']]
        
        table_name = st.selectbox(
            "Select a table",
            table_names
        )
    else :
        table_name = st.text_input("Enter table name")
    
    # data_input = st.text_area(
    #     "Enter data (JSON format for INSERT/UPDATE, WHERE conditions for DELETE)",
    #     height=150,
    #     placeholder='e.g., {"id": 1001, "customer_id": 500, "order_date": "2023-05-15", "total_amount": 199.99}',
    #     key="data_input"
    # )
    
    if st.button("Generate DML"):
        if table_name :
            with st.spinner(f"Generating {dml_operation} statement..."):
                schema_info = st.session_state.get("schema_description", st.session_state.get("dml_schema", ""))
                
                dml_statement = components["sql_engine"].generate_dml(
                    operation=dml_operation,
                    table_name=table_name,
                    schema=schema_info,
                    dialect=dialect

                )
                
                st.subheader(f"Generated {dml_operation} Statement")
                st.code(dml_statement, language="sql")
                
                # Store for execution
                st.session_state.current_sql = dml_statement
                
                # Add execute button
                if st.button("Execute DML"):
                    try:
                        result = components["query_executor"].execute_query(
                            dml_statement,
                            engine=dialect.lower().replace(" ", "_")
                        )
                        st.success("DML executed successfully")
                    except Exception as e:
                        st.error(f"Error executing DML: {str(e)}")
        else:
            st.warning("Please enter table name and data")

# Add a section for saving/loading the application state
st.sidebar.markdown("---")

# if st.sidebar.button("Save Current State"):
#     # Save the schema and other important states
#     state_to_save = {
#         "schema_description": st.session_state.get("schema_description", ""),
#         "schema_design": st.session_state.get("schema_design", {}),
#         "current_sql": st.session_state.get("current_sql", "")
#     }
    
#     # Convert to JSON string
#     state_json = json.dumps(state_to_save)
    
#     # Create download button
#     st.sidebar.download_button(
#         "Download State",
#         state_json,
#         "sql_assistant_state.json",
#         "application/json"
#     )

uploaded_state = st.sidebar.file_uploader("Upload a file", type=["json"])
if uploaded_state is not None:
    try:
        uploaded_data = json.load(uploaded_state)
        
        # Restore state
        for key, value in uploaded_data.items():
            st.session_state[key] = value
        
        st.sidebar.success("Application state restored successfully!")
    except Exception as e:
        st.sidebar.error(f"Error loading state: {str(e)}")

# Add a footer
st.markdown("---")
st.markdown("SQLPilot: Your AI-Powered SQL Assistant | © 2025")
