from flask import Flask, request, jsonify
from nlu_module import NLUModule
from flask_cors import CORS
from schema_design_engine import SchemaDesignEngine
from sql_generation_engine import SQLGenerationEngine
from query_execution_interface import QueryExecutionInterface
from feedback_loop_system import FeedbackLoopSystem
import json
import os

app = Flask(__name__)
CORS(app)
def initialize_components():
    nlu = NLUModule()
    schema_engine = SchemaDesignEngine(nlu)
    sql_engine = SQLGenerationEngine(nlu)
    query_executor = QueryExecutionInterface()
    feedback_system = FeedbackLoopSystem()
    
    return {
        "nlu": nlu,
        "schema_engine": schema_engine,
        "sql_engine": sql_engine,
        "query_executor": query_executor,
        "feedback_system": feedback_system
    }

components = initialize_components()

@app.route("/generate_schema", methods=["POST"])
def generate_schema():
    data = request.json
    business_description = data.get("business_description", "")
    dialect = data.get("dialect", "Trino")
    schema = components["schema_engine"].generate_schema(business_description)
    ddl_statements = components["schema_engine"].generate_ddl(schema, dialect=dialect)
    return jsonify({"schema": schema, "ddl_statements": ddl_statements})

@app.route("/initialize_trino", methods=["POST"])
def initialize_trino():
    """API to initialize Trino connection dynamically"""
    data = request.json
    host = data.get("host", "localhost")
    port = data.get("port", 8080)
    user = data.get("user", "trino")
    catalog = data.get("catalog", "default")
    schema = data.get("schema", "default")

    result = components["query_executor"].initialize_trino(host, port, user, catalog, schema)
    return jsonify(result)

@app.route("/execute_ddl", methods=["POST"])
def execute_ddl():
    data = request.json
    ddl_statement = data.get("ddl", "")
    dialect = data.get("dialect", "trino").lower().replace(" ", "_")
    try:
        result = components["query_executor"].execute_query(ddl_statement, engine=dialect)
        return jsonify({"success": True, "message": "DDL executed successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/translate_sql", methods=["POST"])
def translate_sql():
    data = request.json
    natural_query = data.get("natural_query", "")
    schema_info = data.get("schema_info", "")
    dialect = data.get("dialect", "Trino")
    translated_sql = components["sql_engine"].translate_to_sql(natural_query, schema_info, dialect=dialect)
    return jsonify({"sql": translated_sql.get("sql"), "explaination": translated_sql.get("explanation")})

@app.route("/complete_sql", methods=["POST"])
def complete_sql():
    data = request.json
    partial_query = data.get("partial_query", "")
    schema_info = data.get("schema_info", "")
    dialect = data.get("dialect", "Trino")
    completed_query = components["sql_engine"].complete_sql(partial_query, schema_info, dialect=dialect)
    return jsonify({"completed_sql": completed_query})

@app.route("/execute_query", methods=["POST"])
def execute_query():
    initialize_trino()
    data = request.json
    sql_query = data.get("sql_query", "")
    dialect = data.get("dialect", "trino").lower().replace(" ", "_")
    schema = data.get("schema", None)

    if dialect != "trino":
        return jsonify({"success": False, "error": "Only Trino queries are supported for execution metrics"})

    try:
        # Execute query and fetch results + performance metrics
        result = components["query_executor"].execute_trino_query(sql_query,schema=schema)
        return jsonify({"success": "Query Executed Succesfully", "output": result, "error": None})
    
    except Exception as e:
        error = {
            "name": getattr(e, "name", type(e).__name__),  # Fallback to exception class name
            "message": getattr(e, "message", str(e))      # Fallback to full exception message
        }
        return jsonify({ "success": None,"output" : None, "error": error})

# {
#     "error": "TrinoUserError(type=USER_ERROR, name=SYNTAX_ERROR, message=\"line 1:8: mismatched input 'tab'. Expecting: 'CATALOG', 'FUNCTION', 'MATERIALIZED', 'OR', 'ROLE', 'SCHEMA', 'TABLE', 'VIEW'\", query_id=20250329_150238_00300_iiwxq)",
#     "success": false
# }

@app.route("/generate_dml", methods=["POST"])
def generate_dml():
    data = request.json
    operation = data.get("operation", "")
    table_name = data.get("table_name", "")
    schema_info = data.get("schema_info", "")
    dialect = data.get("dialect", "Trino")
    dml_statement = components["sql_engine"].generate_dml(
        operation=operation,
        table_name=table_name,
        schema=schema_info,
        dialect=dialect
    )
    return jsonify({"dml": dml_statement})

@app.route("/submit_feedback", methods=["POST"])
def submit_feedback():
    data = request.json
    feedback_type = data.get("type", "")
    input_query = data.get("input_query", "")
    output_query = data.get("output_query", "")
    corrected_sql = data.get("corrected_sql", None)
    feedback_score = data.get("feedback_score", 0)
    user_comments = data.get("user_comments", "")

    components["feedback_system"].record_query_feedback(
        feedback_type,
        input_query,
        output_query,
        corrected_sql=corrected_sql,
        feedback_score=feedback_score,
        user_comments=user_comments
    )
    return jsonify({"message": "Feedback submitted successfully"})

@app.route("/get_feedback_stats", methods=["GET"])
def get_feedback_stats():
    stats = components["feedback_system"].get_feedback_statistics()
    return jsonify({"stats": stats})

@app.route("/get_schema_from_file", methods=["POST"])
def get_schema_from_file():
    """Load schema from a JSON file"""
    data = request.json
    try:
        schema_path = data.get("schema_path", "schema.json")
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        return jsonify({"success": True, "schema": schema})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/get_table_names", methods=["GET"])
def get_table_names():
    """Get list of tables from schema"""
    try:
        schema_path = "schema.json"
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        table_names = [table['name'] for table in schema['tables']]
        return jsonify({"success": True, "tables": table_names})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/optimize_query", methods=["POST"])
def optimize_query():
    """Optimize a SQL query"""
    data = request.json
    query = data.get("query", "")
    schema_info = data.get("schema_info", "")
    dialect = data.get("dialect", "Trino")
    try:
        optimized = components["sql_engine"].optimize_query(query, schema_info, dialect)
        return jsonify({"success": True, "optimized_query": optimized})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/understand_intent", methods=["POST"])
def understand_intent():
    """Analyze user's intent from input"""
    data = request.json
    user_input = data.get("input", "")
    try:
        intent = components["nlu"].understand_intent(user_input)
        return jsonify({"success": True, "intent": intent})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/get_similar_queries", methods=["POST"])
def get_similar_queries():
    """Get similar queries from feedback history"""
    data = request.json
    input_text = data.get("input", "")
    limit = data.get("limit", 5)
    try:
        similar = components["feedback_system"].get_similar_queries(input_text, limit)
        return jsonify({"success": True, "similar_queries": similar})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/save_application_state", methods=["POST"])
def save_application_state():
    """Save current application state"""
    data = request.json
    try:
        state = {
            "schema_description": data.get("schema_description", ""),
            "schema_design": data.get("schema_design", {}),
            "current_sql": data.get("current_sql", "")
        }
        return jsonify({"success": True, "state": state})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/load_application_state", methods=["POST"])
def load_application_state():
    """Load saved application state"""
    data = request.json
    try:
        uploaded_data = data.get("state", {})
        return jsonify({"success": True, "loaded_state": uploaded_data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/record_schema_feedback", methods=["POST"])
def record_schema_feedback():
    """Record feedback for schema design"""
    data = request.json
    try:
        components["feedback_system"].record_schema_feedback(
            business_description=data.get("business_description", ""),
            generated_schema=data.get("generated_schema", {}),
            corrected_schema=data.get("corrected_schema"),
            feedback_score=data.get("feedback_score"),
            user_comments=data.get("user_comments")
        )
        return jsonify({"success": True, "message": "Schema feedback recorded"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/get_schema_statistics", methods=["GET"])
def get_schema_statistics():
    """Get statistics about the schema designs"""
    try:
        stats = components["feedback_system"].get_feedback_statistics()
        return jsonify({"success": True, "statistics": stats})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

