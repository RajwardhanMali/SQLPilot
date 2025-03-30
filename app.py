from flask import Flask, request, jsonify
from nlu_module import NLUModule
from flask_cors import CORS
from schema_design_engine import SchemaDesignEngine
from sql_generation_engine import SQLGenerationEngine
from query_execution_interface import QueryExecutionInterface
from feedback_loop_system import FeedbackLoopSystem
import os
from langchain_google_genai import ChatGoogleGenerativeAI
import config 
from talk_to_data import talk_to_data

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

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# ✅ API 1: Upload file and save it
@app.route("/upload_file", methods=["POST"])
def upload_file():
    """Handles file upload and saves it to the server."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    
    # Save file
    file.save(file_path)

    return jsonify({"message": "File uploaded successfully", "file_name": file.filename}), 200

# ✅ API 2: Execute query on the saved file
@app.route("/talk_to_data", methods=["POST"])
def talk_data():
    """Executes query on uploaded file if it exists."""

    file_name = request.json.get("file_name")
    query = request.json.get("query")

    if not file_name or not query:
        return jsonify({"error": "Both 'file_name' and 'query' are required"}), 400

    file_path = os.path.join(UPLOAD_FOLDER, file_name)
    print("File path:")
    print(file_path)
    # Process query inside the same Spark session
    try:
        response = talk_to_data(file_path, query, config.spark)
        return jsonify({"result": response})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
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

@app.route("/translate_sql", methods=["POST"])
def translate_sql():
    data = request.json
    natural_query = data.get("natural_query", "")
    schema_info = data.get("schema_info", "")
    dialect = data.get("dialect", "Trino")
    translated_sql = components["sql_engine"].translate_to_sql(natural_query, schema_info, dialect=dialect)
    return jsonify({"sql": translated_sql.get("sql"), "explaination": translated_sql.get("explanation")})

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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

