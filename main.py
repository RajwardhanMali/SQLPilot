import os
import streamlit as st
import logging
from dotenv import load_dotenv

# Import our components
from nlu_module import NLUModule
from schema_design_engine import SchemaDesignEngine
from sql_generation_engine import SQLGenerationEngine
from query_execution_interface import QueryExecutionInterface
from feedback_loop_system import FeedbackLoopSystem

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sql_assistant.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def main():
    """Main entry point for the SQL Assistant application"""
    logger.info("Starting SQL Assistant application")
    
    # Check for required environment variables
    if not os.environ.get("OPENAI_API_KEY"):
        logger.error("OPENAI_API_KEY environment variable not set")
        print("Error: Please set the OPENAI_API_KEY environment variable")
        return
    
    # The Streamlit app is launched separately via 'streamlit run app.py'
    # This function can be used for command-line operation or testing
    logger.info("SQL Assistant initialized successfully")
    logger.info("Run 'streamlit run app.py' to start the web interface")

if __name__ == "__main__":
    main()