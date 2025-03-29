import sqlite3
import json

class FeedbackLoopSystem:
    def __init__(self, db_path="feedback.db"):
        self.db_path = db_path
        self._initialize_db()
    
    def _initialize_db(self):
        """Initialize the database to store feedback"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create table for storing feedback
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS query_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_type TEXT NOT NULL,
            input_text TEXT NOT NULL,
            generated_sql TEXT NOT NULL,
            corrected_sql TEXT,
            feedback_score INTEGER,
            user_comments TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Create table for schema feedback
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS schema_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_description TEXT NOT NULL,
            generated_schema TEXT NOT NULL,
            corrected_schema TEXT,
            feedback_score INTEGER,
            user_comments TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def record_query_feedback(self, query_type, input_text, generated_sql, 
                            corrected_sql=None, feedback_score=None, user_comments=None):
        """Record feedback for a generated SQL query"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO query_feedback (query_type, input_text, generated_sql, corrected_sql, feedback_score, user_comments)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (query_type, input_text, generated_sql, corrected_sql, feedback_score, user_comments))
        
        conn.commit()
        conn.close()
    
    def record_schema_feedback(self, business_description, generated_schema, 
                             corrected_schema=None, feedback_score=None, user_comments=None):
        """Record feedback for a generated schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Store schema as JSON string
        if isinstance(generated_schema, dict):
            generated_schema = json.dumps(generated_schema)
        
        if corrected_schema and isinstance(corrected_schema, dict):
            corrected_schema = json.dumps(corrected_schema)
        
        cursor.execute('''
        INSERT INTO schema_feedback (business_description, generated_schema, corrected_schema, feedback_score, user_comments)
        VALUES (?, ?, ?, ?, ?)
        ''', (business_description, generated_schema, corrected_schema, feedback_score, user_comments))
        
        conn.commit()
        conn.close()
    
    def get_similar_queries(self, input_text, limit=5):
        """Find similar queries to use for improving generation"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Simple matching based on words in the input
        words = set(input_text.lower().split())
        similar_queries = []
        
        cursor.execute('SELECT input_text, generated_sql, corrected_sql, feedback_score FROM query_feedback')
        for row in cursor.fetchall():
            stored_input, generated_sql, corrected_sql, score = row
            stored_words = set(stored_input.lower().split())
            
            # Calculate simple word overlap as similarity
            overlap = len(words.intersection(stored_words)) / max(len(words), len(stored_words))
            
            if overlap > 0.3:  # Threshold for considering it similar
                similar_queries.append({
                    'input': stored_input,
                    'generated_sql': generated_sql,
                    'corrected_sql': corrected_sql,
                    'feedback_score': score,
                    'similarity': overlap
                })
        
        conn.close()
        
        # Sort by similarity and return top matches
        similar_queries.sort(key=lambda x: x['similarity'], reverse=True)
        return similar_queries[:limit]
    
    def get_feedback_statistics(self):
        """Get statistics about the feedback data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        stats = {}
        
        # Query feedback stats
        cursor.execute('SELECT COUNT(*), AVG(feedback_score) FROM query_feedback')
        count, avg_score = cursor.fetchone()
        stats['query_feedback_count'] = count or 0
        stats['query_feedback_avg_score'] = avg_score or 0
        
        cursor.execute('SELECT query_type, COUNT(*) FROM query_feedback GROUP BY query_type')
        stats['query_type_distribution'] = dict(cursor.fetchall())
        
        # Schema feedback stats
        cursor.execute('SELECT COUNT(*), AVG(feedback_score) FROM schema_feedback')
        count, avg_score = cursor.fetchone()
        stats['schema_feedback_count'] = count or 0
        stats['schema_feedback_avg_score'] = avg_score or 0
        
        conn.close()
        return stats