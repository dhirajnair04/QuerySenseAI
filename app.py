from flask import Flask, render_template, request, jsonify
from agent import QueryAgent
import os
from flask import send_from_directory
from agent import export_jobs

# Initialize Flask app
app = Flask(__name__)

# Initialize the QueryAgent
# This loads the .env file, connects to the DB, and sets up the LLM agent.
try:
    print("Initializing QueryAgent...")
    query_agent = QueryAgent()
    print("QueryAgent initialized successfully.")
except Exception as e:
    print(f"CRITICAL ERROR: Failed to initialize QueryAgent.")
    print(f"Error: {e}")
    # If the agent fails to load (e.g., DB connection), we can't run the app.
    query_agent = None

@app.route('/')
def index():
    """
    Serves the main chat page (index.html).
    """
    return render_template('index.html')

@app.route('/api/chat', methods=['POST'])
def chat():
    """
    API endpoint to handle chat messages.
    Takes a JSON request {'message': 'user_query'}
    Returns a JSON response from the QueryAgent.
    """
    if not query_agent:
        return jsonify({"answer": "Error: The Query Agent is not initialized. Please check server logs.", "data": [], "query": ""}), 500
        
    data = request.json
    user_message = data.get('message')
    history = data.get('history', [])  # <-- 1. Get the history array

    if not user_message:
        return jsonify({"answer": "Error: No message provided.", "data": [], "query": ""}), 400

    try:
        response = query_agent.ask(user_message, history)  # <-- 2. Pass history to the agent

        # Handle the case where agent returns None or an unexpected type
        if not response or not isinstance(response, dict):
            print("⚠️ QueryAgent returned an invalid response format.")
            return jsonify({
                "answer": "I'm sorry, I couldn't process that question at the moment.",
                "data": [],
                "query": "",
                "is_fact": False
            }), 500

        return jsonify(response)

    except Exception as e:
        # Log safely without leaking API details
        print(f"⚠️ Error during agent query: {repr(e)}")
        return jsonify({
            "answer": "I'm sorry, I encountered a temporary issue while generating that insight. Please try again.",
            "data": [],
            "query": "",
            "is_fact": False
        }), 500
    
@app.route('/export_status/<job_id>')
def export_status(job_id):
    job = export_jobs.get(job_id, None)
    if not job:
        return {"status": "not_found"}, 404
    return job

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory("exports", filename, as_attachment=True)


if __name__ == '__main__':
    app.run(debug=True, port=5005)