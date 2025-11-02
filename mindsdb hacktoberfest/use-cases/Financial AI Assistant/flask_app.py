from flask import Flask, render_template, request, jsonify, Response
import mindsdb_sdk
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# --- Configuration ---
MDBG_HOST = os.getenv('MDBG_HOST', 'http://127.0.0.1:47334')
AGENT_NAME = os.getenv('AGENT_NAME', 'financial_reporting_agent')

# Global variables for MindsDB connection
server = None
agent = None

def init_mindsdb():
    global server, agent
    try:
        server = mindsdb_sdk.connect(MDBG_HOST)
        agent = server.agents.get(AGENT_NAME)
        return True
    except Exception as e:
        print(f"Failed to connect to MindsDB: {e}")
        return False

@app.route('/')
def index():
    return render_template('app.html')

@app.route('/chat', methods=['POST'])
def chat():
    try:
        data = request.json
        question = data.get('question', '')
        
        if not question:
            return jsonify({'error': 'No question provided'}), 400
        
        # Get response from agent
        completion = agent.completion_stream([{
            'question': question,
            'answer': None
        }])
        
        # Collect the response
        full_response = ""
        for chunk in completion:
            if 'output' in chunk:
                full_response = chunk['output']
        
        return jsonify({'response': full_response})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/stream_chat', methods=['POST'])
def stream_chat():
    def generate():
        try:
            data = request.json
            question = data.get('question', '')
            
            completion = agent.completion_stream([{
                'question': question,
                'answer': None
            }])
            
            for chunk in completion:
                if 'output' in chunk:
                    yield f"data: {json.dumps({'response': chunk['output']})}\n\n"
                elif 'actions' in chunk or 'steps' in chunk:
                    yield f"data: {json.dumps({'status': 'processing'})}\n\n"
            
            yield f"data: {json.dumps({'status': 'complete'})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return Response(generate(), mimetype='text/plain')

if __name__ == '__main__':
    if init_mindsdb():
        print("MindsDB connected successfully!")
        app.run(debug=True, host='0.0.0.0', port=5000)
    else:
        print("Failed to initialize MindsDB connection")