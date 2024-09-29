from flask import Flask, render_template, request, jsonify
import openai

app = Flask(__name__)

# Replace 'your-api-key' with your actual OpenAI API key
openai.api_key = '<Key>'

@app.route("/")
def index():
    return render_template('chat.html')

@app.route("/get", methods=["POST"])
def chat():
    msg = request.form["msg"]
    response = get_Chat_response(msg)
    return jsonify({"response": response})


def get_Chat_response(text):
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": text}],
        max_tokens=150,
        stop=None,
        temperature=0.7,
    )

    # Access the content from the first message in the response's choices
    return response['choices'][0]['message']['content'].strip()

    #return response.choices[0].text.strip()

if __name__ == '__main__':
    app.run(debug=True)
