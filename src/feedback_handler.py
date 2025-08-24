from flask import Flask, request

app = Flask(__name__)

@app.route('/')
def home():
    return "Feedback handler API is running."

@app.route('/feedback', methods=['POST'])
def receive_feedback():
    data = request.json
    print("Received feedback:", data)
    # Process/store feedback here
    return {"status": "success"}, 200

if __name__ == '__main__':
    app.run(port=5001)
