import json
from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer

app = Flask(__name__)

girl = 0
boy = 0

def poll_kafka():
    global girl, boy
    consumer = KafkaConsumer('result', bootstrap_servers='localhost:9092',
                             auto_offset_reset='latest')
    try:
        for msg in consumer:
            data = json.loads(msg.value.decode('utf8'))
            g = data.get('gender')
            c = int(data.get('count', 0))
            if g == '0':
                girl = c
            elif g == '1':
                boy = c
                print(f"girl={girl}, boy={boy}")
    except Exception as e:
        print(f"Kafka parse error: {e}")

import threading
t = threading.Thread(target=poll_kafka, daemon=True)
t.start()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/data")
def api_data():
    return jsonify({"girl": girl, "boy": boy})

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
