from flask import Flask, request, jsonify, render_template, Response
import json

from utils import *

app = Flask(__name__)


@app.route('/',methods=['POST'])
def predict():
    if request.method == "POST":
        
        req = request.get_json()
        uid = req["uid"]
        book_list = req["book_list"]

        user_features = get_user(uid)
        books_features = get_books(list(map(str, book_list)))
        interactions = get_interactions(uid, list(map(str, book_list)))

        df = pre_process_request_data(user_features, books_features, interactions)
        predicted_results = predict_at_k(df, len(book_list))
        results = get_results(predicted_results, book_list)

        return Response(json.dumps(str(results)), mimetype='application/json')


if __name__ == "__main__":
    app.run( port = 5000, debug=True, host="0.0.0.0")