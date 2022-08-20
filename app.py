import pickle
from unittest import result
from flask import Flask, request, app, jsonify, url_for, render_template
from flask_cors import cross_origin
import pandas as pd
import numpy as np
from app_log import log
from mongodb import MongoDBManagement
import warnings
warnings.filterwarnings("ignore")


app = Flask(__name__)
model = pickle.load(open('model.pkl', 'rb'))

#Running via Api
@app.route('/predict_api', methods=['POST'])
def predict_api():
    if request.method == 'POST':
        try:
            data = request.json["data"]
            new_data = [list(data.values())]
            output = model.predict(new_data)[0]
            if output == 1:
                text = 'The Forest is in Danger'
            else:
                text = 'Forest is Safe'
            return jsonify(text)

        except Exception as e:
            log.error('error in input from Postman', e)
            return jsonify('Check the input again!')
    else:
        return 'Method not POST'


#Running via html
@app.route('/', methods=['POST', 'GET'])
@cross_origin()
def index():
    try:
        log.info("Home page loaded successfully")
        return render_template('index.html')
    except Exception as e:
        log.exception("Something went wrong on initiation process")


@app.route('/single_classification', methods=['POST', "GET"])
def single_classification():
    try:
        log.info("single classification initialization successfull")
        return render_template('single_classification.html')
    except Exception as e:
        log.exception("Something went wrong on single_classification process", e)


@app.route('/predict_classification', methods=['POST', 'GET'])
@cross_origin()
def predict_classification():
    if request.method == 'POST':
        try:
            data=[float(x) for x in request.form.values()]
            final_features = [np.array(data)]
            output=model.predict(final_features)[0]
            if output == 0:
                return render_template('not_fire.html')
            else:
                return render_template('result_fire.html')
            return render_template('predict_classification.html')
        except Exception as e:
            log.error('Input error, check input', e)
    else:
        log.error('Post method expected')


@app.route('/batch_classification', methods=['POST', "GET"])
def batch_classification():
    try:
        log.info("batch_classification initialization successfull")
        mongoClient = MongoDBManagement(username='assignment', password='assignment')
        if mongoClient.isDatabasePresent(db_name='batch_data') == True:
            if mongoClient.isCollectionPresent(collection_name='classification_batch') == True:
                response = mongoClient.getRecords(collection_name='classification_batch')
                print(response)
                if response is not None:
                    batch = [i for i in response]
                    log.info("db batch_classification initialization successfull")
                    batch_reg = pd.DataFrame(batch)
                    test_data = batch_reg.drop(columns='_id')
                    test_data.to_html("class_batch.html")
                    data = model.predict(test_data.values)
                    log.info("Batch Pridiction successfull",)
                    return render_template('batch_classification.html', data=data)
        return render_template('single_classification.html')
    except Exception as e:
        log.exception(" Something went wrong on batch_classification process")


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8000, debug=True)