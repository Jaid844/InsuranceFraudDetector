from wsgiref import simple_server
from flask import Flask, request, render_template
from  flask import Response
import os
from flask_cors import CORS, cross_origin
from prediction_validation import pred
from Training_validation import training_val
from trainmodel import training
from predictfrom_model import predictfrom_model


os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
CORS(app)



@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')


@app.route('/predict', methods=['POST'])
@cross_origin()
def predict_route():
    try:
        if request.json is not None:
            path=request.json['filepath']
            pred_val=pred(path)
            pred_val.predic()
            pre_model=predictfrom_model(path)
            path = pre_model.predict()

            return Response("Prediction file created at Prediction_Output_File" ,headers={'Content-Type': 'application/json'})

        elif request.form is None:
            path=request.form['filepath']
            pre_val=pred(path)
            pre_val.predic()
            pre_model=predictfrom_model(path)
            path=pre_model.predict()
            return Response("Prediction file created at %s"%path,headers={'Content-Type': 'application/json'})

    except Exception as e:
        return Response("Error Occurred! %s" % e,headers={'Content-Type': 'application/json'})


@app.route("/train", methods=['GET'])
@cross_origin()
def train():
     try:
         if request.json['folderPath'] is not None:
            path=request.json['folderPath']
            train_obj=training_val(path)
            train_obj.val()
            trainmodel_obj=training()
            trainmodel_obj.train()
     except Exception as e:
         return Response("Error happned %s" %e,headers={'Content-Type': 'application/json'})
     return Response("Training successfull!!",headers={'Content-Type': 'application/json'})

port = int(os.getenv("PORT",5000))
if __name__ == "__main__":
    app.run(port=port,debug=True,host="0.0.0.0")
























