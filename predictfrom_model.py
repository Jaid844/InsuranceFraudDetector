import numpy as np

from log.applogger import Applogger
from Datagetter_prediction.Dataingestion import data
from file_operation.file_operation import file_op
from  Predictioin_file.Prediction_validation import prediction_validation
from Preprocessing.Preprocessing_file import preprocess
import pandas as pd
from prediction_validation import pred


class predictfrom_model:

    def __init__(self,path):
        self.log=Applogger()
        self.p=preprocess()
        self.preprocess=prediction_validation(path)
        self.data=data()
        self.file=file_op()


    def predict(self):
        try:
            self.file_object=open('Prediction_Logs/Prediction_Logs.txt','a+')
            self.preprocess.deletePredictionFile()  # deletes the existing prediction file from last run!
            self.log.log(self.file_object, 'Start of Prediction')
            data = self.data.get()

            # code change
            # wafer_names=data['Wafer']
            # data=data.drop(labels=['Wafer'],axis=1)

            data = self.p.removecolumn(data,
                                               ['policy_number', 'policy_bind_date', 'policy_state', 'insured_zip',
                                                'incident_location', 'incident_date', 'incident_state', 'incident_city',
                                                'insured_hobbies', 'auto_make', 'auto_model', 'auto_year', 'age',
                                                'total_claim_amount'])  # remove the column as it doesn't contribute to prediction.
            data.replace('?', np.NaN, inplace=True)  # replacing '?' with NaN values for imputation

            # check if missing values are present in the dataset
            is_null_present, cols_with_missing_values = self.p.null_present(data)

            # if missing values are there, replace them appropriately.
            if (is_null_present):
                data = self.p.imputemissingvalue(data, cols_with_missing_values)  # missing value imputation
            # encode categorical data
            data = self.p.encode_cat(data)
            data = self.p.scale_numerical(data)


            kmeans =self.file.load_model('KMeans')

            ##Code changed

            clusters = kmeans.predict(data)
            data['clusters'] = clusters
            clusters = data['clusters'].unique()
            predictions = []
            for i in clusters:
                cluster_data = data[data['clusters'] == i]
                cluster_data = cluster_data.drop(['clusters'], axis=1)
                model_name = self.file.find_correct_model(i)
                model = self.file.load_model(model_name)
                result = (model.predict(cluster_data))
                for res in result:
                    if res == 0:
                        predictions.append('N')
                    else:
                        predictions.append('Y')

            final = pd.DataFrame(list(zip(predictions)), columns=['Predictions'])
            path = "Prediction_Output_File/Predictions.csv"
            final.to_csv("Prediction_Output_File/Predictions.csv", header=True,
                         mode='a+')  # appends result to prediction file
            self.log_writer.log(self.file_object, 'End of Prediction')
        except Exception as e:
            log_file=open("Prediction_Logs/Prediction_Logs.txt",'w')
            self.log.log(log_file,str(e))
            log_file.close()







#c=predictfrom_model(r'C:\Users\91639\Desktop\insurancefraud\Prediction_FileFromDB')
#c.predict()

















