from log.applogger import Applogger
import pandas as pd
from os import listdir



class data:

    def __init__(self):
        self.log=Applogger()
        self.data_file='Prediction_FileFromDB/Inputfile.csv'


    def get(self):
        try:
            self.file = open('Prediction_Logs/data', 'w')
            csv=pd.read_csv(self.data_file)
            self.log.log(self.file,"File have been sent")
            self.file.close()
            return csv
        except Exception as e:
            self.file = open('Prediction_Logs/data', 'w')
            self.log.log(self.file,str(e))
            self.file.close()


