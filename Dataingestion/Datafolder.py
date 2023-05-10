from log.applogger import Applogger
import pandas as pd

class Data:
    """This class  will be able to give data in csv form obtained from database"""

    def __init__(self):
        self.trainingfile="Trainingfiledb/Inputfile.csv"
        self.log=Applogger()


    def datgetter(self):
        self.file = open('Training_Logs/data', 'w')
        try:
            self.file = open('Training_Logs/data', 'w')
            csv=pd.read_csv(self.trainingfile)
            self.log.log(self.file,"Data sent")
            self.file.close()
            return csv

        except Exception as e:
            self.file = open('Training_Logs/data','w')
            self.log.log(str(e))
            self.file.close()
