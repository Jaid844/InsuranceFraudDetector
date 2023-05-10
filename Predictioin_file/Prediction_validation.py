from log.applogger import Applogger
from os import listdir
import os
import json
import shutil
import pandas as pd
from datetime import datetime
import re


class prediction_validation:

    def __init__(self,path):
        self.log=Applogger()
        self.batch=path
        self.schema='schema_prediction.json'


    def valuesfrom_schema(self):
        try:
            self.file=open('Prediction_log/Validation','w')
            self.log.log(self.file,"entred into values section of file log")
            with open(self.schema,'r') as f:
                dic=json.load(f)
            pattern=dic['SampleFileName']
            Length_of_time=dic['LengthOfTimeStampInFile']
            length_of_date=dic['LengthOfDateStampInFile']
            col_name=dic['ColName']
            num_col=dic['NumberofColumns']
            message="Length_of_time is %s "%Length_of_time+ '/t'+"Length if date is %s "%length_of_date+ '/t'+ "Column number  is  %s" %num_col +'/n'
            self.log.log(self.file,message)
            self.file.close()


            return Length_of_time,length_of_date,num_col,col_name
        except Exception as e:
            self.file=open('Prediction_log/Validation','w')
            self.log.log(self.file,str(e))





    def manualReg(self):
        regex="['fraudDetection']+['\_'']+[\d_]+[\d]+\.csv"
        return regex


    def crategood_bad_dir(self):

       try:
           PATH=os.path.join("Prediction_Raw_Files_Validated/", "Good_Raw/")
           if not os.path.isdir(PATH):
               os.makedirs(PATH)

           path=os.path.join("Prediction_Raw_Files_Validated/", "BAD_Raw/")
           if not os.path.isdir(path):
               os.makedirs(path)
           self.file = open('Prediction_log/Validation', 'w')
           self.log.log(self.file,"Good and Bad Directory created")
           self.file.close()


       except Exception as e:
           self.file = open('Prediction_log/Validation', 'w')
           self.log.log(self.file,str(e))
           self.file.close()



    def delete_goodfolder(self):
        try:
            path = 'Prediction_Raw_Files_Validated/'
            if os.path.isdir(path+'Good_Raw/'):
                shutil.rmtree(path+'Good_Raw/')
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.log.log(file,"Good raw folder has been deleted")
            file.close()

        except Exception as e:
            self.file=open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.log.log(self.file,str(e))



    def delete_badfolder(self):
        try:
            path='Prediction_Raw_Files_Validated/'
            if os.path.isdir(path+'Bad_Raw/'):
                shutil.rmtree(path+'Bad_Raw/')
            self.file=open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.log.log(self.file,"Bad raw has been deleted")
        except Exception as e:
            self.file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.log.log(self.file,str(e))



    def move_archive(self):
        now=datetime.now()
        time=now.strftime("%H%T%M")
        date=now.date()

        try:
            path="PredictionArchivedBadData"
            if not os.path.isdir(path):
                os.makedirs(path)
            source = 'Prediction_Raw_Files_Validated/Bad_Raw/'
            dest='PredictionArchivedBadData/BadData_'+str(time)+"_"+str(date)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            files=os.listdir(source)
            for f in files:
                if f not in os.listdir(dest):
                    shutil.move(source+f,dest)
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.log.log(file, "Bad files moved to archive")
            PATH='Prediction_Raw_Files_Validated/'
            if os.path.isdir(PATH+'Bad_Raw/'):
                shutil.rmtree(PATH+'Bad_Raw/')
            self.log.log(file, "Bad Raw Data Folder Deleted successfully!!")
            file.close()

        except Exception as e:
            self.file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.log.log(self.file,str(e))
            self.file.close()




    def validatefilename(self,regex,length_time,length_date):
        self.delete_goodfolder()
        self.delete_badfolder()
        self.crategood_bad_dir()
        files=[ f for f in listdir(self.batch) ]
        try:
            self.file = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            for f in files:
                if (re.match(regex,f)):
                    splitatdot=re.split('.csv',f)
                    splitatdot=re.split('_',splitatdot[0])
                    if len(splitatdot[1])==length_date:
                        if len(splitatdot[2])==length_time:
                            shutil.copy("Prediction_Batch_files/"+f,"Prediction_Raw_Files_Validated/Good_Raw")
                            self.log.log(self.file,"file moved to good directory")
                        else:
                             shutil.copy("Prediction_Batch_files/" + f, "Prediction_Raw_Files_Validated/Bad_Raw/")
                             self.log.log(self.file, "file moved to bad directory")
                    else:
                         shutil.copy("Prediction_Batch_files/" + f, "Prediction_Raw_Files_Validated/Bad_Raw/")
                         self.log.log(self.file, "file moved to bad directory")
                else:
                     shutil.copy("Prediction_Batch_files/" + f, "Prediction_Raw_Files_Validated/Bad_Raw/")
                     self.log.log(self.file, "file moved to bad directory")
            self.file.close()

        except Exception as e:
            self.file = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            self.log.log(self.file,str(e))




    def valiadtecolumn(self,col):
        try:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Column Length Validation Started!!")
            for file in listdir("Prediction_Raw_Files_Validated/Good_Raw/"):
                  csv=pd.read_csv("Prediction_Raw_Files_Validated/Good_Raw/"+ file)
                  if csv.shape[1]==col:
                     csv.to_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file,header=True,index=None)
                  else:
                       shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + file, "Prediction_Raw_Files_Validated/Bad_Raw")
                       self.log.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)

            f.close()
        except Exception as e:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.log.log(f,str(e))
            f.close()


    def missingvalue(self):
        try:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.log.log(f, "Missing Values Validation Started!!")

            for file in listdir('Prediction_Raw_Files_Validated/Good_Raw/'):
                csv=pd.read_csv("Prediction_Raw_Files_Validated/Good_Raw/"+ file)
                count=0
                for col in csv:
                   if (len(csv[col]) - csv[col].count()) == len(csv[col]):
                       count += 1
                       shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + file,
                                    "Prediction_Raw_Files_Validated/Bad_Raw")
                       self.log.log(f,"File moved to bad_raw")
                       break
                if count==0:
                    csv.to_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file, index=None, header=True)

        except Exception as e:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.log.log(f,str(e))
            f.close()

    def deletePredictionFile(self):

        if os.path.exists('Prediction_Output_File/Predictions.csv'):
            os.remove('Prediction_Output_File/Predictions.csv')


































