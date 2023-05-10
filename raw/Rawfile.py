from log.applogger import Applogger
from os import listdir
import os
import re
import json
import pandas as pd
import shutil
import datetime
from datetime import datetime


class rawdata():
    """ this class handels missing value ,created badand gppd directory and  deltes good directory nad bad directory , moves bad file to archieve
          file"""
    def __init__(self,path):
        self.log=Applogger()
        self.path=path
        self.schemepath='schema_training.json'

    def valuesfromschem(self):
        try:
            with open(self.schemepath,'r') as f:
                dic=json.load(f)
            pattern=dic['SampleFileName']
            length_of_date=dic['LengthOfDateStampInFile']
            length_of_time=dic['LengthOfTimeStampInFile']
            no_of_col=dic['NumberofColumns']
            col_name=dic['ColName']

            file=open("Training_Logs/validation_file",'a+')
            message="length of date is::%s "%length_of_date+"\t"+"length of time is::%s "%length_of_time+'/t'+"no of column::%s"%no_of_col+'\n'
            self.log.log(file,"length of column is ",message)
        except Exception as e:
            file=open("Training_Logs/validation_file",'a+')
            self.log.log(file,str(e))

        return length_of_time,length_of_date,no_of_col,col_name



    def manualregeex(self):
        regex="['fraudDetection']+['\_'']+[\d_]+[\d]+\.csv"
        return regex



    def creategood_bad_dir(self):
       try:
           path=os.path.join("Training_file","Good_file/")
           if not os.path.isdir(path):
               os.makedirs(path)
           path=os.path.join("Trainingfile","Bad_file/")
           if not os.path.isdir(path):
               os.makedir(path)

       except Exception as e:
           file=open('Training_Logs/validation_file','a+')
           self.log.log(file,"error %s"%e)

    def deletegoddir(self):
        try:
            file=open("Training_Logs/validation_file",'a+')
            path='Training_file'
            if os.path.isdir(path,'Good_file/'):
                shutil.rmtree(path,'Good_file/')
            self.log.log(file,"good directory has been deleted")
            file.close()
        except Exception as e:
            file=open("Training_Logs/validation_file",'a+')
            self.log.log(file,str(e))


    def deletebaddire(self):
        try:
            file=open("Training_Logs/validation_file",'a+')
            path = 'Training_file'
            if os.path.isdir(path,'Bad_file/'):
                shutil.rmtree(path,'Bad_file/')
            self.log.log((file,"bad directory has been removed"))
            file.close()

        except Exception as e:
            file = open("Training_Logs/validation_file", 'a+')
            self.log.log(file,str(e))


    def movefiletoarchieve(self):
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            source='Trainingfile/Bad_file/'
            if os.path.isdir(source):
                path='Archeivefiles'
                if not os.path.isdir(path):
                    os.makedirs(path)
                path='Archeivefiles/Bad_file'+str(date)+'_'+str(time)
                if not os.path.isdir(path):
                   os.makedir(path)
                files=os.listdir(source)
                for f in files:
                   if f not in os.listdir(path):
                    shutil.move(source+f,path)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.log.log(file,"Bad files moved to archive")
                file.close()
                self.deletebaddire()
        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.log.log(file,str(e))
            file.close()


    def validfilename(self,regex,length_of_date,length_of_time):
        self.deletegoddir()
        self.deletebaddire()
        self.creategood_bad_dir()
        file_name=[f for f in listdir((self.path))]
        try:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            for t in file_name:
                if re.match(regex,t):
                    split=re.split('.csv',t)
                    split=re.split('_',split[0])
                    if len(split[1])==length_of_date:
                        if len(split[2])==length_of_time:
                            shutil.copy('Training_Batch_Files/'+t ,"Training_file/Good_file/")
                            self.log.log(f,"file moved to good _raw directory")

                        else:
                            shutil.copy('Training_Batch_Files/'+t ,"Trainingfile/Bad_file/")
                            self.log.log(f, "file moved to bad_raw directory")
                    else:
                           shutil.copy('Training_Batch_Files/' + t, "Trainingfile/Bad_file/")
                else:
                    shutil.copy('Training_Batch_Files/' + t, "Trainingfile/Bad_file/")
            f.close()

        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.log.log(f,str(e))
            f.close()




    def valdcolumn(self,col_length):
        try:
             f = open("Training_Logs/columnValidationLog.txt", 'w')
             self.log.log(f, "Column Length Validation Started!!")
             for file in listdir('Training_file/Good_file/'):
                 csvfile=pd.read_csv('Training_file/Good_file/'+file)
                 if csvfile.shape[1]==col_length:
                     pass
                 else:
                     shutil.move('Training_file/Good_file/'+file,"Trainingfile/Bad_file/")
                     self.log.log(f, "invalid column length")
             self.log.log(f, "Column Length Validated")
             f.close()
        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'w')
            self.log.log(f, str(e))
            f.close()



    def misssingvalue(self):
        f = open("Training_Logs/missingValuesInColumn.txt", 'w')
        self.log.log(f, "Missing Values Validation Started!!")
        try:
            for file in listdir("Training_file/Good_file/"):
                csv=pd.read_csv("Training_file/Good_file/"+file)
                counter=0
                for columns in csv:
                   if (len(csv[columns])-csv[columns].count())==len(csv[columns]):
                      counter+=1
                      shutil.move("Training_file/Good_file/"+file,"Trainingfile/Bad_file/")
                      self.log.log(f, "file moved to bad directory")
                      break
                   else:
                       csv.to_csv("Training_file/Good_file/"+file,header=True,index=False)
                self.log.log(f,"files have been validated")
        except Exception as e:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.log.log(f,str(e))
            f.close()






