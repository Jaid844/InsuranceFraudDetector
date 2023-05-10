import sqlite3
from log.applogger import Applogger
from os import  listdir
import os
import csv


class db:

     def __init__(self):
          self.path = 'Prediction_Database/'
          self.good = "Prediction_Raw_Files_Validated/Good_Raw"
          self.bad = "Prediction_Raw_Files_Validated/Bad_Raw"
          self.log=Applogger()

     def connection(self, database):
          try:
               db = sqlite3.connect(self.path + database + ".db")
               file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
               self.log.log(file, "Opened %s database successfully" % database)
               file.close()
          except Exception as e:
               file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
               self.log.log(file, str(e))
               file.close()


     def createtabledb(self, database, column):
        try:
             db = sqlite3.connect(database)  # db=self.connection(database)
             db.execute('DROP TABLE IF EXISTS Good_Raw_Data;')
             for key in column.keys():
                  type = column[key]
                  try:
                       db.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{COLIUMN_NAME}" {typedata}'.format(
                            COLIUMN_NAME=key, typedata=type))
                  except:
                    db.execute('CREATE TABLE Good_Raw_Data ({NAME}  {TYPEDATA})'.format(NAME=key, TYPEDATA=type))
             db.close()
             file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
             self.log.log(file, "Tables created successfully!!")
             file.close()

        except Exception as e:
             file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
             self.log.log(file, str(e))
             file.close()

     def insert_into_db(self,database):
          self.db=sqlite3.connect(database)
          files=[f for f in listdir(self.good)]
          log_file = open("Prediction_Logs/DbInsertLog.txt", 'a')
          for file in files:
               try:
                    with open(self.good+'/'+file) as f:
                         next(f)
                         csv_file=csv.reader(f,delimiter="\n")
                         for line in enumerate(csv_file):
                              for list_ in line[1]:
                                   try:
                                        self.db.execute('INSERT INTO Good_Raw_Data values ({VALUES})'.format(VALUES=(list_)))
                                        self.log.log(log_file,"file have been loaded suceesfully %s"%file)
                                        self.db.commit()
                                   except Exception as e:
                                        raise e
               except Exception as e:
                    self.log.log(log_file, str(e))
                    log_file.close()
                    self.db.close()




     def load_into_csv(self,database):
          self.fileFromDb = 'Prediction_FileFromDB/'
          self.fileName = 'InputFile.csv'
          log_file = open("Prediction_Logs/ExportToCsv.txt", 'a+')
          try:
               self.db=sqlite3.connect(database)
               command="SELECT * FROM Good_Raw_Data"
               cursor=self.db.cursor()
               cursor.execute(command)
               result=cursor.fetchall()
               headers = [i[0] for i in cursor.description]
               if not os.path.isdir(self.fileFromDb):
                    os.makedirs(self.fileFromDb)
               file = open('Prediction_FileFromDB/Inputfile.csv', 'w', newline='')
               csvfile = csv.writer(file, delimiter=',', lineterminator='\r\n', quoting=csv.QUOTE_ALL, escapechar='\\')
               csvfile.writerow(headers)
               csvfile.writerows(result)
               self.log.log(log_file, "File exported successfully!!!")

          except Exception as e:
            self.log.log(log_file, "File exporting failed. Error : %s" % e)
            raise e



























