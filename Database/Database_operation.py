import sqlite3
from os import listdir
import os
import csv
from log.applogger import Applogger
from raw.Rawfile import rawdata

class Db:
    def __init__(self):
        self.path='Training_Database/'
        self.good="Training_file/Good_file/"
        self.bad="Trainingfile/Bad_file/"
        self.log=Applogger()
        self.raw=rawdata("Training_file\Good_file")


    def connection(self,database):
        try:
            db=sqlite3.connect(self.path+database+".db")
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.log.log(file, "Opened %s database successfully" % database)
            file.close()
        except Exception as e:
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.log.log(file, str(e))
            file.close()


    def createtabledb(self,database,column):
        try:
             db=sqlite3.connect(database)  #db=self.connection(database)
             c=db.cursor()
             c.execute("SELECT count(name)  FROM sqlite_master WHERE type = 'table' AND name ='Good_Raw_Data'")
             if c.fetchone()[0]==1:
                 db.close()
                 file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
                 self.log.log(file,"Database exsit")
                 file.close()
             else:
                  for key in column.keys():
                      type=column[key]
                      try:
                          db.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{COLIUMN_NAME}" {typedata}'.format(COLIUMN_NAME=key,typedata=type))
                      except:
                             db.execute('CREATE TABLE Good_Raw_Data ({NAME}  {TYPEDATA})'.format(NAME=key,TYPEDATA=type))
                  db.close()
                  file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
                  self.log.log(file, "Tables created successfully!!")
                  file.close()
        except Exception as e:
            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.log.log(file, str(e))
            file.close()


    def insertintodb(self,database):
        db=sqlite3.connect(database) #db=self.connection(database)
        file=[f for f in listdir(self.good)]
        c=db.cursor()
        log_file = open("Training_Logs/DbInsertLog.txt", 'a+')
        for files in file:
            try:
                with open(self.good+'/'+files,'r') as f :
                    next(f)
                    reader=csv.reader(f, delimiter="\n")
                    for list in enumerate(reader):
                        for l in list[1]:
                            try:
                                c.execute("INSERT INTO Good_Raw_Data values ({value})".format(value=l))
                                self.log.log(log_file, " %s: File loaded successfully!!" % files)
                                db.commit()
                            except Exception as e:
                                raise e
            except Exception as e :
                #db.rollback()
                self.log.log(log_file, str(e))
                log_file.close()
                db.close()
        db.close()
        log_file.close()


    def selectingdatafromdb(self,database):
        self.filename="Inputfile.csv"
        self.filefromdb="FileDB"
        log_file=open("Training_Logs/ExportToCsv.txt", 'a+')
        try:
            db=sqlite3.connect(database)#db=self.connection(database)
            query='SELECT * FROM Good_Raw_Data'
            CUR=db.cursor()
            CUR.execute(query)
            results=CUR.fetchall()
            header=[i[0] for i in CUR.description]
            if not os.path.isdir(self.filefromdb):
                os.makedirs(self.filefromdb)
            file=open('Trainingfiledb/Inputfile.csv','w',newline='')
            csvfile=csv.writer(file,delimiter=',',lineterminator='\r\n',quoting=csv.QUOTE_ALL,escapechar='\\')

            csvfile.writerow(header)
            csvfile.writerows(results)

            self.log.log(log_file, "File exported successfully!!!")
            log_file.close()
        except Exception as e:
            self.log.log(log_file, "File exporting failed. Error : %s" % e)
            log_file.close()




