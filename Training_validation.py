from log.applogger import Applogger
from DataTransofrmation.DataTranform import transoform
from raw.Rawfile import rawdata
from Database.Database_operation import Db


class training_val:
    def __init__(self,path):
        self.log=Applogger()
        self.transform=transoform()
        self.rawdata=rawdata(path)
        self.database=Db()
        #self.file_object = open("Training_Logs/Training_Main_Log.txt", 'a+')


    def val(self):
        try:
            self.file_object = open("Training_Logs/Training_Main_Log.txt", 'a+')
            self.log.log(self.file_object, 'Start of Validation on files for Training!!')
            regex=self.rawdata.manualregeex()
            length_of_time,length_date,no_ofcol,col_name=self.rawdata.valuesfromschem()
            self.rawdata.validfilename(regex,length_date,length_of_time)
            self.rawdata.valdcolumn(no_ofcol)
            self.rawdata.misssingvalue()
            self.log.log(self.file_object,"raw file validated")

            self.log.log(self.file_object, "Starting Data Transforamtion!!")
            self.transform.datatrasform()

            self.log.log(self.file_object, "DataTransformation Completed!!!")

            self.log.log(self.file_object,
                            "Creating Training_Database and tables on the basis of given schema!!!")
            self.database.createtabledb("Training",col_name)#database
            self.log.log(self.file_object, "Table creation Completed!!")
            self.log.log(self.file_object, "Insertion of Data into Table started!!!!")
            self.database.insertintodb("Training")#database
            self.log.log(self.file_object, "Insertion in Table completed!!!")
            self.log.log(self.file_object, "Deleting Good Data Folder!!!")
            self.rawdata.deletegoddir()
            self.log.log(self.file_object, "Good_Data folder deleted!!!")
            self.log.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            self.rawdata.movefiletoarchieve()
            self.log.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log.log(self.file_object, "Validation Operation completed!!")
            self.log.log(self.file_object, "Extracting csv file from table")
            self.database.selectingdatafromdb("Training")#database
            self.file_object.close()

        except Exception as e:
            self.file_object = open("Training_Logs/Training_Main_Log.txt", 'a+')
            self.log.log(self.file_object,"error %s"%e)

