from log.applogger import Applogger
from dboperation_predeiction.db_operation import db
from prediction_preprocessing.Preprocessing import transform
from Predictioin_file.Prediction_validation import prediction_validation


class pred:
    def __init__(self,path):
        self.raw=prediction_validation(path)
        self.log=Applogger()
        self.db=db()
        self.transform=transform()


    def predic(self):
        try:
            self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
            self.log.log(self.file_object, 'Start of Validation on files for prediction!!')
            Length_of_time, length_of_date, num_col, col_name=self.raw.valuesfrom_schema()
            regex=self.raw.manualReg()
            self.raw.validatefilename(regex,Length_of_time,length_of_date)
            self.raw.valiadtecolumn(num_col)
            self.raw.missingvalue()
            self.log.log(self.file_object, "Raw Data Validation Complete!!")
            self.log.log(self.file_object, ("Starting Data Transforamtion!!"))
            self.transform.replace_value()
            self.log.log(self.file_object, "DataTransformation Complete")
            self.log.log(self.file_object, "Creating Prediction_Database and tables on the basis of given schema!!!")
            self.db.createtabledb('Prediction',col_name)
            self.log.log(self.file_object, "Table creation Completed!!")
            self.log.log(self.file_object, "Insertion of Data into Table started!!!!")
            self.db.insert_into_db('Prediction')
            self.log.log(self.file_object, "Insertion in Table completed!!!")
            self.log.log(self.file_object, "Deleting Good Data Folder!!!")
            self.raw.delete_goodfolder()
            self.log.log(self.file_object, "Good_Data folder deleted!!!")
            self.log.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            self.raw.move_archive()
            self.log.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log.log(self.file_object, "Validation Operation completed!!")
            self.log.log(self.file_object, "Extracting csv file from table")
            self.db.load_into_csv('Prediction')
            self.file_object.close()


        except Exception as e:
            self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
            self.log.log(self.file_object,str(e))
            self.file_object.close()




#c=pred(r'C:\Users\91639\Desktop\insurancefraud\Prediction_Batch_files')
#c.predic()



















