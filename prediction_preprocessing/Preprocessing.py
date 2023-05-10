from log.applogger import Applogger
import pandas as pd
from os import listdir


class transform:
       def __init__(self):
           self.log=Applogger()
           self.goodDataPath = "Prediction_Raw_Files_Validated/Good_Raw"


       def replace_value(self):
           try:
                log_file = open("Prediction_Logs/dataTransformLog.txt", 'a+')
                files=[f for f in listdir(self.goodDataPath)]
                for f in files:
                    data=pd.read_csv(self.goodDataPath+'/'+f)
                    columns = ["policy_bind_date", "policy_state", "policy_csl", "insured_sex", "insured_education_level",
                               "insured_occupation", "insured_hobbies", "insured_relationship", "incident_state",
                               "incident_date", "incident_type", "collision_type", "incident_severity",
                               "authorities_contacted", "incident_city", "incident_location", "property_damage",
                               "police_report_available", "auto_make", "auto_model"]
                    for col in columns:
                        data[col]=data[col].apply(lambda x: "'" + str(x) + "'")
                    data.to_csv(self.goodDataPath+"/"+f,index=None,header=True)
                    self.log.log(log_file, " %s: File Transformed successfully!!" % f)
                    log_file.close()

           except Exception as e:
               self.log.log(log_file, "Data Transformation failed because:: %s" % e)
               # log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "Data Transformation failed because:: %s" % e + "\n")
               log_file.close()
               raise e














