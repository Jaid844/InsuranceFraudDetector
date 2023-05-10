from os import listdir
import pandas as pd
from log.applogger import Applogger

class transoform:
    """This class transoforms the column into double strings in the categorical section
    """
    def __init__(self):
        self.goodfile='Training_file/Good_file'
        self.log=Applogger()


    def datatrasform(self):
        log_file = open("Training_Logs/dataTransformLog.txt", 'a+')
        try:
            file=[f for f in listdir(self.goodfile)]
            for t in file:
                data=pd.read_csv(self.goodfile+'/'+t)
                columns=["policy_state","policy_csl","insured_sex","insured_education_level","insured_occupation","insured_hobbies","insured_relationship","incident_state","incident_date","incident_type","collision_type","incident_severity","authorities_contacted","incident_city","incident_location","property_damage","police_report_available","auto_make","auto_model","fraud_reported"]
                for col in columns:
                    data[col]=data[col].apply(lambda x :"'"  +str(x)+ "'")
                data.to_csv(self.goodfile + "/" + t, index=None, header=True)
                self.log.log(log_file,"transoformed")
        except Exception as e:
            self.log.log(log_file, str(e))
            log_file.close()
        log_file.close()



#c=transoform()
#c.datatrasform()
