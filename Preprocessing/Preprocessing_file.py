from log.applogger import Applogger
import pandas as pd

from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from Dataingestion.Datafolder import Data
import csv

class preprocess:
    def __init__(self):
        self.log=Applogger()
        self.data=Data()


    def removecolumn(self,data,column):
        self.column=column
        self.data=data
        try:
            self.file = open('Training_Logs/Preprocess', 'w')
            usefuldata=self.data.drop(self.column,axis=1)
            self.log.log(self.file,"Column have been removed")
            self.file.close()
            return  usefuldata
        except Exception as e:
            self.file = open('Training_Logs/Preprocess', 'w')
            self.log.log(self.file,str(e))
            self.file.close()


    def sepratelabelandfeature(self,data,columns):
        self.data=data
        self.column=columns
        try:
            self.file = open('Training_Logs/Preprocess', 'w')
            self.X=self.data.drop(labels=self.column,axis=1)
            self.Y=self.data[self.column]
            self.log.log(self.file,"label and fetaure have been sent")
            self.file.close()
            return  self.X,self.Y
        except Exception as e:
            self.file = open('Training_Logs/Preprocess', 'w')
            self.log.log(self.file,str(e))
            self.file.close()


    def imputemissingvalue(self,data,col):
        self.data=data
        self.col=col
        try:
            self.file = open('Training_Logs/Preprocess', 'w')
           # imputer=SimpleImputer(strategy="most_frequent")
            for i in self.col:
                self.data[i].fillna(self.data[i].mode()[0], inplace=True)
            self.log.log(self.file,"Encoded succesfulyy")
            self.file.close()
            return self.data
        except Exception as e:
            self.file = open('Training_Logs/Preprocess', 'w')
            self.log.log(self.file,str(e))
            self.file.close()


    def null_present(self,data):
        self.data=data
        self.columns_with_missing_value=[]
        self.state=False
        self.cols=data.columns
        try:
             self.file = open('Training_Logs/Preprocess', 'w')
             self.null_count=self.data.isna().sum()
             for i in range(len(self.null_count)):
                 if self.null_count[i]>0:
                     self.state=True
                     self.columns_with_missing_value.append(self.cols[i])
             self.log.log(self.file,"Sent the file of missing value , and the state")
             self.file.close()
             return self.state,self.columns_with_missing_value
        except Exception as e:
            self.file = open('Training_Logs/Preprocess', 'w')
            self.log.log(self.file,str(e))
            self.file.close()

    def encode_cat(self,data):
        self.data= data
        try:
            self.file = open('Training_Logs/Preprocess', 'w')
            self.log.log(self.file,"endoding the data to numerical form categrocial")
            self.cat_df=self.data.select_dtypes(include=["object"]).copy()
            self.cat_df['policy_csl']=self.cat_df['policy_csl'].map({'250/500':2.5,'100/300':1,'500/1000':5})
            self.cat_df['insured_sex']=self.cat_df['insured_sex'].map({'MALE':1,'FEMALE':0})
            self.cat_df['insured_education_level']=self.cat_df['insured_education_level'].map({'JD':1,'High School':2,'College':3,'Masters':4,'Associate':5,'MD':6,'PhD':7})
            self.cat_df['property_damage']=self.cat_df['property_damage'].map({'NO' : 0, 'YES' : 1})
            self.cat_df['police_report_available']=self.cat_df['police_report_available'].map({'YES':1,'NO':0})
            #self.cat_df['fraud_reported']=self.cat_df['fraud_reported'].map({'YES':1,'NO':0})
            try:#block for training
                self.cat_df['fraud_reported'] = self.cat_df['fraud_reported'].map({'N': 0, 'Y': 1})
                cols_to_drop=['policy_csl','insured_sex','insured_education_level','property_damage','police_report_available','fraud_reported']
            except:#block for prediction
                cols_to_drop=['policy_csl','insured_sex','insured_education_level','property_damage','police_report_available']
            for col in self.cat_df.drop(columns=cols_to_drop).columns:
                self.cat_df=pd.get_dummies(self.cat_df,columns=[col],drop_first=True,prefix=col,dtype=int)
            self.data.drop(columns=self.data.select_dtypes(include=['object']).columns,inplace=True)
            self.data=pd.concat([self.cat_df,self.data],axis=1)
            self.log.log(self.file,"encoditon complete")
            self.file.close()
            return self.data
        except Exception as e:
            self.file=open('Training_Logs/Preprocess','w')
            self.log.log(self.file,"errror %s"%e)
            self.file.close()


    def scale_numerical(self,data):
        self.data=data
        self.numerical=self.data[['months_as_customer','policy_deductable','umbrella_limit','capital-gains','capital-loss','incident_hour_of_the_day'
                                  ,'number_of_vehicles_involved', 'bodily_injuries', 'witnesses', 'injury_claim',
                                  'property_claim']]
        try:
            self.file = open('Training_Logs/Preprocess', 'w')
            scaler=StandardScaler()
            self.scaled_data=scaler.fit_transform(self.numerical)
            self.scaled_num_df=pd.DataFrame(data=self.scaled_data,columns=self.numerical.columns,index=self.data.index)
            self.data.drop(columns=self.scaled_num_df.columns,inplace=True)
            self.data=pd.concat([self.scaled_num_df,self.data],axis=1)
            self.log.log(self.file,
                               'scaling for numerical values successful. Exited the scale_numerical_columns method of the Preprocessor')
            self.file.close()
            return self.data
        except Exception as e:
            self.file = open('Training_Logs/Preprocess', 'w')
            self.log.log(self.file,str(e))
            self.file.close()




#e=Applogger()
#c=preprocess()
#d=Data()
#file=d.datgetter()
#useful_data=c.removecolumn(file,['policy_number','policy_bind_date','policy_state','insured_zip','incident_location','incident_date','incident_state','incident_city','insured_hobbies','auto_make','auto_model','auto_year','age','total_claim_amount'])
#useful_data.replace('?',np.nan,inplace=True)
#columns,state=c.null_present(useful_data)
#if state:
#    useful_data=c.imputemissingvalue(useful_data,columns)
#useful_data=c.encode_cat(useful_data)
#X,Y=c.sepratelabelandfeature(useful_data,columns='fraud_reported')
#print(X)
