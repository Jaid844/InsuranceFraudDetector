import pickle
import os
import shutil
from log.applogger import Applogger

class file_op:
    def __init__(self):
        self.model='models/'
        self.log=Applogger()
        self.file = open("Training_Logs/cluser", 'w')



    def save_model(self,model, filename):
        try:
            self.file = open("Training_Logs/cluser", 'w')
            self.log.log(self.file, 'Entered the save_model method of the File_Operation class')
            path=os.path.join(self.model,filename)
            if os.path.isdir(path):
                shutil.rmtree(path)
                os.makedirs(path)
            else:
                os.makedirs(path)
            with open(path+'/'+filename+'.sav','wb') as f:
                pickle.dump(model,f)
            self.file.close()
        except Exception as e:
            self.log.log(self.file,str(e))
            self.file.close()


    def load_model(self,filename):
        self.file = open("Training_Logs/cluser", 'w')
        self.log.log(self.file, 'Entered the load_model method of the File_Operation class')
        try:
            with open(self.model + filename + '/' + filename + '.sav',
                      'rb') as f:
                self.log.log(self.file,
                                       'Model File ' + filename + ' loaded. Exited the load_model method of the Model_Finder class')
                return pickle.load(f)
        except Exception as e:
            self.file = open("Training_Logs/cluser", 'w')
            self.log.log(self.file,
                                   'Exception occured in load_model method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.log.log(self.file,
                                   'Model File ' + filename + ' could not be saved. Exited the load_model method of the Model_Finder class')
            raise Exception()


    def find_correct_model(self,cluster_number):
        self.file_object = open("Training_Logs/cluser", 'w')
        self.log.log(self.file_object, 'Entered the find_correct_model_file method of the File_Operation class')
        try:
            self.cluster_number = cluster_number
            self.folder_name = self.model
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)
            for self.file in self.list_of_files:
                try:
                    if (self.file.index(str(self.cluster_number)) != -1):
                        self.model_name = self.file
                except:
                    continue
            self.model_name = self.model_name.split('.')[0]
            self.log.log(self.file_object,
                                   'Exited the find_correct_model_file method of the Model_Finder class.')
            return self.model_name
        except Exception as e:
            self.file_object = open("Training_Logs/cluser", 'w')
            self.log.log(self.file_object,
                                   'Exception occured in find_correct_model_file method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.log.log(self.file_object,
                                   'Exited the find_correct_model_file method of the Model_Finder class with Failure')
            raise Exception()
