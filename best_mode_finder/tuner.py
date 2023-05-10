from log.applogger import Applogger
from sklearn.model_selection import GridSearchCV
from sklearn.svm import SVC
from sklearn.tree import  DecisionTreeClassifier
from sklearn.metrics import roc_auc_score,accuracy_score

class model_finder:
      def __init__(self):
          self.log=Applogger()
          self.svm=SVC()
          self.DT= DecisionTreeClassifier()


      def parms_for_svm(self,train_X,train_Y):
          try:
              self.file = open('Training_Logs/model_training', 'w')
              self.log.log(self.file, "entered into SVM classifer model")
              self.param_grid = {"kernel": ['rbf', 'sigmoid'],
                                 "C": [0.1, 0.5, 1.0],
                                 "random_state": [0, 100, 200, 300]}
              self.grid=GridSearchCV(estimator=self.svm,param_grid=self.param_grid,cv=5,verbose=3)
              self.grid.fit(train_X,train_Y)

              self.kernel=self.grid.best_params_['kernel']
              self.C=self.grid.best_params_['C']
              self.random_state=self.grid.best_params_['random_state']


              self.model_svm=SVC(C=self.C,kernel=self.kernel,random_state=self.random_state)
              self.model_svm.fit(train_X,train_Y)
              self.log.log(self.file,"trained the model")
              self.file.close()
              return  self.model_svm

          except Exception as e:
               self.file = open('Training_Logs/model_training', 'w')
               self.log.log(self.file,str(e))
               self.file.close()


      def param_for_decisontree(self,train_x,train_y):
          try:
              self.file = open('Training_Logs/model_training', 'w')
              self.log.log(self.file, "entered into Decission Tree classifer model")
              self.param_grid={'criterion':['gini', 'entropy', 'log_loss'],
                               'splitter':['best', 'random']}
              self.grid=GridSearchCV(estimator=self.DT,param_grid=self.param_grid,cv=5,verbose=3)
              self.grid.fit(train_x, train_y)
              self.citreion=self.grid.best_params_['criterion']
              self.splitter=self.grid.best_params_['splitter']
              self.model_DT=DecisionTreeClassifier(criterion=self.citreion,splitter=self.splitter)
              self.model_DT.fit(train_x,train_y)
              self.log.log(self.file, "trained the  Decsiion Tree model")
              self.file.close()
              return self.model_DT
          except Exception as e:
              self.file = open('Training_Logs/model_training', 'w')
              self.log.log(self.file, str(e))
              self.file.close()



      def get_best_model(self,train_x,train_y,test_x,test_y):
          try:
              self.file = open('Training_Logs/model_training', 'a+')
              self.log.log(self.file,"fidnind the best model")
              self.Dt=self.param_for_decisontree(train_x,train_y)
              prediction_of_dt=self.Dt.predict(test_x)
              if len(test_y.unique())==1:
                   self.file = open('Training_Logs/model_training', 'a+')
                   self.dt_score=accuracy_score(test_y,prediction_of_dt)
                   self.log.log(self.file,"Score of dt is "+str(self.dt_socre))
              else:
                   self.file = open('Training_Logs/model_training', 'a+')
                   self.dt_score=roc_auc_score(test_y,prediction_of_dt)
                   self.log.log(self.file,"Score of decision tree is "+str(self.dt_score))

              self.Svm=self.parms_for_svm(train_x,train_y)
              prediction_of_svm=self.Svm.predict(test_x)
              if len(test_y.unique()) == 1:
                  self.file = open('Training_Logs/model_training', 'a+')
                  self.svm_score = accuracy_score(test_y, prediction_of_svm)
                  self.log.log(self.file, "Score of SVM is " + str(self.svm_score))
              else:
                  self.file = open('Training_Logs/model_training', 'a+')
                  self.svm_score = roc_auc_score(test_y, prediction_of_svm)
                  self.log.log(self.file, "Score of SVM is " + str(self.svm_score)+ " score of Decision Tree is " + str(self.dt_score) )
              #self.file.close()

              if self.svm_score>self.dt_score:
                 return "SVM",self.Svm
              else:
                 return 'Decisiion Tree',self.Dt
              self.file.close()


          except  Exception as e:
              self.file=open('Training_Logs/model_training','w')
              self.log.log(self.file,str(e))
              self.file.close()









