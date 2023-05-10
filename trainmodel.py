from log.applogger import Applogger
from Dataingestion.Datafolder import Data
from Preprocessing.Preprocessing_file import preprocess
from Preprocessing.clustering import Cluster
from best_mode_finder.tuner import model_finder
from file_operation.file_operation import file_op
from sklearn.model_selection import  train_test_split
import numpy as np


class training:
    def __init__(self):
        self.data=Data()
        self.preporocess=preprocess()
        self.file_op=file_op()
        self.model=model_finder()
        self.cluster=Cluster()
        self.log=Applogger()



    def train(self):
        try:
            self.file_object=open('Training_Logs/Training_Main_Log.txt','a+')
            data = self.data.datgetter()

            """doing the data preprocessing"""

            data = self.preporocess.removecolumn(data, ['policy_number', 'policy_bind_date', 'policy_state', 'insured_zip',
                                                      'incident_location', 'incident_date', 'incident_state',
                                                      'incident_city', 'insured_hobbies', 'auto_make', 'auto_model',
                                                      'auto_year', 'age',
                                                      'total_claim_amount'])  # remove the column as it doesn't contribute to prediction.
            data.replace('?', np.NaN, inplace=True)  # replacing '?' with NaN values for imputation

            # check if missing values are present in the dataset
            is_null_present, cols_with_missing_values = self.preporocess.null_present(data)

            # if missing values are there, replace them appropriately.
            if (is_null_present):
                data = self.preporocess.imputemissingvalue(data, cols_with_missing_values)  # missing value imputation
        # encode categorical data
            data = self.preporocess.encode_cat(data)

            # create separate features and labels
            X, Y = self.preporocess.sepratelabelandfeature(data, columns='fraud_reported')

            """ Applying the clustering approach"""


            number_of_clusters = self.cluster.elbowplot(X)  # using the elbow plot to find the number of optimum clusters

            # Divide the data into clusters
            X = self.cluster.create_cluster(X, number_of_clusters)

            # create a new column in the dataset consisting of the corresponding cluster assignments.
            X['Labels'] = Y

            # getting the unique clusters from our dataset
            list_of_clusters = X['Cluster'].unique()

            """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""

            for i in list_of_clusters:
                cluster_data = X[X['Cluster'] == i]  # filter the data for one cluster

                # Prepare the feature and Label columns
                cluster_features = cluster_data.drop(['Labels', 'Cluster'], axis=1)
                cluster_label = cluster_data['Labels']

                # splitting the data into training and test set for each cluster one by one
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1 / 3,
                                                                    random_state=355)
                # Proceeding with more data pre-processing steps
                x_train = self.preporocess.scale_numerical(x_train)
                x_test = self.preporocess.scale_numerical(x_test)

                # getting the best model for each of the clusters
                best_model_name, best_model = self.model.get_best_model(x_train, y_train, x_test, y_test)

            # saving the best model to the directory.
                save_model = self.file_op.save_model(best_model, best_model_name + str(i))
            self.log.log(self.file_object, 'Successful End of Training')
            self.file_object.close()

        except Exception as e:
        # logging the unsuccessful Training
            self.log.log(self.file_object, 'Unsuccessful End of Training')
            self.file_object.close()
            raise Exception


# finally:
            #self.file.close()


#c=training()
#c.train()












