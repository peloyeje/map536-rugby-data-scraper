"""module that provides functions and classes to be used internaly by the package"""

from abc import ABC, abstractmethod

import numpy as np



def euclidian_distance(point_A, point_B):
    assert len(point_A) == len(point_B), "points must be of same dimension"

    sum_square = np.float32()
    for dimension in range(0,len(point_A)) :
        point_A_i = np.float32(point_A[dimension])
        point_B_i = np.float32(point_B[dimension])
        sum_square += np.square(point_A_i - point_B_i)
    return np.sqrt(sum_square)


def standardize(std_data, method = "n", greedy = False):
    """method to scale and standardize data.
     beware, no validity check of the data is done in this function
     """

    assert method in ["n", "n-1"], "standardisation method must be either n or n-1"

    if not greedy:
        data = np.copy(std_data)
    else:
        data = std_data

    for col_i in range(data.shape[1]):
        col = data[:,col_i]
        #retrieving column statstics
        col_mean = np.nanmean(col)
        if method == "n":
            col_std_dev = np.nanstd(col)
        else:
            col_std_dev = np.sqrt(np.nansum((col - col_mean) ** 2 )/ (data.shape[0] - 1))
        #standardizing the data
        if col_std_dev != 0:
            col = (col - col_mean) / col_std_dev
            data[:,col_i] = col
        else:
            data[:,col_i] = col -col_mean

    return data

def bootstrap(bootstrap_data, prop = 0.8, which = "ind") :
    """function that generates  a boostraped dataset
    beware, no validity check of the data is done in this function
    """
    assert which in ["ind", "var"], "can only bootstrap varaibles or individuals"
    if which == "var":
        cols =  np.random.randint(0,bootstrap_data.shape[1], np.int32(np.floor(prop * bootstrap_data.shape[1])))
        return (bootstrap_data[:, cols], cols)
    else:
        lines = np.random.randint(0,bootstrap_data.shape[0], np.int32(np.floor(prop * bootstrap_data.shape[0])))
        return (bootstrap_data[lines,:], lines)



class Clustering (ABC) :
    """abstract class for all clustering classes (so far kmeans)"""

    @abstractmethod
    def __init__(self):
        """initialisation of the classifire"""


    @abstractmethod
    def fit(self, fit_data):
        """fit the classifier to the data"""

        #test the validity of the arguments
        data = np.copy(fit_data)
        data = np.array(data)
        assert len(data.shape) == 2 , "data has not a proper shape(not two dimensions)"
        assert not data.dtype.type is  np.object , "data was not well transfered as a array (not consistent shape or non numerical data)"
        self.data = data


    def inertia_analisis(self, verbuous = True):

        try :
            self.clusters
        except NameError:
            raise AssertionError("clustering must be fit before analysing inertia")
        total_inertia, data_centroid = self._inertia(self.data)
        between_inertia = 0
        within_inertia = 0
        for cluster in self.clusters:
            cluster_inertia, cluster_centroid = self._inertia(cluster)
            within_inertia += cluster_inertia
            between_inertia += cluster.shape[0] * (euclidian_distance(cluster_centroid, data_centroid)**2)
        if verbuous:
            print("total inertia")
            print(total_inertia)
            print("between_inertia")
            print(between_inertia)
            print("inertia ratio")
            print(between_inertia / total_inertia)
        else:
            return (total_inertia, within_inertia, between_inertia)


    @staticmethod
    def _inertia (data) :
        """function that calculates the inertia within a cluster and returns the inertia and the centrod of the cluster"""

        inertia = 0
        centroid = Clustering._centroid(data)
        for row_i in range(0, data.shape[0]):
            inertia += euclidian_distance(data[row_i,:], centroid)**2
        return (inertia, centroid)

    @staticmethod
    def _centroid(data):
        """function that returns the centroid of a cluster"""

        centroid = []
        for col_i in range(0, data.shape[1]):
            centroid.append(np.mean(data[:,col_i]))
        centroid = np.array(centroid)
        return centroid
