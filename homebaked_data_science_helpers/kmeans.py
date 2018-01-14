"""module to perform kmeans clustering"""

import numpy as np
from scipy.stats import rv_discrete

from .helpers import euclidian_distance, Clustering

class KMeans (Clustering):
    """ kmeans clustering class """


    def __init__(self, nb_clust, init_method = "standard") :
        """initialisation method"""

        #testing the validity of the arguments
        try :
            nb_clust = np.int16(nb_clust)
            assert nb_clust > 0, "number of cluster must be more than zero"
            self.nb_clust = nb_clust
        except ValueError:
            raise AssertionError("nb_clust must be an integer")

        assert init_method in ["standard", "plus"], "initialisation method must be either 'standard' or 'plus'"
        self.init_method = init_method


    def fit (self, fit_data):
        """fitting method that performs the clustering itself"""
        #verfing the validity of data
        super().fit(fit_data)
        assert self.nb_clust <= self.data.shape[0], "the number of clusters predefined is larger than the number of individuals in the data"

        #initialisation of the clusters
        self.centroids = []
        if self.init_method == "standard":
            temp_data = self.data
            for clust_i in range(0, self.nb_clust):
                row_i = np.random.randint(0, temp_data.shape[0])
                self.centroids.append(temp_data[row_i,:])
                temp_data = np.delete(temp_data, row_i, axis = 0)
        if self.init_method == "plus":
            temp_data = self.data
            first = np.random.randint(0, self.data.shape[0])
            self.centroids.append(temp_data[first,:])
            temp_data = np.delete(temp_data, first, axis = 0)
            for clust_i in range(0, self.nb_clust -1):
                distances = np.apply_along_axis(self._find_distance_closesed_centroid, 1, temp_data, self.centroids)
                sum_distances = np.sum(distances)
                proba = distances / sum_distances
                row = rv_discrete(values=(list(range(0,temp_data.shape[0])),proba)).rvs()
                self.centroids.append(temp_data[row,:])
                temp_data = np.delete(temp_data, row, axis = 0)

        #iterative process
        self.points_cluster = np.apply_along_axis(self._find_closesed_centroid, 1, self.data, self.centroids).tolist()
        while True :
            self.clusters = [[] for i in range(0, self.nb_clust)]

            for i, cluster in enumerate(self.points_cluster):
                self.clusters[cluster].append(self.data[i,:])

            for i, cluster in enumerate(self.clusters):
                cluster = np.array(cluster)
                cluster = cluster.reshape(-1, cluster.shape[-1])
                self.centroids[i] = self._centroid(cluster)

            new_points_cluster = np.apply_along_axis(self._find_closesed_centroid, 1, self.data, self.centroids).tolist()

            if self.points_cluster == new_points_cluster:
                break
            self.points_cluster = new_points_cluster
        #cleaning self.clusters
        for i in range(0,len(self.clusters)):
            self.clusters[i] = np.array(self.clusters[i])



    @staticmethod
    def _find_closesed_centroid(point, centroids):
        """method that finds the closesed centroid to a certain point """

        distances = []
        for centroid in centroids :
            distance = euclidian_distance(point , centroid)
            distances.append(distance)
        return np.argmin(distances)


    @staticmethod
    def _find_distance_closesed_centroid(point, centroids):
        """method that finds the closesed centroid to a certain point """

        distances = []
        for centroid in centroids :
            distance = euclidian_distance(point , centroid)
            distances.append(distance)
        return np.min(distances)
