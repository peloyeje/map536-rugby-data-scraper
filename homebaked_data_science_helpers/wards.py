"""module to perform hierarchical clustering using wards algorythm"""

import numpy as np

from .helpers import Clustering, euclidian_distance


class Wards (Clustering):
    """wards hierircital clustering algorythm"""

    def __init__(self, nb_clust):
        """initilisation of the wards clustering"""

        try :
            nb_clust = np.int16(nb_clust)
            assert nb_clust > 0 or nb_clust == -1, "number of cluster must be more than zero or -1"
            self.nb_clust = nb_clust
        except ValueError:
            raise AssertionError("nb_clust must be an integer")


    def fit(self, data):
        """fiting to data"""

        #testing validity of the data
        if not issubclass(type(data), Clustering):
            super().fit(data)
        else:
            self.data = np.copy(data.data)
        #initilizing clusters
        if issubclass(type(data), Clustering):
            self.clusters = data.clusters
        else:
            self.clusters = []
            for row_i in range(0, self.data.shape[0]):
                self.clusters.append(self.data[row_i,:].reshape(1,-1))

        #variable init for automatic selection
        if self.nb_clust == -1:
            inertias = [np.inf, np.inf, self.inertia_analisis(False)[2]]
            new_second_derivative = 0
        #performing wards
        while True:
            if len(self.clusters) <= self.nb_clust:
                break
            #calculating distances
            dist = []
            for i, cluster_i in enumerate(self.clusters):
                row = []
                for j, cluster_j in enumerate(self.clusters):
                    if i >= j:
                        row.append(np.inf)
                    else:
                        row.append(self.wards_clusters_distance(cluster_i, cluster_j))
                dist.append(row)
            dist = np.matrix(dist)
            #finding two closest clusters
            low = np.argmin(dist)
            low_i = low // dist.shape[1]
            low_j = low % dist.shape[1]
            old_clustering = self.clusters
            self.clusters[low_i] = np.append(self.clusters[low_i], self.clusters.pop(low_j).reshape(-1,self.clusters[low_i].shape[-1]), axis = 0)

            #automatic stop
            if self.nb_clust == -1 :
                new_between = self.inertia_analisis(False)[2]
                inertias = [inertias[1], inertias[2], new_between]
                second_derivative = new_second_derivative
                new_second_derivative = inertias[0] + inertias[2] - 2 * inertias[1]
                if new_second_derivative < second_derivative and second_derivative != np.inf:
                    self.clusters = old_clustering
                    break


    @staticmethod
    def wards_clusters_distance(clust_A, clust_B):
        centroid_A = Clustering._centroid(clust_A)
        centroid_B = Clustering._centroid(clust_B)
        n_A = clust_A.shape[0]
        n_B = clust_B.shape[0]
        distance = ((n_A * n_B) / (n_A + n_B)) * (euclidian_distance(centroid_A, centroid_B) ** 2)
        return(distance)
