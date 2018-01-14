"""module to perform pca and pca_imputation"""

import numpy as np

from .helpers import bootstrap

class PCA:
    """class to perform standard pca """

    def __init__(self, nb_components):
        """initialisation of the parameters :
        -nb_components is the number of desired dimension of the redescripted space
        """

        #testing the validity of the params
        try :
            nb_components = np.int16(nb_components)
            assert nb_components > 0, "number of cluster must be more than zero"
            self.nb_components = nb_components
        except ValueError:
            raise AssertionError("nb_components must be an integer")


    def fit(self, fit_data, greedy = True, verbuous = False):
        """fiting the pca to the data.
        If verbuous is true, there will be a small resume with the varainces and all
        """

        #asserting the calidity of the data
        if not greedy:
            self.data = np.copy(fit_data)
        else:
            self.data = fit_data
        self.data = np.array(fit_data)
        assert len(self.data.shape) == 2 , "data has not a proper shape(not two dimensions)"
        assert not self.data.dtype.type is  np.object , "data was not well transfered as a array probably not consistent shape"
        assert not self.data.dtype.type is  np.object_ , "data was not well transfered as a array probably not consistent shape"

        assert self.data.shape[1] >= self.nb_components, "number of varaibles per individuals is smaller than the desired number of components"

        assert type(verbuous) is bool , "verbous must be a boolean"


        #calculating covariance matrix
        cov = np.dot(self.data.T, self.data)

        #getting the components
        eig_values , eig_vectors = np.linalg.eig(cov)

        self._vect = []
        self._val = []
        for i in range(len(eig_values)):
            max_i = np.argmax(eig_values)
            self._val.append(eig_values[max_i])
            self._vect.append(eig_vectors[:,max_i])
            eig_values = np.delete(eig_values, max_i)
            eig_vectors = np.delete(eig_vectors, max_i, axis = 1)
        self._val = np.array(self._val)
        self._vect = np.array(self._vect).T

        #generating the new data and the variance ratios
        self.components = self._vect[:,:self.nb_components]
        self.new_data = np.dot(self.data, self.components)
        self.variance_ratio = self._val / np.sum(self._val)

        #giving resume if verbous is true
        if verbuous:
            print("new components : \n")
            print(self.components)
            print("\n\n that explain {} of the variance : \n".format(np.sum(self.variance_ratio[:self.nb_components])))
            for i in range(self.nb_components):
                print ("component {} : variance ratio of {}".format(i+1, self.variance_ratio[i]))

        return (self)


    def reconstruct(self):
        """method that reconstructs the data with the information of the components"""
        return np.dot(self.new_data, self.components.T)


def pca_imputation(impute_data, nb_components, n_imput = 1, multiple = False, prop = 0.8, gready = False, verbuous = True, gap = 0.01, first_imputation = "normal"):
    """function to impute missing values using pca"""

    #asserting we have valid parameters
    try :
        nb_components = np.int16(nb_components)
        assert nb_components > 0, "number of cluster must be more than zero"
    except ValueError:
        raise AssertionError("nb_components must be an integer")
    try :
        gap = np.float32(gap)
        assert gap > 0, "gap must be more than zero"
    except ValueError:
        raise AssertionError("gap must be an integer")
    assert multiple or n_imput == 1, "cannot inpute more then one time if multiple is false"
    assert first_imputation in ["mean", "normal"], "first imputation method must be a mean imputation or a normal draw"

    #asserting the validity of the data
    data = np.copy(impute_data)
    data = np.array(data)
    assert len(self.data.shape) == 2 , "data has not a proper shape(not two dimensions)"
    assert not self.data.dtype.type is  np.object , "data was not well transfered as a array (not consistent shape or non numerical data)"
    assert not self.data.dtype.type is  np.object_ , "data was not well transfered as a array (not consistent shape or non numerical data)"
    assert data.shape[1] >= nb_components, "number of varaibles per individuals is smaller than the desired number of components"

    data = data.astype(np.float32)


    #performint the imputation
    if not multiple:
        #single imputation
        #knowing which data is to be inputed
        is_nan = np.isnan(data)
        #replacing nan by mean for initialization
        imputed_data = data
        for col_i in range(data.shape[1]):
            col = data[:,col_i]
            col_mean = np.nanmean(col)
            if first_imputation == "mean":
                mean = col_mean * np.ones((data.shape[0],1))
                imputed_data[:,col_i][is_nan[:,col_i]] = mean[is_nan[:,col_i]].flatten()
            elif first_imputation == "normal":
                col_std = np.nanstd(col)
                imputed_data[:,col_i][is_nan[:,col_i]] = np.random.normal(col_mean, col_std,imputed_data[:,col_i][is_nan[:,col_i]].shape)
            else:
                raise RuntimeError("internal error while finding the first imputation method")

        while True:
            #performing the pca
            last = np.copy(imputed_data[is_nan].flatten())
            pca = PCA(nb_components).fit(imputed_data, False)
            pca_data = pca.reconstruct()
            imputed_data[is_nan] = pca_data[is_nan]
            diff = np.sum(np.abs(imputed_data[is_nan].flatten() - last))
            if diff < gap:
                break
    else:
        #multiple imputation
        #initialisation of variables
        imputed_data = np.zeros(data.shape)
        lines_imputed = {}
        if verbuous :
            imputed = {}

        #performing each imputation
        for set_i in range(n_imput) :
            #bootstraping the datasets
            bootstrap_res = bootstrap(data, prop, "ind")
            bootstrap_set = bootstrap_res[0]
            bootstrap_lines = bootstrap_res[1]
            #knowing which data is to be inputed
            is_nan = np.isnan(bootstrap_set)
            #first imputation
            for col_i in range(data.shape[1]):
                col = data[:,col_i]
                col_mean = np.nanmean(col)
                if first_imputation == "mean":
                    mean = col_mean * np.ones((data.shape[0],1))
                    bootstrap_set[:,col_i][is_nan[:,col_i]] = mean[is_nan[:,col_i]].flatten()
                elif first_imputation == "normal":
                    col_std = np.nanstd(col)
                    bootstrap_set[:,col_i][is_nan[:,col_i]] = np.random.normal(col_mean,col_std,bootstrap_set[:,col_i][is_nan[:,col_i]].shape)
                else:
                    raise RuntimeError("internal error while finding the first imputation method")
            #performing the pca
            while True:
                last = np.copy(bootstrap_set[is_nan].flatten())
                pca = PCA(nb_components).fit(bootstrap_set, False)
                pca_data = pca.reconstruct()
                bootstrap_set[is_nan] = pca_data[is_nan]
                diff = np.sum(np.abs(bootstrap_set[is_nan].flatten() - last))
                if diff < gap:
                    break

            #if we are verbuous we have to keep in memory the imputed data
            if verbuous :
                for bootstrap_line, true_line in enumerate(bootstrap_lines):
                    for col_i in range(bootstrap_set.shape[1]) :
                        value = bootstrap_set[bootstrap_line,col_i]
                        if is_nan[bootstrap_line, col_i] :
                            try:
                                imputed[(true_line, col_i)].append(value)
                            except KeyError:
                                imputed[(true_line, col_i)] = [value]

            #adding the imputed data to the final dataset
            for line_i in range(bootstrap_set.shape[0]):
                imputed_data[bootstrap_lines[line_i],:] += bootstrap_set[line_i,:]
                try:
                    lines_imputed[bootstrap_lines[line_i]] +=1
                except KeyError:
                    lines_imputed[bootstrap_lines[line_i]] = 1

        #dividing by the number of imputation made to each missing values
        for line, number in lines_imputed.items():
            imputed_data[line,:] = imputed_data[line,:] / number

        #just a litle of bulshit
        if verbuous:
            imputed_stats = {}
            for key , data in imputed.items():
                imputed_stats[key] = np.std(data)
            mean_std = np.mean([std for key, std in imputed_stats.items()])
            print("mean standard deviation of imputations: {}".format(mean_std))
            more = input("more? [y/n]")
            while not more in ["y", "n"]:
                print("you must enter 'y' or 'n'")
                more = input("more? [y/n]")
            if more == "y" :
                for key, std in imputed_stats.items():
                    print("missing {} has imputation standrd deviation of {}".format(key, std))

    return imputed_data
