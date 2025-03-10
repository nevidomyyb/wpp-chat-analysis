import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA
from sklearn.cluster._hdbscan.hdbscan import HDBSCAN
from time import time

class Clusterizer:
    
    def tfid(self, file):
        df = pd.read_csv(file, sep=';')
        new_df = df.groupby('sender').agg({'ngram': ' '.join}).reset_index()
        vec = TfidfVectorizer(
            ngram_range=(1,6),
            max_features=100000,
            analyzer="char_wb",
        )
        X = vec.fit_transform(new_df['ngram'])
        return X, new_df

    def PCA(self):
        x, df = self.tfid('./staged/unique_ngrams.csv')
        pca = PCA(n_components=5)
        X_reduced = pca.fit_transform(x.toarray())
        return X_reduced, df

    def run_HDBSCAN_PCA(self):
        x, df = self.PCA()
        hdbscan = HDBSCAN(min_cluster_size=2)
        clusters = hdbscan.fit_predict(x)
        
        df['cluster'] = clusters
        df = df.drop('ngram', axis=1)
        df = df.groupby('sender').agg({'cluster': 'first'}).reset_index()
        df.to_csv('./HDBSCAN_PCA.csv', sep=';', index=False)
        
if __name__ == "__main__":
    
    clusterizer = Clusterizer()
    clusterizer.run_HDBSCAN_PCA()

    