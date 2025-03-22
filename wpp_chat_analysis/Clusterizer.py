import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster._hdbscan.hdbscan import HDBSCAN
from sklearn.cluster import DBSCAN, KMeans
from time import time
from sklearn.manifold import TSNE
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


class Clusterizer:
    
    def tfid(self, file, return_old_df:bool = False):
        df = pd.read_csv(file, sep=';')
        new_df = df.groupby('sender').agg({'ngram': ' '.join}).reset_index()
        vec = TfidfVectorizer(
            ngram_range=(1,6),
            max_features=100000,
            analyzer="char_wb",
        )
        if return_old_df:
            X = vec.fit_transform(df['ngram'])
            return X, df
        X = vec.fit_transform(new_df['ngram'])
        return X, new_df

    def PCA_ungrouped(self):
        x, df = self.tfid('./staged/unique_ngrams.csv', True)
        pca = PCA(n_components=5)
        X_reduced = pca.fit_transform(x)
        return X_reduced, df

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
        
    def run_DBSCAN_PCA(self):
        x, df = self.PCA()
        dbscan = DBSCAN(eps=0.1, min_samples=3, metric='cosine', algorithm='auto')
        clusters = dbscan.fit_predict(x)
        
        df['cluster'] = clusters
        df.drop('ngram', axis=1)
        df = df.groupby('sender').agg({"cluster": 'first'}).reset_index()
        df.to_csv('./DBSCAN_PCA.csv', sep=';', index=False)
    
    def run_KMEANS_PCA(self):
        x, df = self.PCA_ungrouped()
        kmeans = KMeans(n_clusters=6, n_init='auto')
        clusters = kmeans.fit_transform(x)
        print("Clusters ->")
        print(clusters)
        df['cluster'] = clusters
        df.drop('ngram', axis=1)
        df = df.groupby('sender').agg({'cluster': 'first'}).reset_index()
        df.to_csv('./KMEANS_PCA.csv', sep=';', index=False)
    
    def view_TSNE(self):
        x ,df = self.PCA_ungrouped()
        # clusters = pd.read_csv('./KMEANS_PCA.csv', sep=';')['cluster']
        y = df['sender'].astype('category').values
        tsne = TSNE(n_components=2, perplexity=30, random_state=42)
        x_tsne = tsne.fit_transform(x)
        plt.figure(figsize=(10,8))
        sns.scatterplot(x=x_tsne[:, 0], y=x_tsne[:, 1], hue=y, palette='bright', s=100)
        plt.title("Clusters de senders baseado na similaridade dos n-grams")
        plt.xlabel("t-SNE Dim 1")
        plt.ylabel("t-SNE Dim 2")
        plt.legend(title="Cluster", bbox_to_anchor=(1, 1))
        plt.show()
        
        
if __name__ == "__main__":
    
    clusterizer = Clusterizer()
    # clusterizer.run_HDBSCAN_PCA()
    # clusterizer.run_DBSCAN_PCA()
    clusterizer.run_KMEANS_PCA()
    # clusterizer.view_TSNE()

    