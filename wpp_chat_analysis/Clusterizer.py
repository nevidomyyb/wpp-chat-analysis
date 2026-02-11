import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import accuracy_score, precision_score, recall_score
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster._hdbscan.hdbscan import HDBSCAN
from sklearn.cluster import DBSCAN, KMeans
from sklearn.neighbors import KNeighborsClassifier
from time import time
from sklearn.manifold import TSNE
import numpy as np
import pickle
import matplotlib.pyplot as plt
import seaborn as sns

import os

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
        pca = PCA(n_components=1000)
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
    
    def run_KNN_PCA(self):
        x, df = self.PCA_ungrouped()
        embedding_df = pd.DataFrame(x, columns=[f'embedding_{i}' for i in range(x.shape[1])])
        df = pd.concat([df, embedding_df], axis=1)
        x = df.filter(like='embedding_')
        y = df['sender']
        if os.path.exists('./model_KNN_PCA.pkl'):
            print(x)
            return
        k_values = list(range(1, 31))
        scores = []
        scaler = StandardScaler()
        X_scalled = scaler.fit_transform(x)
        for k in k_values:
            knn = KNeighborsClassifier(n_neighbors=k)
            score = cross_val_score(knn, X_scalled, y, cv=5)
            scores.append(np.mean(score))
        best_index = np.argmax(scores)
        best_k = k_values[best_index]
        
        knn = KNeighborsClassifier(n_neighbors=best_k)
        X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)
        knn.fit(X_train, y_train)
        y_pred = knn.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, average='macro')
        recall = recall_score(y_test, y_pred, average='macro')
        
        metrics = pd.DataFrame({
            "accuracy": [accuracy],  
            "precision": [precision],  
            "recall": [recall]  
        }).to_csv('./KNN_metrics.csv', sep=';')
        with open('model_KNN_PCA.pkl', "wb") as f:
            pickle.dump(knn, f)
    
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
    # clusterizer.run_KMEANS_PCA()
    clusterizer.run_KNN_PCA()
    # clusterizer.view_TSNE()

    