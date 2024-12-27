import os
import pandas as pd
from nltk.corpus import stopwords
from collections import Counter
import re
from utils import remove
import polars as pl
from time import time

class Curated:
    
    def __init__(self, curated_path: str):
        self.curated_path = curated_path
        if not os.path.isdir(curated_path):
            os.makedirs(curated_path)
            
    def create_messages_quantity_member(self, original_file: str, final_file: str) -> None:
        df = pd.read_csv(original_file, sep=';')
        counts = df.groupby('sender',).size()
        counts.to_csv(final_file, sep=';', header=True)
        
    def create_words_quantity_member_day(self, original_file: str, final_file: str) -> None:
        df = pl.scan_csv(original_file, separator=';', has_header=True)
        df = df.with_columns(
            pl.col("date_time")
            .str.extract(r"^(\d{2}/\d{2}/\d{4})") 
            .str.strptime(pl.Date, format="%d/%m/%Y")
            .alias("date") 
        )
        df = df.with_columns([
            pl.col('message').str.split(' ').alias('list_words')
        ])
        df = df.with_columns([
            pl.col('list_words').list.len().alias('count')
        ])
        df = df.group_by(['sender', 'date']).agg(pl.col('count').sum().alias('count')).sort(['sender', 'date'])
        df = df.collect()
        df.write_csv(final_file,separator=';', include_header=True)
        
    def create_words_quantity_member(self, original_file: str, final_file: str) -> None:
        df = pd.read_csv(original_file, sep=';')
        senders = df['sender'].drop_duplicates().to_list()
        dfs = [df[df['sender'] == sender] for sender in senders]
        sender_word_frequency = {}
        stopwords_ = set(stopwords.words('portuguese'))
        
        for df in dfs:
            sender = df['sender'].iloc[0]
            
            df = df.fillna("Mensagem inválida")
            all_words = ' '.join(df['message']).lower()
            words = re.findall('\w+', all_words)
            
            filtered_words = [word for word in words if word not in stopwords_ and word not in remove and len(word) >= 5]
            word_counts_sender = Counter(filtered_words).most_common(20)
            sender_word_frequency[sender] = word_counts_sender
        data = []
        for member, words in sender_word_frequency.items():
            for word, count in words:
                data.append({'member': member, 'word': word, 'count': count})
        df = pd.DataFrame(data)
        df.to_csv(final_file, sep=';', header=True, index=False)
        
    def create(self, ) -> None:
        start = time()
        self.create_messages_quantity_member(os.path.join('./raw/', 'raw_with_names.csv'), os.path.join(self.curated_path, 'messages_quantity_member.csv'))
        print(f"{(time()-start)/60} minutes to create messages quantity by member file.")
        self.create_words_quantity_member_day(os.path.join('./raw/', 'raw_with_names.csv'), os.path.join(self.curated_path, 'words_quantity_member_day.csv'))
        print(f"{(time()-start)/60} minutes to create words quantity by member by day file.")
        self.create_words_quantity_member(os.path.join('./raw/', 'raw_with_names.csv'), os.path.join(self.curated_path, 'words_quantity_member.csv'))
        print(f"{(time()-start)/60} minutes to words quantity by member file and finish everything.")