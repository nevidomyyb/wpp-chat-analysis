import os
import polars as pl
import pandas as pd
from time import time, sleep
from nltk import ngrams
from nltk.corpus import stopwords
from collections import Counter
import re

class Staged:
    
    def __init__(self, staged_path: str) -> None:
        self.staged_path = staged_path
        if not os.path.isdir(staged_path):
            os.makedirs(staged_path)
            
    def create_activity_summary(self, original_file: str, final_file: str) -> None:
        df = pl.read_csv(original_file, separator=';', has_header=True)
        df = df.drop('message')
        
        df = df.with_columns(
            pl.col("date_time")
            .str.replace(r" - $", "")
            .str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M")
        )
        condition = None
        for i in range(0,24):
            if i < 10:
                l = f"0{i}"
                j = f"0{i+1}" if i < 8 else f"{i+1}"
            else:
                l = i
                j = i+1 if i != 23 else "00"
            slot_label = f"{l}:00 - {j}:00"
            if i != 23:
                current_condition = pl.when(pl.col("date_time").dt.hour().is_between(i, i+1)).then(pl.lit(slot_label))
            else:
                current_condition = pl.when(pl.col("date_time").dt.time() > 23).then(pl.lit(slot_label))
                
            if condition is None:
                condition = current_condition 
            elif i != 23:
                condition = condition.when(pl.col("date_time").dt.hour().is_between(i, i+1)).then(pl.lit(slot_label))
            else:
                condition = condition.when(pl.col("date_time").dt.time() > 23).then(pl.lit(slot_label))
                
        condition = condition.otherwise(pl.lit("Unknown"))
        df = df.with_columns(condition.alias("time_slot")).drop('date_time')
        df = df.group_by(['sender', 'time_slot']).agg(pl.len().alias("message_count")).sort(['sender', 'time_slot'])
        df.write_csv(final_file, separator=';', include_header=True)
    
    def create_messages_by_time(self, original_file: str, final_file: str) -> None:
        df = pl.read_csv(original_file, separator=';', has_header=True)
        df = df.with_columns(
            pl.col("date_time")
            .str.replace(r" - $", "")
            .str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M")
        )
        df = df.with_columns([
            pl.col("date_time").dt.time().alias("time")   
        ])
        df = df.group_by(['time', 'sender']).len()
        df.write_csv(final_file, separator=';', include_header=True)
        
    def create_most_sended_ngrams(self, original_file: str, final_file: str) -> None:
        df = pl.scan_csv(original_file, separator=';', has_header=True)
        phrase_counts = df.group_by(['sender', 'phrase']).agg(pl.count("phrase").alias("count"))
        sorted_phrases = phrase_counts.sort(['count', 'sender'], descending=[True, False])
        top_20 = sorted_phrases.group_by('sender').head(30)
        result = top_20.collect()
        result.write_csv(final_file, separator=';', include_header=True)
        
    def create_unique_ngrams(self, original_file: str, final_file: str) -> None:
        data = [["sender", "ngram"],]
        original_df = pd.read_csv(original_file, sep=';')
        stop_words = set(stopwords.words('portuguese'))
        for i, row in original_df.iterrows():
            message = str(row['message']).lower()
            message = re.sub(r'http\S+', '', message)
            message = re.sub(r'[^a-zA-ZÀ-ú ]', '', message)
            tokenized = message.split()
            tokens = [token for token in tokenized if token not in stop_words]
            _ngram = list(ngrams(tokens, 6))
            if len(_ngram) == 0:
                continue
            if len(_ngram) == 1:
                data.append([row['sender'], " ".join(_ngram[0])])
            else:
                i, j = 0, len(_ngram)-1
                data.append([row['sender'], " ".join(_ngram[0])])
                while i != j:
                    intersec = Counter(_ngram[i]) & Counter(_ngram[j])
                    if len(intersec) >= 3:
                        j-=1
                    else:
                        data.append([row['sender'], " ".join(_ngram[j])])
                        j-=1
        df_final = pd.DataFrame(data)               
        df_final.to_csv(final_file, sep=';', )
        
    def create(self):
        start = time()
        self.create_activity_summary(os.path.join('./raw/', 'raw_with_names.csv'), os.path.join(self.staged_path, 'activity_summary.csv'))
        print(f"{(time()-start)/60} minutes to create activity summary file.")
        self.create_messages_by_time(os.path.join('./raw/', 'raw_with_names.csv'), os.path.join(self.staged_path, 'messages_by_time.csv'))
        print(f"{(time()-start)/60} minutes to messages by time summary file.")
        self.create_most_sended_ngrams(os.path.join('./raw/', 'ngrams_with_names.csv'), os.path.join(self.staged_path, 'most_sended_ngrams.csv'))
        print(f"{(time()-start)/60} minutes to create most sended ngrams file .")
        self.create_unique_ngrams(os.path.join('./raw/', 'raw_with_names.csv'), os.path.join(self.staged_path, 'unique_ngrams.csv'))
        print(f"{time()-start/60} to finish everything.")


if __name__ == "__main__":
    staged = Staged('./staged')
    staged.create_unique_ngrams('./raw/raw_with_names.csv', os.path.join(staged.staged_path, 'unique_ngrams.csv'))