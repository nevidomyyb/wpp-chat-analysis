import os
import pandas as pd
import polars as pl
from utils import load_chat, parse_messages, phrases_to_remove
from numbers_ import numbers_mapping as numb
from time import time

class Raw:
    
    def __init__(self, raw_path: str) -> None:         
        self.raw_path = raw_path
        if not os.path.isdir(raw_path):
            os.mkdir(raw_path)
    
    def create_raw_with_names(self, df: pd.DataFrame, file_final: str) -> None:
        #np is a dict {"Name": "Number"} mapping each Member
        #the "Name" key is the values from the text.
        names_numbers_mapping = numb
        df['sender'] = df['sender'].map(names_numbers_mapping).fillna('Desconhecido')
        df.to_csv(file_final, sep=';', header=True)
        
    def create_ngrams(self, message):
        words = message.split()
        ngrams =[]
        for n in range(3, len(words)+1):
            for i in range(len(words) -n + 1):
                ngram = words[i: i + n]
                phrase = ' '.join(ngram)
                if phrase not in phrases_to_remove: ngrams.append(phrase)
        return ngrams
    
    def create_ngrams_with_names(self, original_file: str, file_final):
        file = original_file
        reader = pl.read_csv_batched(file, separator=';', has_header=True, batch_size=10000, n_threads=5)
        for batch_number, batch in enumerate(reader.next_batches(10)):
            message_senders_pairs = batch.select(['message', 'sender']).rows()
            if batch_number == 0:
                df_batch = pl.DataFrame(schema={"sender": pl.String, "phrase": pl.String})
            else:
                df_batch = pl.scan_csv(file_final, separator=';', has_header=True)
            for message, sender in message_senders_pairs:
                ngrams = self.create_ngrams(message)
                
                if len(ngrams) == 0:
                    continue
                df_this_message = pl.DataFrame({"sender": [sender] * len(ngrams),"phrase": ngrams })
                if isinstance(df_batch, pl.LazyFrame):
                    df_batch = df_batch.collect().vstack(df_this_message)
                else:
                    df_batch = pl.concat([df_batch, df_this_message])
            df_batch.write_csv(file_final, separator=';', include_header=True)
    
    def create(self, original_file_path: str) -> bool:
        start = time()
        chat_data = load_chat(original_file_path)
        df = parse_messages(chat_data)
        df.to_csv(os.path.join(self.raw_path, 'raw.csv'), sep=';')
        print(f"{(time()-start)/60} minutes to create raw file.")
        self.create_raw_with_names(df, os.path.join(self.raw_path, 'raw_with_names.csv'))
        print(f"{(time()-start)/60} minutes to create raw_with_names file.")
        self.create_ngrams_with_names(os.path.join(self.raw_path, 'raw_with_names.csv'), os.path.join(self.raw_path, 'ngrams_with_names.csv'))
        print(f"{(time()-start)/60} minutes to create ngrams_with_names file and finish everything.")
        
        