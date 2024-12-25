import polars as pl
from time import time
from wpp_chat_analysis.utils import phrases_to_remove

        
def quantify_ngrams():
    file = 'phrases_sender.csv'
    df = pl.scan_csv(file, separator=';', has_header=True)
    phrase_counts = df.group_by(['sender', 'phrase']).agg(pl.count("phrase").alias("count"))
    sorted_phrases = phrase_counts.sort(['count', 'sender'], descending=[True, False])
    top_20 = sorted_phrases.group_by('sender').head(30)
    result = top_20.collect()
    result.write_csv('./most_sended_phrases.csv', separator=';', include_header=True)
    
        
if __name__ == "__main__":
    start = time()
    quantify_ngrams()
    print(f"Finished the process with: {(time()-start)/60} minutes.")