import polars as pl
from time import time
from main import phrases_to_remove

def create_ngrams(message):
    words = message.split()
    ngrams =[]
    for n in range(3, len(words)+1):
        for i in range(len(words) -n + 1):
            ngram = words[i: i + n]
            phrase = ' '.join(ngram)
            if phrase not in phrases_to_remove: ngrams.append(phrase)
    return ngrams

def read_save_batched():
    file = './messages_with_names.csv'
    reader = pl.read_csv_batched(file, separator=';', has_header=True, batch_size=10000, n_threads=5)
    for batch_number, batch in enumerate(reader.next_batches(10)):
        print(F'Processing batch: {batch_number}')
        message_senders_pairs = batch.select(['message', 'sender']).rows()
        if batch_number == 0:
            df_batch = pl.DataFrame(schema={"sender": pl.String, "phrase": pl.String})
        else:
            df_batch = pl.scan_csv('./phrases_sender.csv', separator=';', has_header=True)
        for message, sender in message_senders_pairs:
            ngrams = create_ngrams(message)
            
            if len(ngrams) == 0:
                continue
            df_this_message = pl.DataFrame({"sender": [sender] * len(ngrams),"phrase": ngrams })
            if isinstance(df_batch, pl.LazyFrame):
                df_batch = df_batch.collect().vstack(df_this_message)
            else:
                df_batch = pl.concat([df_batch, df_this_message])
        df_batch.write_csv('phrases_sender.csv', separator=';')
        
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
    read_save_batched()
    quantify_ngrams()
    print(f"Finished the process with: {(time()-start)/60} minutes.")