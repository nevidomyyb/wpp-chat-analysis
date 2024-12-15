import polars as pl
import nltk

nltk.download('stopwords')

def generate_words_count_by_day() -> None:
    df = pl.scan_csv('./messages_with_names.csv', separator=';', has_header=True)
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
    df.write_csv('./words_count_by_day.csv',separator=';', include_header=True)
if __name__ == "__main__":
    generate_words_count_by_day()