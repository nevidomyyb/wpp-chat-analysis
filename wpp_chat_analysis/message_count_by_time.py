import polars as pl
from time import time

def get_message_count_by_time() -> None:
    df = pl.read_csv('./messages_with_names.csv', separator=';')
    df = df.with_columns(
        pl.col("date_time")
        .str.replace(r" - $", "")
        .str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M")
    )
    df = df.with_columns([
        pl.col("date_time").dt.time().alias("time")   
    ])
    df = df.group_by(['time', 'sender']).len()
    df.write_csv('./message_count_by_time.csv', separator=';', include_header=True)
    
if __name__ == '__main__':
    get_message_count_by_time()