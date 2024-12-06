import polars as pl
from time import time
import re

def get_active_time() -> None:
    df = pl.read_csv('./messages_with_names.csv', separator=';')
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
    df.write_csv('./member_activity_summary.csv', separator=';')
if __name__ == '__main__':
    get_active_time()
    