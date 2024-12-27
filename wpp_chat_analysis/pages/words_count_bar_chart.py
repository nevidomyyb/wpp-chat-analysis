import polars as pl
import bar_chart_race as bcr
from time import time


start = time()
df = pl.read_csv('./curated/words_quantity_member_day.csv', separator=';')
df = df.with_columns(pl.col('date').str.to_date('%Y-%m-%d').alias('date'))
df = df.drop_nulls('date')
df = df.pivot(on='sender', index='date', values='count', aggregate_function='sum')
df = df.sort('date')
df = df.fill_null(0)
df = df.with_columns(
    pl.col(col).cum_sum().alias(f"{col}-cumsum")
    for col in df.columns if col != 'date'
)
    
df = df.drop([col for col in df.columns if 'cumsum' not in col and col != 'date'])
df = df.rename({col: col.replace('-cumsum', "") for col in df.columns})
df = df.to_pandas()
df = df.set_index('date')
df = df.sort_index()
print(df)
    
    

    
# Prepare data and visualization
bcr.bar_chart_race(
    df=df,
    filename='bar_chart_race.mp4',
    n_bars=5,
    steps_per_period=20,
    period_length=100,
    interpolate_period=False
)

print("Finished")
print(f"With: {time()-start} seconds")