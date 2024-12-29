import polars as pl
import bar_chart_race as bcr
from time import time, sleep
from utils import sidebar_func
import streamlit as st
import os
class WordsCountBarChart:
    
    def __init__(self):
        st.set_page_config(layout='wide', page_icon='📽️', page_title='Words Count Bar Chart')
        sidebar_func('words count bar chart')
        
    def draw(self, ):
        st.title("Evolution of spoken words by member.")
        if not os.path.exists(os.path.join('./curated', 'bar_chart_race.mp4')):
            _, col, _ = st.columns([1,4,1])
            with col:
                start = st.button('Create video', use_container_width=True)
                if start:
                    with st.spinner("Processing"):
                        st.write("Reading data...")
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
                        st.write("Creating indexes...")
                        df = df.drop([col for col in df.columns if 'cumsum' not in col and col != 'date'])
                        df = df.rename({col: col.replace('-cumsum', "") for col in df.columns})
                        df = df.to_pandas()
                        df = df.set_index('date')
                        df = df.sort_index()
                        st.write("Begin of video creation...")
                        st.write("It will take a while.")
                        bcr.bar_chart_race(
                            df=df,
                            filename=os.path.join('./curated', 'bar_chart_race.mp4'),
                            n_bars=5,
                            steps_per_period=20,
                            period_length=100,
                            interpolate_period=False
                        )
                        st.rerun()
        else:
            st.video(os.path.join('./curated', 'bar_chart_race.mp4'))

                    
words_count_bar_chart = WordsCountBarChart()
words_count_bar_chart.draw()