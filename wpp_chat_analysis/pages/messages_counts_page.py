import streamlit as st
import pandas as pd
import altair
from math import sqrt

def calc_var(avarege, df):
    values = df['quantity'].values.tolist()
    sum_all = sum((x-avarege)**2 for x in values)
    return sum_all/len(values)
st.set_page_config(layout='wide')
st.title("Messages counts of member in the Sem Limites group between 2019-2024")
df = pd.read_csv('./curated/messages_quantity_member.csv',sep=';')

average = df['quantity'].sum()/df.shape[0]

df['above_average'] = df['quantity'] > average
variance = calc_var(average, df)
standard_deviation = sqrt(variance)
coefficient_variation = (standard_deviation/average) * 100
df['mean'] = average

bar = altair.Chart(df).mark_bar().encode(
    x=altair.X("sender", title="Member", axis=altair.Axis(labelColor="white", titleColor="gold", titleFontSize=20)),
    y=altair.Y("quantity", title="Messages", axis=altair.Axis(labelColor="white", titleColor="gold", titleFontSize=20)),
    color=altair.condition(
        altair.datum.above_average,
        altair.value('rgb(247,99,12)'),
        altair.value('rgb(0,120,215)')
    )
)
mean = altair.Chart(df).mark_rule(color='rgb(22,198,12)').encode(
    y='mean'
)
chart = (bar+mean).properties(width=800).interactive()

st.altair_chart(chart, use_container_width=True)
legends, metrics = st.columns([1, 3])

with legends:
    st.header("Legends")
    st.write(f":large_green_square: Average of messages.")
    st.write(f":large_blue_square: Members with a message count below the average.")
    st.write(f":large_orange_square: Members with a message count above the average.")

with metrics:
    st.header("Metrics")
    st.subheader(f"Average of :blue[{average:.2f}] messages.")
    st.write(f"This would mean that each member of the group sent {average:.2f} messages, but the standard deviation of :red[{standard_deviation:.2f}] messages\
        and a coefficient of variation of :red[{coefficient_variation:.2f}%] emphasizes a spread of data, indicating disproportionate participation in message counts.")

st.divider()