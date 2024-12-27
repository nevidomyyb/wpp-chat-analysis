import streamlit as st
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import pandas as pd
import altair
import io

df = pd.read_csv('./curated/words_quantity_member.csv', sep=';')
df_messages = pd.read_csv('./curated/messages_quantity_member.csv', sep=';')
members_oc = df['member'].value_counts()
top_20 = members_oc.head(20).index
df = df[df['member'].isin(top_20)]

st.set_page_config(layout='wide')
st.title("Word frequency by member in the Sem Limites group between 2019-2024")
members = df['member'].unique().tolist()
member = st.selectbox("Member to display data", members, index=None,)
if member != None:
    df_selected = df[df['member'] == member]
    df_selected = df_selected.sort_values(by='count', ascending=True).reset_index(drop=True)
    df_selected['cumulative_sum'] = df_selected['count'].cumsum()
    df_selected['cumulative_percentage'] = round(100*df_selected.cumulative_sum/df_selected['count'].sum(), 2)
    sum_ = df_selected['count'].sum()
    mean_ = sum_/df_selected.shape[0]
    
    df_ = df_messages[df_messages['sender'] == member]
    total_messages = df_.iloc[0]['quantity']
    
    represent_percentage = (sum_/total_messages) * 100
    
    df_selected['mean'] = mean_
    base = altair.Chart(df_selected).encode(
        x=altair.X(
            'word', sort=df_selected['word'].tolist(), 
            title="Word", axis=altair.Axis(labelColor='white', titleColor='gold', titleFontSize=20, labelAngle=15)
        )
    )
    bar = base.mark_bar().encode(
        y=altair.Y('count', title='Count'),
        color=altair.value('rgb(0,120,215)')
    )  
    line = base.mark_line(color='rgb(22,198,12)').encode(
        y=altair.Y(
            'cumulative_percentage', 
            title='Cumulative Percentage', axis=altair.Axis(labelColor='white', titleColor='gold')
        )
    )
    mean = altair.Chart(df_selected).mark_rule(color='rgb(247,99,12)').encode(
        y=altair.Y('mean', title='Average')
    )
    chart = (bar+line+mean).properties(width=800).interactive()
    st.altair_chart(chart, use_container_width=True)
    legends, metrics, word_cloud = st.columns([1, 1, 4])

    with legends:
        st.header("Legends")
        st.write(f":large_green_square: Cumulative percentage of word.")
        st.write(f":large_blue_square: Word frequency")
        st.write(f":large_orange_square: Avarage of word frequency.")

    with metrics:
        st.header("Metrics")
        st.subheader(f"Average of :orange[{mean_:.2f}] words among the 20 most repeated.")
        st.write(f'With a total of :green[{total_messages}] messages, the sum of 20 most frequent words is :green[{sum_}] and this represents :green[{represent_percentage:.2f}%] of total messages.')
    with word_cloud:
        st.header(f"Word cloud for {member}")
        word_freq = dict(zip(df_selected['word'], df['count']))
        wc = WordCloud(
            width=800,
            height=400,
            background_color='white',
            colormap='viridis'
        ).generate_from_frequencies(word_freq)
        fig, ax = plt.subplots(figsize=(6,3))
        ax.imshow(wc, interpolation='bilinear')
        ax.axis('off')
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        
        st.image(buf, use_container_width=True)
    st.divider()
    