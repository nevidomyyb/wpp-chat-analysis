import pandas as pd
from time import time
import re
from typing import List
from collections import Counter
from nltk.corpus import stopwords
import nltk
from numbers_ import numbers_mapping as numb

nltk.download('stopwords')

remove = [
    'mídia', 'oculta', 'n', 'vou', 'tá', 'the', 'q', 'sim', 'vai', 'né',
    'pra', 'aí', 'lá', 'mensagem', 'bom', 'nn', 'vcs', 'c', 'ta', 'ai',
    'cara', 'mn', 'eh', 'so', 'poha', 'tô', 'pq', 'acho', 'ja', 'kaakaakaakaa',
    'véi', 'kkk', 'vc', 'mt', 'apagada', 'inválida', 'editada', 'https', 'ainda',
    'fazer', 'kaakaakaa', 'menos'
]

phrases_to_remove = [
    'localização em tempo real compartilhada', 'em tempo real compartilhada', 'localização em tempo real',
    'não não não não não não', 'não não não não', 'não não não não não',
    
]

MEDIA_REGEX = re.compile(r'<Mídia oculta>')
PUNCTUATION_REGEX = re.compile(r'[^\w\s]')

def get_quantity_message_by_member() -> None:
    df = pd.read_csv('./messages_with_names.csv', sep=';')
    counts = df.groupby('sender').size()
    counts.to_csv('./messages_by_member.csv', sep=';')

def generate_word_frequency() -> None:
    df = pd.read_csv('./messages_with_names.csv', sep=';')
    senders = df['sender'].drop_duplicates().to_list()
    dfs = [df[df['sender'] == sender] for sender in senders]
    sender_word_frequency = {}
    stopwords_ = set(stopwords.words('portuguese'))
    
    for df in dfs:
        sender = df['sender'].iloc[0]
        
        df = df.fillna("Mensagem inválida")
        all_words = ' '.join(df['message']).lower()
        words = re.findall('\w+', all_words)
        
        filtered_words = [word for word in words if word not in stopwords_ and word not in remove and len(word) >= 5]
        word_counts_sender = Counter(filtered_words).most_common(20)
        sender_word_frequency[sender] = word_counts_sender
    data = []
    for member, words in sender_word_frequency.items():
        for word, count in words:
            data.append({'member': member, 'word': word, 'count': count})
    df = pd.DataFrame(data)
    df.to_csv('words_by_member.csv', sep=';')
