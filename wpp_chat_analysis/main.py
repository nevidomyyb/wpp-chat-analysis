import pandas as pd
from time import time
import re
from typing import List
from collections import Counter
from nltk.corpus import stopwords
import nltk
from .numbers_ import numbers_mapping as np

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

def load_chat(file_path: str) -> List[str]:
    with open(file_path, "r", encoding="utf-8") as file:
        chat_data = file.readlines()
    return chat_data

def parse_messages(chat_data: List[str]) -> pd.DataFrame:
    messages = []
    pattern = r"(\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2} - )(.+?): (.*)"
    lines = 0
    for line in chat_data:
        lines+=1
        match = re.match(pattern, line)
        if match:
            date_time = match.group(1)
            sender = match.group(2)
            message = match.group(3)
            messages.append({"date_time": date_time, "sender": sender, "message": message})
    print(f"Lines: {lines}")
    return pd.DataFrame(messages)

def get_numbers_and_names(df: pd.DataFrame) -> None:
    #np is a dict {"Name": "Number"} mapping each Member
    #the "Name" key is the values from the text.
    names_numbers_mapping = np
    df['sender'] = df['sender'].map(names_numbers_mapping).fillna('Desconhecido')
    df.to_csv('./messages_with_names.csv', sep=';')

def get_quantity_message_by_member() -> None:
    df = pd.read_csv('./messages_with_names.csv', sep=';')
    counts = df.groupby('sender').size()
    counts.to_csv('./messages_by_member.csv', sep=';')

def generate_word_frequency() -> None:
    df = pd.read_csv('./messages_with_names.csv', sep=';')
    df_eduardo = df[df['sender'] == "Eduardo"]
    df_lucas = df[df['sender'] == "Lucas"]
    df_vagoneta = df[df['sender'] == "Vagoneta"]
    df_diesel = df[df['sender'] == "Vin Diesel"]
    df_pedro = df[df['sender'] == "Pedro"]
    df_will = df[df['sender'] == "Will"]
    df_luciano = df[df['sender'] == "Luciano"]
    df_matheus = df[df['sender'] == "Matheus"]
    df_jean = df[df['sender'] == "Jean"]
    df_gui = df[df['sender'] == "Guilherme"]
    df_heitor = df[df['sender'] == "Heitor"]
    df_lian = df[df['sender'] == "Lian"]
    df_caua = df[df['sender'] == "Cauã"]
    df_joao = df[df['sender'] == "João"]
    dfs = [
        df_eduardo, df_lucas, df_vagoneta, df_diesel, df_pedro, df_will, df_joao,
        df_matheus, df_jean, df_gui, df_heitor, df_lian, df_caua, df_luciano
    ]
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


if __name__ == "__main__":
    start = time()
    # chat_data = load_chat('./SLChat.txt')
    # df = parse_messages(chat_data)
    # df.to_csv('./messages_raw.csv', sep=';')
    # get_numbers_and_names(df)
    # get_quantity_message_by_member()
    print(f"Time elapsed: {(time()-start)/60} minutes.")