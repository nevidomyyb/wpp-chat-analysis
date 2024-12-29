from streamlit_extras.switch_page_button import switch_page
import streamlit as st

def add_page(title:str, page: str, current_page: str, icon="😃"):
    if st.button(title, use_container_width=True, type='secondary', disabled=current_page==page, icon=icon):
        switch_page(page)

def sidebar_func(current_page: str):
    with st.sidebar:
        add_page('Message Counts', 'messages counts page', current_page, "🧿")
        add_page('Top Words', 'top words page', current_page, "🧾")
        add_page('Words Count Bar Race', 'words count bar chart', "📽️")
        
        
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

