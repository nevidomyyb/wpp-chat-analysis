import streamlit as st
from streamlit_extras.switch_page_button import switch_page
import os
from Raw import Raw
from Staged import Staged
from Curated import Curated
import tempfile


class LandingPage:
    
    def __init__(self,):
        st.set_page_config(
            page_title='Chat Analysis',
            layout='wide',
            initial_sidebar_state='collapsed',
        )
    
    def draw(self,):
        if not (os.path.isdir('./raw') and os.path.isdir('./staged') and os.path.isdir('./curated')):
            self.drawFileInput()
        else:
            switch_page('messages counts page')
    def drawFileInput(self,):
        file = st.file_uploader(label='WhatsApp file history', accept_multiple_files=False, type='.txt')
        if file:
            with st.spinner("Cleaning data..."):
                temp_dir = os.path.join('./', 'temp')
                os.makedirs(temp_dir, exist_ok=True)
                temp_file_path = os.path.join(temp_dir, file.name)
                with open(temp_file_path, "w", encoding="utf-8") as temp_file:
                    temp_file.write(file.getvalue().decode("utf-8"))
                    
                try:
                    raw = Raw('./raw/')
                    raw.create(temp_file_path)
                    staged = Staged('./staged/')
                    staged.create()
                    curated = Curated('./curated/')
                    curated.create()
                finally:
                    os.remove(temp_file_path)
                if not os.listdir(temp_dir):
                    os.rmdir(temp_dir)
            switch_page('messages counts page')
        
landing_page = LandingPage()
landing_page.draw()
