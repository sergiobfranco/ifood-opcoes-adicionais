"""
Página para visualizar histórico de execuções.
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime

st.set_page_config(page_title="Histórico", page_icon="📊", layout="wide")

st.title("📊 Histórico de Execuções")

# Buscar arquivos de output com timestamp
from config.settings import PASTA_OUTPUT

if PASTA_OUTPUT.exists():
    files = list(PASTA_OUTPUT.glob("*.xlsx"))
    
    if files:
        data = []
        for f in files:
            stat = f.stat()
            data.append({
                'Arquivo': f.name,
                'Data Modificação': datetime.fromtimestamp(stat.st_mtime),
                'Tamanho (KB)': stat.st_size / 1024
            })
        
        df = pd.DataFrame(data).sort_values('Data Modificação', ascending=False)
        
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhum arquivo processado ainda.")
else:
    st.error("Pasta de output não encontrada.")