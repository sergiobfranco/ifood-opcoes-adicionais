"""
Módulo para análise de nível de protagonismo das marcas nas notícias.
Utiliza API DeepSeek para classificação baseada em conceitos pré-definidos.
"""

import pandas as pd
import requests
import time
import re
import logging
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path

from config.settings import DEEPSEEK_API_KEY, w_marcas

logger = logging.getLogger(__name__)

API_URL = "https://api.deepseek.com/v1/chat/completions"
HEADERS = {
    "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
    "Content-Type": "application/json"
}

NIVEL_MAPPING = {
    "Nível 1": "Protagonista",
    "Nível 2": "Referência em Matéria de Concorrente",
    "Nível 3": "Referência Contextual/Setor",
    "Nível 4": "Citação Relevante",
    "Nível 5": "Figurante"
}


def should_process_brand(marca: str, canais: str, texto: str) -> bool:
    """
    Determina se uma marca deve ser processada baseado no campo Canais.
    """
    if marca == 'iFood':
        if "'Institucional'" in canais:
            return True
        if 'iFood' in texto:
            return True
        return False
    
    if marca == '99':
        if "'Institucional 99'" in canais:
            return True
        if '99' in texto:
            return True
        return False
    
    # Outras marcas: verificar se está no campo Canais
    if re.search(r'\b' + re.escape(marca.lower()) + r'\b', canais.lower()):
        return True
    
    return False


def analyze_protagonist_level(
    texto: str,
    marca: str,
    conceitos_df: pd.DataFrame,
    max_retries: int = 3
) -> str:
    """
    Chama API DeepSeek para determinar nível de protagonismo.
    """
    prompt = f"""
Considere os seguintes níveis de protagonismo e seus conceitos:

{conceitos_df.to_string(index=False)}

Analise o seguinte texto de notícia (contendo Título e Conteúdo) e determine a qual Nível de Protagonismo a marca "{marca}" se enquadra melhor DENTRO DESTA NOTÍCIA.
Considere como a marca "{marca}" é mencionada e qual papel ela desempenha no conteúdo, incluindo a análise do título conforme os conceitos de cada nível.
Responda SOMENTE com o Nível correspondente (por exemplo: Nível 1, Nível 2, etc.).
Se a marca "{marca}" não for mencionada de forma relevante ou o conteúdo não se enquadrar em nenhum dos níveis apresentados PARA ESSA MARCA ESPECÍFICA, responda 'Nenhum Nível Encontrado'.

Texto da Notícia:
{texto}
"""
    
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {
                "role": "system",
                "content": "Você é um analista de conteúdo especializado em classificar notícias por nível de protagonismo de uma marca específica, analisando tanto o título quanto o conteúdo."
            },
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.2
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
            response.raise_for_status()
            nivel = response.json()['choices'][0]['message']['content'].strip()
            return nivel.replace(":", "").strip()
        except Exception as e:
            logger.warning(f"Tentativa {attempt + 1} falhou: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return "Erro na API"
    
    return "Erro na API"


def analyze_protagonist(
    df_news: pd.DataFrame,
    concepts_file: Path,
    output_file: Path,
    brands: List[str]
) -> pd.DataFrame:
    """
    Processa todas as notícias e determina protagonismo por marca.
    VERSÃO COM ARQUIVO DE CONCEITOS.
    """
    logger.info("Carregando conceitos de protagonismo...")
    
    try:
        df_conceitos = pd.read_excel(concepts_file)
    except FileNotFoundError:
        logger.error(f"Arquivo não encontrado: {concepts_file}")
        return pd.DataFrame(columns=['Id', 'Marca', 'Nivel'])
    
    if 'Nivel' not in df_conceitos.columns or 'Conceito' not in df_conceitos.columns:
        logger.error("Colunas 'Nivel' ou 'Conceito' não encontradas")
        return pd.DataFrame(columns=['Id', 'Marca', 'Nivel'])
    
    return _process_protagonist_analysis(df_news, df_conceitos, output_file, brands)


def analyze_protagonist_simplified(
    df_news: pd.DataFrame,
    output_file: Path,
    brands: List[str]
) -> pd.DataFrame:
    """
    Versão simplificada sem arquivo de conceitos externo.
    Usa conceitos hardcoded - PARA USO NO STREAMLIT.
    """
    logger.info("Analisando protagonismo (versão simplificada sem arquivo de conceitos)...")
    
    # Conceitos hardcoded
    df_conceitos = pd.DataFrame({
        'Nivel': ['Nível 1', 'Nível 2', 'Nível 3', 'Nível 4', 'Nível 5'],
        'Conceito': [
            'Protagonista - Marca é o foco principal da notícia',
            'Referência em Matéria de Concorrente - Marca mencionada ao falar de concorrente',
            'Referência Contextual/Setor - Marca citada no contexto do setor',
            'Citação Relevante - Marca mencionada de forma relevante',
            'Figurante - Marca apenas citada'
        ]
    })
    
    return _process_protagonist_analysis(df_news, df_conceitos, output_file, brands)


def _process_protagonist_analysis(
    df_news: pd.DataFrame,
    df_conceitos: pd.DataFrame,
    output_file: Path,
    brands: List[str]
) -> pd.DataFrame:
    """
    Função interna que faz o processamento real.
    Compartilhada entre as duas versões públicas.
    """
    resultados = []
    
    for idx, row in df_news.iterrows():
        noticia_id = row['Id']
        titulo = str(row['Titulo']).strip()
        conteudo = str(row['Conteudo']).strip()
        canais = str(row.get('Canais', '')).strip()
        
        texto_completo = f"Título: {titulo}\n\nConteúdo: {conteudo}"
        
        if not texto_completo.strip():
            continue
        
        marcas_processadas = set()
        
        for marca in brands:
            if not should_process_brand(marca, canais, texto_completo):
                continue
            
            if marca in marcas_processadas:
                continue
            
            logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Avaliando ID {noticia_id} para marca: {marca}")
            
            nivel = analyze_protagonist_level(texto_completo, marca, df_conceitos)
            
            resultados.append({
                'Id': noticia_id,
                'Marca': marca,
                'Nivel': nivel
            })
            
            marcas_processadas.add(marca)
            time.sleep(1)
        
        # Se nenhuma marca foi processada, adicionar INDEFINIDO
        if not any(r['Id'] == noticia_id for r in resultados):
            resultados.append({
                'Id': noticia_id,
                'Marca': 'INDEFINIDO',
                'Nivel': 'INDEFINIDO'
            })
    
    df_result = pd.DataFrame(resultados)
    
    if not df_result.empty:
        df_result = df_result.drop_duplicates(subset=['Id', 'Marca'], keep='first')
        
        # Substituir nomes dos níveis
        df_result['Nivel'] = df_result['Nivel'].apply(
            lambda x: NIVEL_MAPPING.get(x, x)
        )
    
    df_result.to_excel(output_file, index=False)
    logger.info(f"Resultado salvo: {output_file} ({len(df_result)} registros)")
    
    return df_result