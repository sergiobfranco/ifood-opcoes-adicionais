"""
Módulo para identificação de porta-vozes não cadastrados usando LLM.
Analisa notícias sem porta-vozes cadastrados e usa DeepSeek para encontrar novos.
"""

import pandas as pd
import requests
import time
import logging
from datetime import datetime
from typing import List, Dict
from pathlib import Path

from config.settings import DEEPSEEK_API_KEY, PASTA_OUTPUT

logger = logging.getLogger(__name__)

API_URL = "https://api.deepseek.com/v1/chat/completions"
HEADERS = {
    "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
    "Content-Type": "application/json"
}


def find_unregistered(
    df_sem_porta_voz: pd.DataFrame,
    df_news: pd.DataFrame,
    output_file: Path,
    valid_brands: List[str],
    max_retries: int = 3
) -> pd.DataFrame:
    """
    Processa notícias sem porta-vozes cadastrados usando LLM.
    """
    logger.info("Identificando porta-vozes não cadastrados...")
    
    # Inicializar DataFrame vazio
    df_result = pd.DataFrame(columns=['Id', 'Titulo', 'Midia', 'Veiculo', 'Porta_Voz', 'Marca'])
    df_result.to_excel(output_file, index=False)
    
    # Renomear e merge
    df_sem_pv = df_sem_porta_voz.rename(columns={'Id': 'Noticia_Id'}).copy()
    df_subset = df_news[['Id', 'Titulo', 'Conteudo', 'Midia', 'Veiculo']].copy()
    
    df_para_processar = pd.merge(
        df_sem_pv,
        df_subset,
        left_on='Noticia_Id',
        right_on='Id',
        how='left'
    ).drop(columns=['Id', 'Titulo_x']).rename(columns={'Noticia_Id': 'Id'})
    
    if df_para_processar.duplicated(subset=['Id']).any():
        df_para_processar = df_para_processar.drop_duplicates(subset=['Id'], keep='first')
    
    logger.info(f"Processando {len(df_para_processar)} notícias sem porta-voz cadastrado")
    
    resultados = []
    
    for _, row in df_para_processar.iterrows():
        noticia_id = row['Id']
        titulo = str(row['Titulo_y']).strip()
        conteudo = str(row['Conteudo']).strip()
        midia = row['Midia_y']
        veiculo = row['Veiculo_y']
        
        if not titulo and not conteudo:
            resultados.append({
                'Id': noticia_id,
                'Titulo': titulo,
                'Midia': midia,
                'Veiculo': veiculo,
                'Porta_Voz': "Conteúdo Vazio",
                'Marca': None
            })
            continue
        
        texto_completo = f"Título: {titulo}\n\nConteúdo: {conteudo}"
        
        prompt = f"""
Analise o seguinte texto de notícia (contendo Título e Conteúdo) e identifique todos os nomes de indivíduos mencionados que parecem ser porta-vozes ou fontes relevantes para alguma marca ou entidade citada na notícia.

Para cada indivíduo identificado que parece estar falando em nome de uma marca/entidade:
- Informe o nome do indivíduo.
- Informe a marca ou entidade em nome da qual ele parece estar falando, com base no contexto da notícia.

Se nenhum indivíduo for identificado como porta-voz relevante, responda apenas "Nenhum porta-voz identificado".

Formato da resposta esperado: Liste os indivíduos e suas marcas no formato "Nome do Porta-Voz: Marca/Entidade". Se houver múltiplos porta-vozes, liste cada um em uma nova linha.

Texto da Notícia:
{texto_completo}
"""
        
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {
                    "role": "system",
                    "content": "Você é um analista de conteúdo especializado em identificar porta-vozes e as entidades que representam em textos de notícias."
                },
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.2
        }
        
        try:
            logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Submetendo notícia ID {noticia_id}")
            response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
            response.raise_for_status()
            resposta = response.json()['choices'][0]['message']['content'].strip()
            
            if resposta == "Nenhum porta-voz identificado":
                resultados.append({
                    'Id': noticia_id,
                    'Titulo': titulo,
                    'Midia': midia,
                    'Veiculo': veiculo,
                    'Porta_Voz': "Nenhum porta-voz identificado",
                    'Marca': None
                })
            else:
                linhas = resposta.split('\n')
                for linha in linhas:
                    if ':' in linha:
                        partes = linha.split(':', 1)
                        nome_pv = partes[0].strip()
                        marca_entidade = partes[1].strip() if len(partes) > 1 else None
                        
                        resultados.append({
                            'Id': noticia_id,
                            'Titulo': titulo,
                            'Midia': midia,
                            'Veiculo': veiculo,
                            'Porta_Voz': nome_pv,
                            'Marca': marca_entidade
                        })
        
        except Exception as e:
            logger.error(f"Erro ao processar notícia ID {noticia_id}: {e}")
            resultados.append({
                'Id': noticia_id,
                'Titulo': titulo,
                'Midia': midia,
                'Veiculo': veiculo,
                'Porta_Voz': "Erro na API",
                'Marca': None
            })
        
        time.sleep(1)
    
    df_result = pd.DataFrame(resultados)
    
    # Filtrar
    if not df_result.empty and 'Porta_Voz' in df_result.columns:
        df_result = df_result[df_result['Porta_Voz'] != "Erro no Processamento"].copy()
    
    if not df_result.empty and 'Marca' in df_result.columns:
        marca_col = df_result['Marca'].copy().fillna("VALOR_NAO_EM_WMARCAS")
        df_result = df_result[marca_col.isin(valid_brands)].copy()
    
    df_result.to_excel(output_file, index=False)
    logger.info(f"Resultado salvo: {output_file} ({len(df_result)} registros)")

    # Salvar cópia histórica timestamped também em PASTA_OUTPUT (manter o arquivo original em partials)
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        hist_name = f"{output_file.stem}_{timestamp}{output_file.suffix}"
        hist_path = PASTA_OUTPUT / hist_name
        # Garantir que a pasta de output exista
        PASTA_OUTPUT.mkdir(parents=True, exist_ok=True)
        df_result.to_excel(hist_path, index=False)
        logger.info(f"Cópia histórica salva em PASTA_OUTPUT: {hist_path}")
    except Exception as e:
        logger.error(f"Falha ao salvar cópia histórica em PASTA_OUTPUT: {e}")
    
    return df_result