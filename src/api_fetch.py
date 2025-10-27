"""
Módulo para buscar dados da API de favoritos do iFood.
Realiza requisições POST, trata erros 500 com retry e salva em Excel.
"""

import requests
import pandas as pd
import json
import time
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


def fetch_api_data(
    config_file: Path,
    output_full: Path,
    output_small: Path,
    max_retries: int = 1
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Busca dados da API de marcas do iFood.
    
    Args:
        config_file: Caminho para o arquivo de configuração JSON
        output_full: Caminho para salvar DataFrame completo
        output_small: Caminho para salvar DataFrame reduzido
        max_retries: Número máximo de tentativas em caso de erro 500
    
    Returns:
        tuple: (df_completo, df_small) - DataFrames completo e reduzido
    """
    logger.info(f"Lendo configuração da API: {config_file}")
    
    try:
        with open(config_file, "r") as f:
            api_configs = json.load(f)
            logger.info(f"Configurações carregadas: {len(api_configs)} endpoints")
    except FileNotFoundError:
        logger.error(f"Arquivo de configuração não encontrado: {config_file}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao ler JSON: {e}")
        raise
    
    all_dfs = []
    
    for config in api_configs:
        url = config["url"]
        data = config["data"]
        retry_count = 0
        
        while retry_count <= max_retries:
            logger.info(f"Tentativa {retry_count + 1} para {url}")
            
            try:
                response = requests.post(url, json=data, timeout=30)
                logger.info(f"Status code: {response.status_code}")
                
                if response.status_code == 200:
                    news_data = response.json()
                    df_api = pd.json_normalize(news_data)
                    logger.info(f"DataFrame criado com {len(df_api)} registros")
                    all_dfs.append(df_api)
                    break
                    
                elif response.status_code == 500 and retry_count < max_retries:
                    logger.warning("Erro 500. Aguardando 5 segundos...")
                    all_dfs = []
                    time.sleep(5)
                    retry_count += 1
                    continue
                else:
                    logger.error(f"Erro na requisição: {response.status_code}")
                    break
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro de conexão: {e}")
                break
    
    if not all_dfs:
        logger.warning("Nenhum DataFrame foi recuperado")
        return pd.DataFrame(), pd.DataFrame()
    
    # Concatenar todos os DataFrames
    final_df = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"DataFrame final: {len(final_df)} registros")
    
    # Criar versão reduzida
    required_cols_small = ['Id', 'Titulo', 'Conteudo', 'IdVeiculo', 'Canais']
    existing_cols_small = [col for col in required_cols_small if col in final_df.columns]
    
    if len(existing_cols_small) == len(required_cols_small):
        final_df_small = final_df[existing_cols_small].copy()
    else:
        logger.warning(f"Colunas faltando: {set(required_cols_small) - set(existing_cols_small)}")
        final_df_small = pd.DataFrame(columns=required_cols_small)
    
    # Salvar arquivos
    try:
        final_df.to_excel(output_full, index=False)
        logger.info(f"Arquivo completo salvo: {output_full}")
        
        final_df_small.to_excel(output_small, index=False)
        logger.info(f"Arquivo reduzido salvo: {output_small}")
    except Exception as e:
        logger.error(f"Erro ao salvar arquivos: {e}")
        raise
    
    return final_df, final_df_small