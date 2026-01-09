"""
Módulo para identificação de porta-vozes cadastrados nas notícias.
Carrega lista de porta-vozes, busca menções no conteúdo e gera relatório.
"""

import pandas as pd
import re
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple

logger = logging.getLogger(__name__)


def clean_excel_file(file_path: Path) -> pd.DataFrame:
    """
    Limpa arquivo Excel removendo linhas/colunas vazias iniciais.
    """
    logger.info(f"Limpando arquivo: {file_path}")
    
    df_temp = pd.read_excel(file_path, header=None)
    arquivo_modificado = False
    
    # Verificar primeira linha vazia
    if len(df_temp) > 0:
        primeira_linha = df_temp.iloc[0]
        primeira_linha_vazia = primeira_linha.isna().all() or \
                              all(str(val).strip() == '' for val in primeira_linha if pd.notna(val))
        
        if primeira_linha_vazia:
            df_temp = df_temp.iloc[1:].reset_index(drop=True)
            arquivo_modificado = True
            logger.info("Primeira linha vazia removida")
    
    # Verificar primeira coluna vazia
    if len(df_temp.columns) > 0:
        primeira_coluna = df_temp.iloc[:, 0]
        primeira_coluna_vazia = primeira_coluna.isna().all() or \
                               all(str(val).strip() == '' for val in primeira_coluna if pd.notna(val))
        
        if primeira_coluna_vazia:
            df_temp = df_temp.iloc[:, 1:].reset_index(drop=True)
            arquivo_modificado = True
            logger.info("Primeira coluna vazia removida")
    
    if arquivo_modificado:
        df_temp.to_excel(file_path, index=False, header=False)
        logger.info("Arquivo sobrescrito com correções")
    
    df = pd.read_excel(file_path, header=0)
    logger.info(f"Arquivo carregado: {len(df)} linhas, colunas: {list(df.columns)}")
    
    return df


def identify_spokespersons(
    df_news: pd.DataFrame,
    spokesperson_file: Path,
    output_file: Path
) -> pd.DataFrame:
    """
    Função principal que identifica porta-vozes nas notícias.
    """
    logger.info("Iniciando identificação de porta-vozes...")
    
    try:
        df_porta_vozes = clean_excel_file(spokesperson_file)
    except FileNotFoundError:
        logger.error(f"Arquivo não encontrado: {spokesperson_file}")
        df_porta_vozes = pd.DataFrame(columns=['Coluna/Opção Adicional', 'ID Resposta', 'Resposta'])
    
    if df_porta_vozes.empty or 'Resposta' not in df_porta_vozes.columns:
        logger.warning("DataFrame de porta-vozes vazio ou sem coluna 'Resposta'")
        return pd.DataFrame(columns=['Id', 'Titulo', 'Midia', 'Veiculo', 'Porta_Voz', 'Marca', 'ID_Porta_Voz'])
    
    # Criar dicionários de lookup
    porta_vozes_dict = {}
    porta_vozes_id_dict = {}
    
    for _, row in df_porta_vozes.iterrows():
        nome = row['Resposta']
        coluna_opcao = str(row['Coluna/Opção Adicional'])
        id_resposta = row.get('ID Resposta')
        
        marca = None
        for prefix in ['Porta Vozes ', 'Porta-vozes ', 'Porta-Vozes ']:
            if coluna_opcao.startswith(prefix):
                marca = coluna_opcao.replace(prefix, '', 1).strip()
                break
        
        if pd.notna(nome) and str(nome).strip() != '':
            porta_vozes_dict[nome] = marca
            porta_vozes_id_dict[nome] = id_resposta
    
    logger.info(f"Dicionário criado com {len(porta_vozes_dict)} porta-vozes")
    
    # Buscar porta-vozes nas notícias
    records = []
    
    for _, row in df_news.iterrows():
        noticia_id = row['Id']
        conteudo = str(row['Conteudo']).lower()
        titulo = row['Titulo']
        midia = row['Midia']
        veiculo = row['Veiculo']
        
        found_spokespersons = set()
        
        for nome in porta_vozes_dict.keys():
            if nome and re.search(r'\b' + re.escape(nome.lower()) + r'\b', conteudo):
                found_spokespersons.add(nome)
        
        if found_spokespersons:
            for pv in found_spokespersons:
                records.append({
                    'Id': noticia_id,
                    'Titulo': titulo,
                    'Midia': midia,
                    'Veiculo': veiculo,
                    'Porta_Voz': pv,
                    'Marca': porta_vozes_dict.get(pv),
                    'ID_Porta_Voz': porta_vozes_id_dict.get(pv)
                })
        else:
            records.append({
                'Id': noticia_id,
                'Titulo': titulo,
                'Midia': midia,
                'Veiculo': veiculo,
                'Porta_Voz': "Sem porta-voz",
                'Marca': None,
                'ID_Porta_Voz': None
            })
    
    df_result = pd.DataFrame(records).drop_duplicates(subset=['Id', 'Marca', 'Porta_Voz'], keep='first')
    logger.info(f"Identificação concluída: {len(df_result)} registros")
    
    df_result.to_excel(output_file, index=False)
    logger.info(f"Arquivo salvo: {output_file}")
    
    return df_result