# src/batch_update_creator.py

"""
Módulo para criação de planilha de atualização em lote.
Transforma dados consolidados em formato pivotado por marca.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import List

from config.settings import (
    arq_consolidado,
    arq_lote_final
)

logger = logging.getLogger(__name__)


def create_id_marca_field(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria campo IdMarca combinando Id + Marca.
    """
    if 'Id' in df.columns and 'Marca' in df.columns:
        df['IdMarca'] = df['Id'].astype(str) + '_' + df['Marca'].astype(str)
        logger.info("Campo 'IdMarca' criado")
    else:
        logger.error("Colunas 'Id' ou 'Marca' não encontradas")
        raise ValueError("Colunas essenciais ausentes")
    return df


def replace_ifood_typo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Substitui "Ifood" por "iFood" em todas as colunas string.
    """
    logger.info("Substituindo 'Ifood' por 'iFood'...")
    
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].str.replace('Ifood', 'iFood', case=True, regex=False)
            except AttributeError:
                # Coluna object mas sem valores string
                pass
    
    logger.info("Substituição concluída")
    return df


def merge_duplicate_id_marca(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mescla registros com IdMarca duplicados.
    Pega primeiro valor não-nulo para cada coluna.
    """
    logger.info("Mesclando registros com IdMarca duplicados...")
    
    cols_to_merge = [col for col in df.columns if col != 'IdMarca']
    
    def merge_and_fill(group):
        merged = {}
        for col in cols_to_merge:
            non_null = group[col].dropna()
            merged[col] = non_null.iloc[0] if not non_null.empty else None
        return pd.Series(merged)
    
    df_merged = df.groupby('IdMarca').apply(merge_and_fill).reset_index()
    
    logger.info(f"Mesclagem concluída: {len(df_merged)} registros")
    return df_merged


def pivot_columns_by_brand(
    df: pd.DataFrame,
    columns_range: tuple = (11, 16)
) -> pd.DataFrame:
    """
    Pivoteia colunas especificadas por marca.
    Cria colunas no formato {Marca}_{ColunaNome}.
    """
    logger.info("Pivoteando colunas por marca...")
    
    start_idx, end_idx = columns_range
    
    if len(df.columns) <= end_idx - 1:
        logger.warning(f"DataFrame tem apenas {len(df.columns)} colunas")
        return pd.DataFrame(columns=['Id'])
    
    colunas_para_processar = df.columns[start_idx:end_idx].tolist()
    logger.info(f"Colunas a processar: {colunas_para_processar}")
    
    # Agrupar por Id
    grouped = df.groupby('Id')
    registros_consolidados = []
    
    for id_valor, group_df in grouped:
        novo_registro = {'Id': id_valor}
        
        for _, row in group_df.iterrows():
            marca = row.get('Marca')
            if pd.isna(marca):
                continue
            
            for col_original in colunas_para_processar:
                if col_original in row:
                    conteudo = row[col_original]
                    nova_coluna = f"{marca}_{col_original}"
                    novo_registro[nova_coluna] = conteudo
        
        registros_consolidados.append(novo_registro)
    
    df_final = pd.DataFrame(registros_consolidados)
    logger.info(f"Pivoteamento concluído: {len(df_final)} registros")
    
    return df_final


def create_batch_update_sheet(
    input_path: Path,
    output_path: Path
) -> pd.DataFrame:
    """
    Função principal para criar planilha de atualização em lote.
    """
    logger.info(f"Carregando consolidado de: {input_path}")
    df = pd.read_excel(input_path)
    logger.info(f"Quantidade inicial: {len(df)} registros")
    
    # Pipeline de transformações
    df = create_id_marca_field(df)
    df = replace_ifood_typo(df)
    df = merge_duplicate_id_marca(df)
    
    # Remover duplicados remanescentes
    logger.info(f"Removendo duplicados finais de IdMarca...")
    df = df.drop_duplicates(subset=['IdMarca'], keep='first').reset_index(drop=True)
    logger.info(f"Após remoção: {len(df)} registros")
    
    # Remover coluna IdMarca
    df = df.drop(columns=['IdMarca'])
    
    # Ordenar por Id
    df = df.sort_values(by='Id').reset_index(drop=True)
    
    # Pivotear colunas por marca
    df_final = pivot_columns_by_brand(df)
    
    # Salvar resultado
    df_final.to_excel(output_path, index=False)
    logger.info(f"Arquivo salvo: {output_path}")
    
    return df_final