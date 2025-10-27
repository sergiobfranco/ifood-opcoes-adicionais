"""
Módulo para consolidação dos resultados de todas as análises em um DataFrame único.
Duplica registros por porta-voz/marca e prepara estrutura para consolidação final.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import List, Dict, Set, Tuple

logger = logging.getLogger(__name__)

# Colunas do DataFrame original a preservar
ORIGINAL_COLUMNS = [
    'Id', 'Titulo', 'Midia', 'Conteudo', 'UrlVisualizacao',
    'DataVeiculacao', 'Veiculo', 'Canais', 'ClassificacaoVeiculo', 'Avaliacao'
]

# Colunas adicionais para consolidação
CONSOLIDATION_COLUMNS = [
    'pv_cadastrados', 'pv_nao_cadastrados', 'nivel_protagonismo',
    'texto_nota', 'Assunto', 'score_similaridade_assunto'
]


def initialize_consolidated_df(df_news: pd.DataFrame) -> pd.DataFrame:
    """Inicializa DataFrame consolidado com colunas necessárias."""
    existing_cols = [col for col in ORIGINAL_COLUMNS if col in df_news.columns]
    df_consolidated = df_news[existing_cols].copy()
    
    if 'Marca' not in df_consolidated.columns:
        df_consolidated['Marca'] = None
    
    for col in CONSOLIDATION_COLUMNS:
        if col not in df_consolidated.columns:
            df_consolidated[col] = None
    
    logger.info(f"DataFrame consolidado inicializado: {len(df_consolidated)} registros")
    return df_consolidated


def consolidate_spokespersons(
    df_news: pd.DataFrame,
    df_spokespersons: pd.DataFrame
) -> pd.DataFrame:
    """
    Primeira etapa: consolida porta-vozes cadastrados.
    Duplica registros por porta-voz encontrado.
    """
    logger.info("Consolidando porta-vozes cadastrados...")
    
    df_consolidated = initialize_consolidated_df(df_news)
    
    # Filtrar porta-vozes válidos
    df_valid = df_spokespersons[
        df_spokespersons['Porta_Voz'] != "Sem porta-voz"
    ].copy()
    
    # Remover duplicados
    df_unique = df_valid.drop_duplicates(subset=['Id', 'Marca', 'Porta_Voz']).copy()
    
    duplicates = []
    duplicated_ids = set()
    missing_ids = []
    
    for _, row in df_unique.iterrows():
        noticia_id = row['Id']
        marca = row['Marca']
        porta_voz = row['Porta_Voz']
        
        match = df_consolidated[df_consolidated['Id'] == noticia_id]
        
        if not match.empty:
            record = match.iloc[0].copy()
            record['Marca'] = marca
            record['pv_cadastrados'] = porta_voz
            duplicates.append(record)
            duplicated_ids.add(noticia_id)
        else:
            missing_ids.append(noticia_id)
    
    # Remover originais que foram duplicados
    df_consolidated = df_consolidated[~df_consolidated['Id'].isin(duplicated_ids)].copy()
    
    # Adicionar duplicatas
    if duplicates:
        df_duplicates = pd.DataFrame(duplicates)
        df_consolidated = pd.concat([df_consolidated, df_duplicates], ignore_index=True)
    
    logger.info(f"Consolidação PV cadastrados: {len(df_consolidated)} registros")
    logger.info(f"  - Duplicatas criadas: {len(duplicates)}")
    logger.info(f"  - IDs não encontrados: {len(missing_ids)}")
    
    return df_consolidated


def consolidate_unregistered_spokespersons(
    df_consolidated: pd.DataFrame,
    df_unregistered: pd.DataFrame
) -> pd.DataFrame:
    """
    Segunda etapa: consolida porta-vozes não cadastrados.
    Atualiza registros existentes ou cria novos.
    """
    logger.info("Consolidando porta-vozes não cadastrados...")
    
    # ADICIONE ESTA VALIDAÇÃO NO INÍCIO:
    if df_unregistered.empty or 'Porta_Voz' not in df_unregistered.columns:
        logger.warning("DataFrame de porta-vozes não cadastrados está vazio ou sem coluna 'Porta_Voz'")
        return df_consolidated

    # Filtrar relevantes
    df_relevant = df_unregistered[
        (df_unregistered['Porta_Voz'] != "Nenhum porta-voz identificado") &
        (df_unregistered['Porta_Voz'] != "Conteúdo Vazio") &
        (df_unregistered['Porta_Voz'] != "Erro na API") &
        (df_unregistered['Porta_Voz'] != "Erro de Processamento")
    ].copy()
    
    df_unique = df_relevant.drop_duplicates(subset=['Id', 'Marca', 'Porta_Voz']).copy()
    
    updates = []
    new_records = []
    
    for _, row in df_unique.iterrows():
        noticia_id = row['Id']
        marca = row['Marca']
        porta_voz = row['Porta_Voz']
        
        existing = df_consolidated[
            (df_consolidated['Id'] == noticia_id) &
            (df_consolidated['Marca'] == marca)
        ]
        
        if not existing.empty:
            for idx in existing.index:
                updates.append({
                    'index': idx,
                    'Marca': marca,
                    'pv_nao_cadastrados': porta_voz
                })
        else:
            base = df_consolidated[df_consolidated['Id'] == noticia_id]
            if not base.empty:
                record = base.iloc[0].copy()
                record['Marca'] = marca
                record['pv_nao_cadastrados'] = porta_voz
                new_records.append(record)
    
    # Aplicar atualizações
    if updates:
        for update in updates:
            idx = update['index']
            df_consolidated.loc[idx, 'Marca'] = update['Marca']
            df_consolidated.loc[idx, 'pv_nao_cadastrados'] = update['pv_nao_cadastrados']
        logger.info(f"  - Atualizados: {len(updates)} registros")
    
    # Adicionar novos
    if new_records:
        df_new = pd.DataFrame(new_records)
        df_consolidated = pd.concat([df_consolidated, df_new], ignore_index=True)
        logger.info(f"  - Adicionados: {len(new_records)} registros")
    
    logger.info(f"Consolidação PV não cadastrados: {len(df_consolidated)} registros")
    return df_consolidated


def consolidate_protagonist_level(
    df_consolidated: pd.DataFrame,
    df_protagonist: pd.DataFrame
) -> pd.DataFrame:
    """
    Terceira etapa: consolida níveis de protagonismo.
    """
    logger.info("Consolidando níveis de protagonismo...")
    
    # ADICIONE:
    if df_protagonist.empty or 'Nivel' not in df_protagonist.columns:
        logger.warning("DataFrame de protagonismo vazio ou inválido")
        return df_consolidated

    df_unique = df_protagonist.drop_duplicates(subset=['Id', 'Marca', 'Nivel']).copy()
    
    updates = []
    new_records = []
    
    for _, row in df_unique.iterrows():
        noticia_id = row['Id']
        marca = row['Marca']
        nivel = row['Nivel']
        
        existing = df_consolidated[
            (df_consolidated['Id'] == noticia_id) &
            (df_consolidated['Marca'] == marca)
        ]
        
        if not existing.empty:
            for idx in existing.index:
                updates.append({
                    'index': idx,
                    'Marca': marca,
                    'nivel_protagonismo': nivel
                })
        else:
            base = df_consolidated[df_consolidated['Id'] == noticia_id]
            if not base.empty:
                record = base.iloc[0].copy()
                record['Marca'] = marca
                record['nivel_protagonismo'] = nivel
                record['pv_cadastrados'] = ''
                record['pv_nao_cadastrados'] = ''
                new_records.append(record)
    
    if updates:
        for update in updates:
            idx = update['index']
            df_consolidated.loc[idx, 'Marca'] = update['Marca']
            df_consolidated.loc[idx, 'nivel_protagonismo'] = update['nivel_protagonismo']
        logger.info(f"  - Atualizados: {len(updates)} registros")
    
    if new_records:
        df_new = pd.DataFrame(new_records)
        df_consolidated = pd.concat([df_consolidated, df_new], ignore_index=True)
        logger.info(f"  - Adicionados: {len(new_records)} registros")
    
    logger.info(f"Consolidação protagonismo: {len(df_consolidated)} registros")
    return df_consolidated


def consolidate_notes(
    df_consolidated: pd.DataFrame,
    df_notes: pd.DataFrame
) -> pd.DataFrame:
    """
    Quarta etapa: consolida textos de notas.
    """
    logger.info("Consolidando notas...")
    
    # ADICIONE:
    if df_notes.empty:
        logger.warning("DataFrame de notas vazio")
        return df_consolidated

    df_notes_clean = df_notes.rename(columns={'Texto_Nota': 'texto_nota_orig'}).copy()
    df_unique = df_notes_clean.drop_duplicates(subset=['Id', 'Marca']).copy()
    
    updates = []
    new_records = []
    
    for _, row in df_unique.iterrows():
        noticia_id = row['Id']
        marca = row['Marca']
        texto_nota = row['texto_nota_orig']
        
        existing = df_consolidated[
            (df_consolidated['Id'] == noticia_id) &
            (df_consolidated['Marca'] == marca)
        ]
        
        if not existing.empty:
            for idx in existing.index:
                updates.append({
                    'index': idx,
                    'Marca': marca,
                    'texto_nota': texto_nota
                })
        else:
            base = df_consolidated[df_consolidated['Id'] == noticia_id]
            if not base.empty:
                record = base.iloc[0].copy()
                record['Marca'] = marca
                record['texto_nota'] = texto_nota
                new_records.append(record)
    
    if updates:
        for update in updates:
            idx = update['index']
            df_consolidated.loc[idx, 'Marca'] = update['Marca']
            df_consolidated.loc[idx, 'texto_nota'] = update['texto_nota']
        logger.info(f"  - Atualizados: {len(updates)} registros")
    
    if new_records:
        df_new = pd.DataFrame(new_records)
        df_consolidated = pd.concat([df_consolidated, df_new], ignore_index=True)
        logger.info(f"  - Adicionados: {len(new_records)} registros")
    
    logger.info(f"Consolidação notas: {len(df_consolidated)} registros")
    return df_consolidated


def consolidate_subjects(
    df_consolidated: pd.DataFrame,
    df_subjects: pd.DataFrame
) -> pd.DataFrame:
    """
    Quinta etapa: consolida assuntos.
    Apenas atualiza campo existente, sem duplicação.
    """
    logger.info("Consolidando assuntos...")
    
    if 'Assunto' not in df_consolidated.columns:
        df_consolidated['Assunto'] = None
    
    # ADICIONE:
    if df_subjects.empty or 'Assunto' not in df_subjects.columns:
        logger.warning("DataFrame de assuntos vazio ou sem coluna 'Assunto'")
        return df_consolidated

    if not all(col in df_subjects.columns for col in ['Id', 'Assunto']):
        logger.error("df_subjects não contém colunas necessárias")
        return df_consolidated
    
    assunto_map = df_subjects.set_index('Id')['Assunto'].to_dict()
    
    updates_count = 0
    for index, row in df_consolidated.iterrows():
        noticia_id = row['Id']
        assunto_novo = assunto_map.get(noticia_id)
        
        if assunto_novo is not None:
            df_consolidated.at[index, 'Assunto'] = assunto_novo
            updates_count += 1
    
    logger.info(f"Campo 'Assunto' atualizado em {updates_count} registros")
    return df_consolidated


def filter_and_save_consolidated(
    df_consolidated: pd.DataFrame,
    output_path: Path
) -> pd.DataFrame:
    """
    Filtra registros inválidos e salva consolidado final.
    """
    logger.info("Aplicando filtragem final...")
    logger.info(f"Registros antes: {len(df_consolidated)}")
    
    cond_nao = df_consolidated['Marca'] == "NÃO"
    cond_vazio = (
        df_consolidated['Marca'].isna() |
        (df_consolidated['Marca'].astype(str).str.strip() == '')
    )
    
    df_filtered = df_consolidated[~(cond_nao | cond_vazio)].copy()
    
    logger.info(f"Registros após filtragem: {len(df_filtered)}")
    logger.info(f"Registros removidos: {len(df_consolidated) - len(df_filtered)}")
    
    df_filtered.to_excel(output_path, index=False)
    logger.info(f"Consolidado salvo: {output_path}")
    
    return df_filtered