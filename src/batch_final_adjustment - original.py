"""
Módulo para adequação final do lote de atualização.
Realiza transformações, renomeações e formatação com hyperlinks.
NOTA: Lookups de IDs foram simplificados - adicione conforme necessário.
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


def process_final_batch(
    df_lote_final: pd.DataFrame,
    df_original: pd.DataFrame
) -> pd.DataFrame:
    """
    Função principal de processamento final do lote.
    """
    logger.info("Iniciando processamento final do lote...")
    
    # 1. Ajustar Rappi_Assunto
    df = adjust_rappi_subject(df_lote_final.copy())
    
    # 2. Criar DataFrame limpo
    df = create_clean_dataframe(df, df_original)
    
    # 3. Renomear colunas
    df = rename_columns(df)
    
    # 4. Preencher Nota do iFood
    if 'Nota do iFood' in df.columns:
        df['Nota do iFood'] = df['Nota do iFood'].apply(
            lambda x: 'Não' if pd.isna(x) or str(x).strip() == '' else 'Sim'
        )
    
    # 5. Criar coluna Esforço
    df['Esforço'] = 'Reativo'
    
    # Mover Esforço após Assunto Específico II
    if 'Assunto Específico II' in df.columns:
        idx = df.columns.get_loc('Assunto Específico II')
        esforco_col = df.pop('Esforço')
        df.insert(idx + 1, 'Esforço', esforco_col)
    
    # 6. Adicionar colunas ID
    df = add_id_columns(df)
    
    # 7. LOOKUPS DE IDs (SIMPLIFICADO - ADICIONE CONFORME NECESSÁRIO)
    df = lookup_spokesperson_ids(df)
    df = lookup_protagonist_ids(df)
    df = lookup_effort_ids(df)
    df = lookup_note_ids(df)
    
    # 8. Salvar com hyperlinks
    from config.settings import arq_lote_final_limpo, PASTA_OUTPUT
    
    save_with_hyperlinks(df, arq_lote_final_limpo)
    
    # 9. Salvar com timestamp no Google Drive
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_file = PASTA_OUTPUT / f"Tabela_atualizacao_em_lote_limpo_{timestamp}.xlsx"
    save_with_hyperlinks(df, timestamped_file)
    
    logger.info("Processamento final concluído")
    return df


def adjust_rappi_subject(df: pd.DataFrame) -> pd.DataFrame:
    """Preenche Rappi_Assunto com cascata."""
    logger.info("Ajustando Rappi_Assunto...")
    
    colunas_fallback = [
        'iFood_Assunto', '99_Assunto', 'Outra Marca/Entidade_Assunto',
        'Meituan_Assunto', '99Food_Assunto', 'Keeta_Assunto'
    ]
    
    count = 0
    for index, row in df.iterrows():
        if pd.isna(df.loc[index, 'Rappi_Assunto']) or str(df.loc[index, 'Rappi_Assunto']).strip() == "":
            for col in colunas_fallback:
                if col in df.columns:
                    valor = df.loc[index, col]
                    if pd.notna(valor) and str(valor).strip() != "":
                        df.loc[index, 'Rappi_Assunto'] = valor
                        count += 1
                        break
    
    logger.info(f"Rappi_Assunto ajustado em {count} registros")
    return df


def create_clean_dataframe(df: pd.DataFrame, df_original: pd.DataFrame) -> pd.DataFrame:
    """Cria DataFrame limpo com colunas selecionadas."""
    logger.info("Criando DataFrame limpo...")
    
    colunas_interesse = [
        'Id', 'iFood_pv_cadastrados', 'iFood_texto_nota', 'iFood_Assunto',
        'iFood_nivel_protagonismo', 'Rappi_nivel_protagonismo', 'Rappi_Assunto',
        'DoorDash_nivel_protagonismo', 'Meituan_nivel_protagonismo',
        'Keeta_nivel_protagonismo', '99_nivel_protagonismo',
        'Rappi_pv_cadastrados', 'DoorDash_pv_cadastrados',
        'Meituan_pv_cadastrados', 'Keeta_pv_cadastrados', '99_pv_cadastrados',
        # Adicionar colunas ID de porta-vozes e outros
        'ID_pv_cadastrados', 'ID_Rappi_pv_cadastrados', 'ID_DoorDash_pv_cadastrados',
        'ID_Meituan_pv_cadastrados', 'ID_Keeta_pv_cadastrados', 'ID_99_pv_cadastrados',
        'ID_iFood_nivel_protagonismo', 'ID_Rappi_nivel_protagonismo',
        'ID_DoorDash_nivel_protagonismo', 'ID_Meituan_nivel_protagonismo',
        'ID_Keeta_nivel_protagonismo', 'ID_99_nivel_protagonismo'
    ]
    
    colunas_existentes = [col for col in colunas_interesse if col in df.columns]
    df_clean = df[colunas_existentes].copy()
    
    if 'Id' in df_clean.columns:
        id_index = df_clean.columns.get_loc('Id')
        df_clean.insert(id_index + 1, 'Jornalista/Fonte/Replicador/Autor', '')
    
    # Merge com dados originais
    colunas_merge = ['Id', 'UrlVisualizacao', 'Titulo']
    if 'UrlOriginal' in df_original.columns:
        colunas_merge.append('UrlOriginal')
    
    df_subset = df_original[colunas_merge].drop_duplicates(subset=['Id']).copy()
    df_clean = pd.merge(df_clean, df_subset, on='Id', how='left')
    
    # Reordenar
    if 'Id' in df_clean.columns:
        id_index = df_clean.columns.get_loc('Id')
        colunas_ordenadas = list(df_clean.columns)
        
        cols_to_move = [c for c in ['UrlVisualizacao', 'UrlOriginal', 'Titulo'] if c in colunas_ordenadas]
        for col in cols_to_move:
            colunas_ordenadas.remove(col)
        for col in reversed(cols_to_move):
            colunas_ordenadas.insert(id_index + 1, col)
        
        df_clean = df_clean[colunas_ordenadas].copy()
    
    return df_clean


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia colunas."""
    renomear = {
        'iFood_pv_cadastrados': 'Porta-vozes iFood',
        'iFood_texto_nota': 'Nota do iFood',
        'iFood_Assunto': 'Assunto Específico',
        'iFood_nivel_protagonismo': 'Nível de Protagonismo iFood',
        'Rappi_nivel_protagonismo': 'Nivel de Protagonismo Rappi',
        'Rappi_Assunto': 'Assunto Específico II',
        'DoorDash_nivel_protagonismo': 'Nivel de Protagonismo DoorDash',
        'Meituan_nivel_protagonismo': 'Nivel de Protagonismo Meituan',
        'Keeta_nivel_protagonismo': 'Nivel de Protagonismo Keeta',
        '99_nivel_protagonismo': 'Nivel de Protagonismo 99',
        'Rappi_pv_cadastrados': 'Porta Vozes Rappi',
        'DoorDash_pv_cadastrados': 'Porta Vozes Doordash',
        'Meituan_pv_cadastrados': 'Porta Vozes Meituan',
        'Keeta_pv_cadastrados': 'Porta Vozes Keeta',
        '99_pv_cadastrados': 'Porta Vozes 99',
        # Também renomear as colunas ID correspondentes
        'ID_pv_cadastrados': 'ID Porta-vozes iFood',
        'ID_Rappi_pv_cadastrados': 'ID Porta Vozes Rappi',
        'ID_DoorDash_pv_cadastrados': 'ID Porta Vozes Doordash',
        'ID_Meituan_pv_cadastrados': 'ID Porta Vozes Meituan',
        'ID_Keeta_pv_cadastrados': 'ID Porta Vozes Keeta',
        'ID_99_pv_cadastrados': 'ID Porta Vozes 99'
    }
    
    df.rename(columns={k: v for k, v in renomear.items() if k in df.columns}, inplace=True)
    return df


def add_id_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona colunas ID antes das colunas de conteúdo (apenas se não existirem ou estiverem vazias)."""
    logger.info("Adicionando colunas ID...")
    
    colunas_para_id = [
        'Jornalista/Fonte/Replicador/Autor', 'Porta-vozes iFood', 'Nota do iFood',
        'Assunto Específico', 'Nível de Protagonismo iFood', 'Nivel de Protagonismo Rappi',
        'Assunto Específico II', 'Esforço', 'Nivel de Protagonismo DoorDash',
        'Nivel de Protagonismo Meituan', 'Nivel de Protagonismo Keeta',
        'Nivel de Protagonismo 99', 'Porta Vozes Rappi', 'Porta Vozes Doordash',
        'Porta Vozes Meituan', 'Porta Vozes Keeta', 'Porta Vozes 99'
    ]
    
    for col_nome in reversed([c for c in colunas_para_id if c in df.columns]):
        col_index = df.columns.get_loc(col_nome)
        nome_coluna_id = f"ID {col_nome}"
        
        # Só criar a coluna ID se ela não existir ou estiver completamente vazia
        if nome_coluna_id not in df.columns or df[nome_coluna_id].isna().all():
            df.insert(col_index, nome_coluna_id, '')
    
    logger.info("Colunas ID adicionadas/verificados")
    return df


def save_with_hyperlinks(df: pd.DataFrame, output_path: Path) -> None:
    """Salva Excel com hyperlinks."""
    import xlsxwriter
    
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Sheet1', index=False)
        
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        
        hyperlink_format = workbook.add_format({'font_color': 'blue', 'underline': 1})
        
        if 'UrlVisualizacao' in df.columns:
            url_col_index = df.columns.get_loc('UrlVisualizacao')
            
            for row_num in range(1, len(df) + 1):
                url_value = df.iloc[row_num - 1, url_col_index]
                if pd.notna(url_value) and isinstance(url_value, str):
                    worksheet.write_url(row_num, url_col_index, url_value, hyperlink_format, 'Abrir URL')
    
    logger.info(f"Arquivo salvo com hyperlinks: {output_path}")


def load_lookup_file(lookup_path: Path) -> pd.DataFrame:
    """Carrega arquivo de lookup e retorna DataFrame."""
    if not lookup_path.exists():
        logger.warning(f"Arquivo de lookup não encontrado: {lookup_path}")
        return pd.DataFrame()
    
    try:
        df = pd.read_excel(lookup_path)
        logger.info(f"Lookup carregado: {lookup_path.name} ({len(df)} registros)")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar lookup {lookup_path}: {e}")
        return pd.DataFrame()


def lookup_spokesperson_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Preenche IDs dos porta-vozes usando lookup."""
    logger.info("Aplicando lookup de IDs para porta-vozes...")
    
    # Para porta-vozes, os IDs já vêm do consolidator
    # Esta função é mantida para consistência da interface
    logger.info("IDs de porta-vozes já aplicados no consolidator")
    return df


def lookup_protagonist_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Preenche IDs dos níveis de protagonismo usando lookup."""
    logger.info("Aplicando lookup de IDs para níveis de protagonismo...")
    
    from config.settings import arq_nivel_protagonismo_id
    
    df_lookup = load_lookup_file(arq_nivel_protagonismo_id)
    if df_lookup.empty:
        return df
    
    # Criar dicionário de lookup: Resposta -> ID Resposta
    lookup_dict = {}
    for _, row in df_lookup.iterrows():
        resposta = str(row['Resposta']).strip()
        id_resposta = row['ID Resposta']
        lookup_dict[resposta] = id_resposta
    
    # Colunas de protagonismo a processar
    protagonist_columns = [
        'Nível de Protagonismo iFood', 'Nivel de Protagonismo Rappi',
        'Nivel de Protagonismo DoorDash', 'Nivel de Protagonismo Meituan',
        'Nivel de Protagonismo Keeta', 'Nivel de Protagonismo 99'
    ]
    
    updated_count = 0
    for col in protagonist_columns:
        if col in df.columns:
            id_col = f"ID {col}"
            if id_col in df.columns:
                for idx, valor in df[col].items():
                    if pd.notna(valor) and str(valor).strip():
                        valor_str = str(valor).strip()
                        id_valor = lookup_dict.get(valor_str)
                        if id_valor is not None:
                            df.at[idx, id_col] = id_valor
                            updated_count += 1
    
    logger.info(f"IDs de protagonismo aplicados: {updated_count} registros")
    return df


def lookup_effort_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Preenche IDs dos esforços usando lookup."""
    logger.info("Aplicando lookup de IDs para esforços...")
    
    from config.settings import arq_esforco_id
    
    df_lookup = load_lookup_file(arq_esforco_id)
    if df_lookup.empty:
        return df
    
    # Criar dicionário de lookup: Resposta -> ID Resposta
    lookup_dict = {}
    for _, row in df_lookup.iterrows():
        resposta = str(row['Resposta']).strip()
        id_resposta = row['ID Resposta']
        lookup_dict[resposta] = id_resposta
    
    # Coluna de esforço
    effort_col = 'Esforço'
    if effort_col in df.columns:
        id_col = f"ID {effort_col}"
        if id_col in df.columns:
            updated_count = 0
            for idx, valor in df[effort_col].items():
                if pd.notna(valor) and str(valor).strip():
                    valor_str = str(valor).strip()
                    id_valor = lookup_dict.get(valor_str)
                    if id_valor is not None:
                        df.at[idx, id_col] = id_valor
                        updated_count += 1
            
            logger.info(f"IDs de esforço aplicados: {updated_count} registros")
    
    return df


def lookup_note_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Preenche IDs das notas usando lookup."""
    logger.info("Aplicando lookup de IDs para notas...")
    
    from config.settings import arq_nota_id
    
    df_lookup = load_lookup_file(arq_nota_id)
    if df_lookup.empty:
        return df
    
    # Criar dicionário de lookup: Resposta -> ID Resposta
    lookup_dict = {}
    for _, row in df_lookup.iterrows():
        resposta = str(row['Resposta']).strip()
        id_resposta = row['ID Resposta']
        lookup_dict[resposta] = id_resposta
    
    # Coluna de nota
    note_col = 'Nota do iFood'
    if note_col in df.columns:
        id_col = f"ID {note_col}"
        if id_col in df.columns:
            updated_count = 0
            for idx, valor in df[note_col].items():
                if pd.notna(valor) and str(valor).strip():
                    valor_str = str(valor).strip()
                    id_valor = lookup_dict.get(valor_str)
                    if id_valor is not None:
                        df.at[idx, id_col] = id_valor
                        updated_count += 1
            
            logger.info(f"IDs de nota aplicados: {updated_count} registros")
    
    return df