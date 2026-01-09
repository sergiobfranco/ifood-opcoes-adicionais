"""
Rotina de pós-processamento para preencher IDs de porta-vozes

Leitura do arquivo de saída (Excel/CSV/JSON), busca dos nomes de porta-vozes
na tabela de porta-vozes fornecida e preenchimento das colunas de ID
correspondentes. Gera relatório com nomes não encontrados e ambíguos.
"""
from pathlib import Path
import pandas as pd
import argparse
import logging
import difflib
import unicodedata
import re
from datetime import datetime

logger = logging.getLogger(__name__)


def normalize(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.strip()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()


def split_names(cell: str):
    """Simplificado: assume um nome por célula, sem divisão."""
    if pd.isna(cell):
        return []
    text = str(cell).strip()
    return [text] if text else []


def load_lookup(lookup_path: Path) -> pd.DataFrame:
    from src.spokesperson_identifier import clean_excel_file

    if not lookup_path.exists():
        logger.error(f"Lookup não encontrado: {lookup_path}")
        return pd.DataFrame()

    try:
        df = clean_excel_file(lookup_path)
    except Exception:
        df = pd.read_excel(lookup_path)

    return df


def build_lookup_dicts(df_lookup: pd.DataFrame):
    # Espera colunas: 'Coluna/Opção Adicional', 'ID Resposta', 'Resposta' (conforme outros módulos)
    name_to_id = {}
    name_to_brand = {}

    if df_lookup is None or df_lookup.empty:
        return name_to_id, name_to_brand

    for _, row in df_lookup.iterrows():
        nome = row.get('Resposta') or row.get('Porta_Voz') or row.get('Nome')
        if pd.isna(nome) or str(nome).strip() == '':
            continue
        nome_norm = normalize(nome)
        id_val = row.get('ID Resposta') or row.get('ID_Porta_Voz') or row.get('ID')
        coluna = str(row.get('Coluna/Opção Adicional') or '')

        name_to_id[nome_norm] = id_val
        name_to_brand[nome_norm] = coluna

    return name_to_id, name_to_brand


def find_best_match(name: str, lookup_keys: list, cutoff: float):
    """Match exato case-insensitive, sem fuzzy."""
    name_norm = normalize(name)
    if not name_norm:
        return None, 0.0

    # Exact match only
    if name_norm in lookup_keys:
        return name_norm, 1.0

    return None, 0.0


def process_file(input_path: Path, lookup_path: Path, inplace: bool = False):
    # Carregar arquivo de saída
    suffix = input_path.suffix.lower()
    if suffix in ['.xlsx', '.xls']:
        df = pd.read_excel(input_path)
    elif suffix == '.csv':
        df = pd.read_csv(input_path)
    elif suffix == '.json':
        df = pd.read_json(input_path)
    else:
        raise ValueError(f'Formato não suportado: {suffix}')

    df_lookup = load_lookup(lookup_path)
    name_to_id, name_to_brand = build_lookup_dicts(df_lookup)
    lookup_keys = list(name_to_id.keys())

    # Detectar colunas de porta-vozes (heurística)
    candidate_cols = [c for c in df.columns if re.search(r'porta', c, re.IGNORECASE) and not c.lower().startswith('id ')]

    if not candidate_cols:
        logger.warning('Nenhuma coluna de porta-vozes detectada automaticamente')
        return input_path

    report_rows = []

    for col in candidate_cols:
        id_col = f"ID {col}"
        if id_col not in df.columns:
            df[id_col] = ''

        for idx, val in df[col].items():
            names = split_names(val)
            ids_found = []
            nao_encontrados = []

            for name in names:
                matched, score = find_best_match(name, lookup_keys, 1.0)  # Exact match only
                if matched:
                    ids_found.append(str(name_to_id.get(matched)))
                else:
                    nao_encontrados.append({'row': idx, 'col': col, 'name': name})

            # escrever IDs separados por ;
            if ids_found:
                df.at[idx, id_col] = ';'.join([i for i in ids_found if i not in [None, 'nan']])

            # coletar para relatório
            for n in nao_encontrados:
                report_rows.append({**n, 'type': 'not_found'})

    # Salvar arquivo atualizado
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if inplace:
        output_path = input_path
    else:
        output_path = input_path.with_name(f"{input_path.stem}_with_ids_{timestamp}{input_path.suffix}")

    if output_path.suffix.lower() in ['.xlsx', '.xls']:
        df.to_excel(output_path, index=False)
    elif output_path.suffix.lower() == '.csv':
        df.to_csv(output_path, index=False)
    else:
        df.to_json(output_path, orient='records', force_ascii=False)

    # Salvar relatório
    report_df = pd.DataFrame(report_rows)
    if not report_df.empty:
        report_path = input_path.with_name(f"{input_path.stem}_spokesperson_report_{timestamp}.csv")
        report_df.to_csv(report_path, index=False)
        logger.info(f"Relatório salvo: {report_path}")
    else:
        logger.info('Nenhum problema detectado no mapeamento de porta-vozes')

    logger.info(f"Arquivo atualizado salvo: {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description='Pós-processamento: preencher IDs de porta-vozes')
    parser.add_argument('input', help='Arquivo de saída (xlsx/csv/json)')
    parser.add_argument('lookup', help='Arquivo de porta-vozes (Excel)')
    parser.add_argument('--inplace', action='store_true', help='Sobrescrever arquivo de entrada')
    parser.add_argument('--verbose', action='store_true')

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    input_path = Path(args.input)
    lookup_path = Path(args.lookup)

    process_file(input_path, lookup_path, inplace=args.inplace)


if __name__ == '__main__':
    main()
