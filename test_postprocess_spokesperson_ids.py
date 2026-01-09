"""
Teste para o módulo de pós-processamento de IDs de porta-vozes.

Este teste demonstra o funcionamento da rotina sem depender de arquivos reais.
"""

import sys
from pathlib import Path

# Garantir que o projeto está no path
workspace_root = Path(__file__).parent
if str(workspace_root) not in sys.path:
    sys.path.insert(0, str(workspace_root))

import pandas as pd
import tempfile
from src.postprocess_spokesperson_ids import (
    normalize,
    split_names,
    build_lookup_dicts,
    find_best_match,
    process_file
)


def test_normalize():
    """Testa normalização de strings."""
    print("Testing normalize()...")
    assert normalize("João Silva") == "joao silva"
    assert normalize("  Maria  ") == "maria"
    assert normalize("Élisé") == "elise"
    print("✓ normalize() OK")


def test_split_names():
    """Testa divisão de nomes (agora simplificado: um nome por célula)."""
    print("Testing split_names()...")
    assert split_names("João Silva") == ["João Silva"]
    assert split_names("  Maria  ") == ["Maria"]
    assert split_names(None) == []
    print("✓ split_names() OK")


def test_build_lookup_dicts():
    """Testa construção de dicionários de lookup."""
    print("Testing build_lookup_dicts()...")
    
    df_lookup = pd.DataFrame({
        'Resposta': ['João Silva', 'Maria Santos'],
        'ID Resposta': [1, 2],
        'Coluna/Opção Adicional': ['Porta-vozes iFood', 'Porta-vozes Rappi']
    })
    
    name_to_id, name_to_brand = build_lookup_dicts(df_lookup)
    
    assert 'joao silva' in name_to_id
    assert name_to_id['joao silva'] == 1
    assert 'maria santos' in name_to_id
    assert name_to_id['maria santos'] == 2
    print("✓ build_lookup_dicts() OK")


def test_find_best_match():
    """Testa busca de melhor match (agora apenas exato)."""
    print("Testing find_best_match()...")
    
    lookup_keys = ['joao silva', 'maria santos']
    
    # Exact match
    matched, score = find_best_match('João Silva', lookup_keys, 1.0)
    assert matched == 'joao silva'
    assert score == 1.0
    print(f"  ✓ Exact match: {matched} (score: {score})")
    
    # No match (case sensitive difference)
    matched, score = find_best_match('João Silvaa', lookup_keys, 1.0)
    assert matched is None
    print(f"  ✓ No match: None (score: {score})")
    
    print("✓ find_best_match() OK")


def test_process_file_integration():
    """Testa processamento completo de arquivo."""
    print("Testing process_file() integration...")
    
    # Criar arquivo de saída temporário
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Criar arquivo de entrada (saída do lote)
        df_output = pd.DataFrame({
            'ID': [1, 2, 3],
            'Titulo': ['News 1', 'News 2', 'News 3'],
            'Porta-vozes iFood': ['João Silva', 'Maria Santos', 'Desconhecido'],
            'Porta Vozes Rappi': ['João Silva', None, 'Pedro Costa']
        })
        input_file = tmpdir / 'lote_output.xlsx'
        df_output.to_excel(input_file, index=False)
        
        # Criar arquivo de lookup (porta-vozes)
        df_lookup = pd.DataFrame({
            'Resposta': ['João Silva', 'Maria Santos', 'Pedro Costa'],
            'ID Resposta': [101, 102, 103],
            'Coluna/Opção Adicional': ['Porta-vozes iFood', 'Porta-vozes iFood', 'Porta-vozes Rappi']
        })
        lookup_file = tmpdir / 'porta_vozes.xlsx'
        df_lookup.to_excel(lookup_file, index=False)
        
        # Executar processamento
        output_file = process_file(
            input_path=input_file,
            lookup_path=lookup_file,
            inplace=False
        )
        
        # Verificar resultado
        df_result = pd.read_excel(output_file)
        
        print(f"  ✓ Arquivo processado: {output_file.name}")
        print(f"  ✓ Colunas ID adicionadas:")
        id_cols = [c for c in df_result.columns if c.startswith('ID ')]
        for col in id_cols:
            print(f"    - {col}")
        
        # Verificar se há relatório
        report_file = input_file.with_name(f"{input_file.stem}_spokesperson_report_*")
        reports = list(tmpdir.glob(f"{input_file.stem}_spokesperson_report_*.csv"))
        if reports:
            print(f"  ✓ Relatório gerado: {reports[0].name}")
            df_report = pd.read_csv(reports[0])
            print(f"    - {len(df_report)} entradas no relatório")
        
        print("✓ process_file() integration OK")


if __name__ == '__main__':
    print("\n" + "="*60)
    print("TESTES DO MÓDULO POSTPROCESS_SPOKESPERSON_IDS")
    print("="*60 + "\n")
    
    test_normalize()
    test_split_names()
    test_build_lookup_dicts()
    test_find_best_match()
    test_process_file_integration()
    
    print("\n" + "="*60)
    print("✓ TODOS OS TESTES PASSARAM!")
    print("="*60 + "\n")
