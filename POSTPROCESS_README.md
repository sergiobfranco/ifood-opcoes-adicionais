# Pós-Processamento de IDs de Porta-vozes

## O que faz

Este módulo (`src/postprocess_spokesperson_ids.py`) é uma **rotina de pós-processamento** que preenche automaticamente os IDs dos porta-vozes no arquivo final do lote.

### Fluxo

1. **Lê** arquivo de saída (Excel/CSV/JSON)
2. **Carrega** tabela de porta-vozes com IDs
3. **Normaliza** nomes (remove acentos, casefolding, trimming)
4. **Busca** matches exatos (case-insensitive)
5. **Preenche** coluna `ID {coluna_porta_vozes}`
6. **Gera** relatório com nomes não encontrados e ambíguos

## Uso

### Automático (integrado no pipeline)

Após cada execução do pipeline (via Streamlit ou Celery Beat), o pós-processamento roda automaticamente na **etapa 16** (última).

Preenche IDs em **ambos os arquivos**:
- `Tabela_atualizacao_em_lote_limpo.xlsx` (arquivo principal)
- `Tabela_atualizacao_em_lote_limpo_YYYYMMDD_HHMMSS.xlsx` (arquivo com timestamp)

Nenhuma ação necessária — tudo acontece automaticamente.

### Manual (via CLI)

```bash
# Uso básico
python src/postprocess_spokesperson_ids.py <arquivo_saida.xlsx> <arquivo_porta_vozes.xlsx>

# Exemplo
python src/postprocess_spokesperson_ids.py data/output/lote_final_limpo.xlsx config/Ifood_porta_vozes_ID.xlsx

# Sobrescrever arquivo de entrada (em vez de criar novo)
python src/postprocess_spokesperson_ids.py lote.xlsx porta_vozes.xlsx --inplace

# Com output verbose
python src/postprocess_spokesperson_ids.py lote.xlsx porta_vozes.xlsx --verbose
```

### Via código Python

```python
from pathlib import Path
from src.postprocess_spokesperson_ids import process_file

process_file(
    input_path=Path("data/output/lote_final.xlsx"),
    lookup_path=Path("config/porta_vozes.xlsx"),
    inplace=False            # True = sobrescrever, False = novo arquivo
)
```

## Parâmetros

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| `input` | path | - | Arquivo de saída (xlsx/csv/json) |
| `lookup` | path | - | Arquivo de porta-vozes (Excel) |
| `--inplace` | flag | false | Se presente, sobrescreve arquivo de entrada |
| `--verbose` | flag | false | Output debug ativo |

## Saídas

### 1. Arquivo atualizado
```
lote_final_limpo_with_ids_20260109_122345.xlsx
```
Contém as colunas originais + colunas `ID {coluna_porta_vozes}` preenchidas.

### 2. Relatório de inconsistências (se houver)
```
lote_final_limpo_spokesperson_report_20260109_122345.csv
```

Contém:
- `row`: índice da linha
- `col`: coluna de porta-vozes
- `name`: nome não encontrado
- `type`: 'not_found'

**Exemplo:**
```csv
row,col,name,type
0,Porta-vozes iFood,Desconhecido,not_found
```

## Normalização

A busca é **case-insensitive** e **accent-insensitive**:
- `João Silva` = `joao silva` ✓
- `Maria` = `MARIA` ✓
- `José` = `Jose` ✓

## Matching

**Apenas match exato** (case-insensitive). Não há fuzzy matching.

**Exemplos:**
```
"João Silva" → "João Silva" ✓ Match
"João Silvaa" → não encontrado ✗
"JOÃO SILVA" → "João Silva" ✓ Match
```

## Testes

```bash
# Rodar suite de testes
python test_postprocess_spokesperson_ids.py
```

Testa:
- Normalização de strings
- Split de múltiplos nomes
- Construção de lookup dicts
- Fuzzy matching
- Processamento completo de arquivo

## Logs

O módulo registra:
- Quantidade de nomes não encontrados
- Quantidade de matches fuzzy (ambíguos)
- Caminho dos arquivos gerados

Consulte os logs do Celery/Docker para detalhes.

## Troubleshooting

### "Nenhuma coluna de porta-vozes detectada"
- Verifique se as colunas contêm a palavra "porta" no nome
- Ajuste a heurística em `process_file()` se necessário

### Relatório muito grande (muitos não encontrados)
- Verifique se a tabela de porta-vozes está completa
- Considere fuzzy matching com cutoff menor

### Alguns IDs aparecem como vazio mesmo após processamento
- Pode ser fuzzy match ambíguo (score entre 0.85 e 0.999)
- Revise o relatório para ajustar cutoff ou tabela de lookup

## Configuração no pipeline

No `tasks.py`, a chamada é:
```python
postprocess_spokesperson_ids.process_file(
    input_path=settings.arq_lote_final_limpo,
    lookup_path=Path(uploaded_files['porta_vozes']),
    fuzzy_cutoff=0.85,
    inplace=True
)
```

Para desabilitar a etapa 16, comente ou remova essa chamada.
