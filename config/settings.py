"""
Configurações globais do projeto iFood.
Adaptado para ambiente Docker com Streamlit.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional

import logging
logger = logging.getLogger(__name__)

load_dotenv()

# Caminhos base
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
INPUTS_PERSISTENTES_DIR = DATA_DIR / "inputs_persistentes"
UPLOADS_DIR = BASE_DIR / "uploads"

# Estrutura de pastas
PASTA_API = DATA_DIR / "api"
PASTA_PARTIALS = DATA_DIR / "partials"
PASTA_OUTPUT = DATA_DIR / "output"

# Pastas de lookups (arquivos de referência)
LOOKUPS_DIR = BASE_DIR / "config" / "lookups"
NIVEL_PROTAGONISMO_DIR = LOOKUPS_DIR / "nivel_protagonismo"
ESFORCO_DIR = LOOKUPS_DIR / "esforco"
NOTA_DIR = LOOKUPS_DIR / "nota"


def get_lookup_file(lookup_dir: Path) -> Optional[Path]:
    """
    Retorna o primeiro arquivo .xlsx encontrado na pasta de lookup.
    """
    if not lookup_dir.exists():
        return None
    
    xlsx_files = list(lookup_dir.glob("*.xlsx"))
    return xlsx_files[0] if xlsx_files else None


# Buscar arquivos de lookup automaticamente
arq_nivel_protagonismo_id = get_lookup_file(NIVEL_PROTAGONISMO_DIR)
arq_esforco_id = get_lookup_file(ESFORCO_DIR)
arq_nota_id = get_lookup_file(NOTA_DIR)


def create_folder_structure():
    """Cria estrutura de pastas necessária."""
    PASTA_API.mkdir(parents=True, exist_ok=True)
    PASTA_PARTIALS.mkdir(parents=True, exist_ok=True)
    PASTA_OUTPUT.mkdir(parents=True, exist_ok=True)
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    INPUTS_PERSISTENTES_DIR.mkdir(parents=True, exist_ok=True)
    
    # Criar pastas de lookups
    NIVEL_PROTAGONISMO_DIR.mkdir(parents=True, exist_ok=True)
    ESFORCO_DIR.mkdir(parents=True, exist_ok=True)
    NOTA_DIR.mkdir(parents=True, exist_ok=True)

# API Configuration
API_CONFIG_FILE = BASE_DIR / "config" / "api_marca_config.json"
USER_PREFERENCES_FILE = BASE_DIR / "config" / "user_preferences.json"

# Chave API DeepSeek
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

if not DEEPSEEK_API_KEY:
    raise ValueError("DEEPSEEK_API_KEY não encontrada nas variáveis de ambiente")

# Celery
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

# Arquivos de saída
arq_api_original = PASTA_API / "Favoritos_Marcas.xlsx"
arq_api = PASTA_API / "Favoritos_Marcas_small.xlsx"

# Arquivos intermediários
arq_porta_vozes_encontrados = PASTA_PARTIALS / "Porta_Vozes_Ifood_Encontrados_ID.xlsx"
arq_porta_vozes_nao_cadastrados = PASTA_PARTIALS / "Porta_Vozes_Ifood_Nao_Cadastrados.xlsx"
arq_protagonismo_result = PASTA_PARTIALS / "resultados_protagonismo.xlsx"
arq_notas = PASTA_PARTIALS / "Notas_Ifood.xlsx"
arq_assuntos_result_atende = PASTA_PARTIALS / "resultados_assuntos_atende.xlsx"
arq_assuntos_result = PASTA_PARTIALS / "resultados_assuntos.xlsx"

# Arquivos finais
arq_consolidado = PASTA_OUTPUT / "Favoritos_Marca_Consolidado.xlsx"
arq_lote_final = PASTA_OUTPUT / "Tabela_atualizacao_em_lote.xlsx"
arq_lote_final_limpo = PASTA_OUTPUT / "Tabela_atualizacao_em_lote_limpo.xlsx"

# Marcas
w_marcas = ['iFood', 'Rappi', 'DoorDash', 'Meituan', 'Keeta', '99', '99Food']


def create_folder_structure():
    """Cria estrutura de pastas necessária."""
    PASTA_API.mkdir(parents=True, exist_ok=True)
    PASTA_PARTIALS.mkdir(parents=True, exist_ok=True)
    PASTA_OUTPUT.mkdir(parents=True, exist_ok=True)
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    INPUTS_PERSISTENTES_DIR.mkdir(parents=True, exist_ok=True)  # ← ADICIONE ESTA LINHA

# Google Drive Configuration
GOOGLE_DRIVE_CREDENTIALS = BASE_DIR / "config" / "google_drive_credentials.json"
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

# Validações com feedback apropriado
if GOOGLE_DRIVE_CREDENTIALS.exists() and GOOGLE_DRIVE_CREDENTIALS.is_file():
    logger.info(f"✅ Credenciais do Google Drive encontradas: {GOOGLE_DRIVE_CREDENTIALS}")
else:
    logger.warning(f"⚠️ Arquivo de credenciais não encontrado (ou não é um arquivo): {GOOGLE_DRIVE_CREDENTIALS}")

if GOOGLE_DRIVE_FOLDER_ID:
    logger.info(f"✅ Pasta do Google Drive configurada: {GOOGLE_DRIVE_FOLDER_ID}")
else:
    logger.warning("⚠️ GOOGLE_DRIVE_FOLDER_ID não configurado no .env")