"""
Módulo para criação e verificação da estrutura de pastas.
Adaptado do ambiente Colab para ambiente local.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def setup_project_folders(base_path: Path) -> bool:
    """
    Cria estrutura de pastas do projeto.
    
    Args:
        base_path: Caminho base do projeto
    
    Returns:
        bool: True se sucesso
    """
    try:
        folders = [
            base_path / "data" / "api",
            base_path / "data" / "partials",
            base_path / "data" / "output",
            base_path / "config"
        ]
        
        for folder in folders:
            folder.mkdir(parents=True, exist_ok=True)
            logger.info(f"Pasta criada/verificada: {folder}")
        
        logger.info("Estrutura de pastas configurada com sucesso")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao criar estrutura de pastas: {e}")
        return False