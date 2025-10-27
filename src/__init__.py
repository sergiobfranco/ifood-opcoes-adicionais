"""Pacote principal com módulos de processamento."""

from . import setup_folders
from . import api_fetch
from . import spokesperson_identifier
from . import protagonist_analyzer
from . import unregistered_spokesperson_finder
from . import notes_analyzer
from . import delivery_establishments_identifier
from . import consolidator
from . import batch_update_creator
from . import batch_final_adjustment

__all__ = [
    'setup_folders',
    'api_fetch',
    'spokesperson_identifier',
    'protagonist_analyzer',
    'unregistered_spokesperson_finder',
    'notes_analyzer',
    'delivery_establishments_identifier',
    'consolidator',
    'batch_update_creator',
    'batch_final_adjustment'
]