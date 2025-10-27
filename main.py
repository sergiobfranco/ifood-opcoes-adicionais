"""
Pipeline principal de processamento de notícias iFood.
Orquestra todas as etapas do fluxo de trabalho.
"""

import logging
import sys
from pathlib import Path

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ifood_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Imports dos módulos
from config import settings
from src import setup_folders
from src import api_fetch
from src import spokesperson_identifier
from src import protagonist_analyzer
from src import unregistered_spokesperson_finder
from src import notes_analyzer
from src import delivery_establishments_identifier
from src import consolidator
from src import batch_update_creator
from src import batch_final_adjustment


def main():
    """Executa pipeline completo de processamento."""
    
    logger.info("=" * 80)
    logger.info("INICIANDO PIPELINE DE PROCESSAMENTO IFOOD")
    logger.info("=" * 80)
    
    try:
        # Etapa 1: Setup de pastas
        logger.info("\n[ETAPA 1/15] Configurando estrutura de pastas...")
        settings.create_folder_structure()
        
        # Etapa 2: Buscar dados da API
        logger.info("\n[ETAPA 2/15] Buscando dados da API...")
        final_df, final_df_small = api_fetch.fetch_api_data(
            config_file=settings.API_CONFIG_FILE,
            output_full=settings.arq_api_original,
            output_small=settings.arq_api
        )
        
        if final_df.empty:
            logger.error("Nenhum dado retornado da API. Abortando pipeline.")
            return
        
        logger.info(f"API retornou {len(final_df)} notícias")
        
        # Etapa 3: Identificar porta-vozes cadastrados
        logger.info("\n[ETAPA 3/15] Identificando porta-vozes cadastrados...")
        df_porta_vozes_encontrados = spokesperson_identifier.identify_spokespersons(
            df_news=final_df,
            spokesperson_file=settings.arq_porta_vozes,
            output_file=settings.arq_porta_vozes_encontrados
        )
        
        # Etapa 4: Analisar protagonismo (LLM)
        logger.info("\n[ETAPA 4/15] Analisando níveis de protagonismo...")
        df_protagonismo = protagonist_analyzer.analyze_protagonist(
            df_news=final_df,
            concepts_file=settings.arq_protagonismo,
            output_file=settings.arq_protagonismo_result,
            brands=settings.w_marcas
        )
        
        # Etapa 5: Identificar porta-vozes não cadastrados (LLM)
        logger.info("\n[ETAPA 5/15] Identificando porta-vozes não cadastrados...")
        df_pv_nao_cadastrados = unregistered_spokesperson_finder.find_unregistered(
            df_sem_porta_voz=df_porta_vozes_encontrados[
                df_porta_vozes_encontrados['Porta_Voz'] == "Sem porta-voz"
            ],
            df_news=final_df,
            output_file=settings.arq_porta_vozes_nao_cadastrados,
            valid_brands=settings.w_marcas
        )
        
        # Etapa 6: Analisar notas oficiais
        logger.info("\n[ETAPA 6/15] Analisando notas oficiais...")
        df_notas = notes_analyzer.analyze_notes(
            df_news=final_df,
            output_file=settings.arq_notas,
            brands=settings.w_marcas
        )
        
        # Etapa 7: Identificar estabelecimentos que usam delivery
        logger.info("\n[ETAPA 7/15] Identificando estabelecimentos delivery...")
        df_assuntos_atende = delivery_establishments_identifier.identify_establishments(
            df_news=final_df,
            output_file=settings.arq_assuntos_result_atende
        )
        
        # Cópia para compatibilidade
        df_assuntos_result = df_assuntos_atende.copy()
        
        # Etapa 8: Consolidação - Porta-vozes cadastrados
        logger.info("\n[ETAPA 8/15] Consolidando porta-vozes cadastrados...")
        df_consolidated = consolidator.consolidate_spokespersons(
            df_news=final_df,
            df_spokespersons=df_porta_vozes_encontrados
        )
        
        # Etapa 9: Consolidação - Porta-vozes não cadastrados
        logger.info("\n[ETAPA 9/15] Consolidando porta-vozes não cadastrados...")
        df_consolidated = consolidator.consolidate_unregistered_spokespersons(
            df_consolidated=df_consolidated,
            df_unregistered=df_pv_nao_cadastrados
        )
        
        # Etapa 10: Consolidação - Protagonismo
        logger.info("\n[ETAPA 10/15] Consolidando níveis de protagonismo...")
        df_consolidated = consolidator.consolidate_protagonist_level(
            df_consolidated=df_consolidated,
            df_protagonist=df_protagonismo
        )
        
        # Etapa 11: Consolidação - Notas
        logger.info("\n[ETAPA 11/15] Consolidando notas...")
        df_consolidated = consolidator.consolidate_notes(
            df_consolidated=df_consolidated,
            df_notes=df_notas
        )
        
        # Etapa 12: Consolidação - Assuntos
        logger.info("\n[ETAPA 12/15] Consolidando assuntos...")
        df_consolidated = consolidator.consolidate_subjects(
            df_consolidated=df_consolidated,
            df_subjects=df_assuntos_result
        )
        
        # Etapa 13: Filtrar e salvar consolidado final
        logger.info("\n[ETAPA 13/15] Salvando consolidado final...")
        df_consolidated = consolidator.filter_and_save_consolidated(
            df_consolidated=df_consolidated,
            output_path=settings.arq_consolidado
        )
        
        # Etapa 14: Criar planilha de atualização em lote
        logger.info("\n[ETAPA 14/15] Criando planilha de atualização em lote...")
        df_lote_final = batch_update_creator.create_batch_update_sheet(
            input_path=settings.arq_consolidado,
            output_path=settings.arq_lote_final
        )
        
        # Etapa 15: Adequação final do lote
        logger.info("\n[ETAPA 15/15] Realizando adequação final do lote...")
        df_lote_limpo = batch_final_adjustment.process_final_batch(
            df_lote_final=df_lote_final,
            df_original=final_df
        )
        
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE CONCLUÍDO COM SUCESSO!")
        logger.info("=" * 80)
        logger.info(f"\nArquivos gerados:")
        logger.info(f"  - Consolidado: {settings.arq_consolidado}")
        logger.info(f"  - Lote: {settings.arq_lote_final}")
        logger.info(f"  - Lote limpo: {settings.arq_lote_final_limpo}")
        logger.info(f"  - Arquivos timestamped no Google Drive")
        
    except Exception as e:
        logger.error(f"\n{'=' * 80}")
        logger.error(f"ERRO NO PIPELINE: {e}")
        logger.error(f"{'=' * 80}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()