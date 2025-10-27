"""
Tasks do Celery para processamento do pipeline iFood.
Inclui processamento manual e agendado.
"""

import logging
import traceback
from datetime import datetime
from pathlib import Path  # ← ADICIONE ESTA LINHA
from celery_app import celery_app
from celery import Task
from config import settings
from src import (
    spokesperson_identifier,
    protagonist_analyzer,
    notes_analyzer,
    consolidator,
    batch_final_adjustment,
    unregistered_spokesperson_finder,  # ← ADICIONE ESTA LINHA
    delivery_establishments_identifier,  # ← ADICIONE
    batch_update_creator  # ← ADICIONE
)

logger = logging.getLogger(__name__)

class CallbackTask(Task):
    """Classe base para tasks com callback de progresso."""
    
    def on_success(self, retval, task_id, args, kwargs):
        logger.info(f"Task {task_id} concluída com sucesso")
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error(f"Task {task_id} falhou: {exc}")


def _execute_pipeline(task_instance, uploaded_files: dict):
    """
    Função auxiliar que executa o pipeline completo.
    """
    def update_progress(current, total, status):
        task_instance.update_state(
            state='PROGRESS',
            meta={
                'current': current,
                'total': total,
                'status': status,
                'percent': int((current / total) * 100)
            }
        )
    
    total_steps = 15

    # Etapa 1: Setup
    settings.create_folder_structure()
    update_progress(1, total_steps, "Configurando ambiente...")
    
    # Etapa 2: Buscar API
    update_progress(2, total_steps, "Buscando notícias da API...")
    
    # Importar função dentro da execução
    from src.api_fetch import fetch_api_data
    
    final_df, df_small = fetch_api_data(
        config_file=settings.BASE_DIR / "config" / "api_marca_config.json",
        output_full=settings.PASTA_API / "Favoritos_Marca_API.xlsx",
        output_small=settings.PASTA_API / "Favoritos_Marca_API_small.xlsx",
        max_retries=3
    )

    logger.info(f"API retornou {len(final_df)} notícias (completo)")
    logger.info(f"Colunas: {final_df.columns.tolist()}")

    if final_df.empty:
        return {
            'status': 'FAILURE',
            'message': 'API não retornou notícias'
        }

    if final_df.empty:
        return {
            'status': 'FAILURE',
            'message': 'API não retornou notícias'
        }

    try:
        # Etapa 3: Carregar porta-vozes
        logger.info("Carregando porta-vozes cadastrados...")
        
        update_progress(3, total_steps, "Identificando porta-vozes cadastrados...")
        df_porta_vozes = spokesperson_identifier.identify_spokespersons(
            df_news=final_df,
            spokesperson_file=Path(uploaded_files['porta_vozes']),
            output_file=settings.arq_porta_vozes_encontrados
        )
        
        # Etapa 4: Protagonismo
        update_progress(4, total_steps, "Analisando protagonismo (pode demorar)...")
        # NOTA: Removido arquivo de conceitos - usar hardcoded ou criar dinamicamente
        df_protagonismo = protagonist_analyzer.analyze_protagonist_simplified(
            df_news=final_df,
            output_file=settings.arq_protagonismo_result,
            brands=settings.w_marcas
        )
        
        # Etapa 5: Porta-vozes não cadastrados
        update_progress(5, total_steps, "Identificando porta-vozes não cadastrados...")
        df_pv_nao_cad = unregistered_spokesperson_finder.find_unregistered(
            df_sem_porta_voz=df_porta_vozes[df_porta_vozes['Porta_Voz'] == "Sem porta-voz"],
            df_news=final_df,
            output_file=settings.arq_porta_vozes_nao_cadastrados,
            valid_brands=settings.w_marcas
        )
        
        # Etapa 6: Notas
        update_progress(6, total_steps, "Analisando notas oficiais...")
        df_notas = notes_analyzer.analyze_notes(
            df_news=final_df,
            output_file=settings.arq_notas,
            brands=settings.w_marcas
        )
        
        # Etapa 7: Estabelecimentos delivery
        update_progress(7, total_steps, "Identificando estabelecimentos...")
        df_assuntos = delivery_establishments_identifier.identify_establishments(
            df_news=final_df,
            output_file=settings.arq_assuntos_result_atende
        )
        
        df_assuntos_result = df_assuntos.copy()
        
        # Etapas 8-12: Consolidação
        update_progress(8, total_steps, "Consolidando dados (1/5)...")
        df_consol = consolidator.consolidate_spokespersons(final_df, df_porta_vozes)
        
        update_progress(9, total_steps, "Consolidando dados (2/5)...")
        df_consol = consolidator.consolidate_unregistered_spokespersons(df_consol, df_pv_nao_cad)
        
        update_progress(10, total_steps, "Consolidando dados (3/5)...")
        df_consol = consolidator.consolidate_protagonist_level(df_consol, df_protagonismo)
        
        update_progress(11, total_steps, "Consolidando dados (4/5)...")
        df_consol = consolidator.consolidate_notes(df_consol, df_notas)
        
        update_progress(12, total_steps, "Consolidando dados (5/5)...")
        df_consol = consolidator.consolidate_subjects(df_consol, df_assuntos_result)
        
        # Etapa 13: Salvar consolidado
        update_progress(13, total_steps, "Salvando consolidado...")
        df_consol = consolidator.filter_and_save_consolidated(df_consol, settings.arq_consolidado)
        
        # Etapa 14: Lote
        update_progress(14, total_steps, "Criando planilha de lote...")
        df_lote = batch_update_creator.create_batch_update_sheet(
            settings.arq_consolidado,
            settings.arq_lote_final
        )
        
        # Etapa 15: Adequação final
        update_progress(15, total_steps, "Finalizando...")
        df_final = batch_final_adjustment.process_final_batch(df_lote, final_df)

        result = {
            'status': 'SUCCESS',
            'message': 'Pipeline concluído com sucesso!',
            'files': {
                'consolidado': str(settings.arq_consolidado),
                'lote': str(settings.arq_lote_final),
                'lote_limpo': str(settings.arq_lote_final_limpo),
                'pv_nao_cadastrados': str(settings.arq_porta_vozes_nao_cadastrados)
            },
            'stats': {
                'total_noticias': len(final_df),
                'porta_vozes_encontrados': len(df_porta_vozes),
                'registros_consolidados': len(df_consol)
            }
        }


        # Upload para Google Drive (via API)
        try:
            from src.google_drive_uploader import upload_file_to_drive, find_or_create_subfolder
            from config.settings import GOOGLE_DRIVE_CREDENTIALS, GOOGLE_DRIVE_FOLDER_ID
            
            if GOOGLE_DRIVE_CREDENTIALS.exists() and GOOGLE_DRIVE_CREDENTIALS.is_file() and GOOGLE_DRIVE_FOLDER_ID:
                logger.info("Iniciando upload para Google Drive...")
                
                # Buscar/criar subpasta "lotes"
                subfolder_id = find_or_create_subfolder(
                    parent_folder_id=GOOGLE_DRIVE_FOLDER_ID,
                    subfolder_name='lotes',
                    credentials_path=GOOGLE_DRIVE_CREDENTIALS
                )
                
                # Upload para subpasta
                drive_file = upload_file_to_drive(
                    file_path=settings.arq_lote_final_limpo,
                    folder_id=subfolder_id,
                    credentials_path=GOOGLE_DRIVE_CREDENTIALS
                )
                
                logger.info(f"Arquivo enviado para Google Drive: {drive_file.get('webViewLink')}")
                
                result['drive_upload'] = {
                    'success': True,
                    'file_id': drive_file.get('id'),
                    'link': drive_file.get('webViewLink'),
                    'folder': 'AUTOMAÇÃO/lotes'
                }
            else:
                logger.warning("Upload para Google Drive não configurado")
                result['drive_upload'] = {'success': False, 'reason': 'não configurado'}
                
        except Exception as e:
            logger.error(f"Erro no upload para Google Drive: {e}")
            result['drive_upload'] = {'success': False, 'error': str(e)}

        return result


        
    except Exception as e:
        logger.exception("Erro durante processamento:")
        return {
            'status': 'FAILURE',
            'message': str(e),
            'traceback': traceback.format_exc()
        }

# 2. TASK PARA EXECUÇÃO MANUAL (wrapper simples)
@celery_app.task(bind=True, base=CallbackTask)
def process_pipeline(self, uploaded_files: dict):
    """
    Task para processamento manual via interface Streamlit.
    """
    return _execute_pipeline(self, uploaded_files)

# 3. TASK PARA EXECUÇÃO AGENDADA
@celery_app.task(bind=True, base=CallbackTask)
def process_pipeline_scheduled(self):
    """
    Task executada automaticamente pelo Celery Beat às 00:01.
    Usa os últimos arquivos salvos em inputs_persistentes.
    """
    from pathlib import Path
    
    logger.info("=" * 80)
    logger.info("INICIANDO PROCESSAMENTO AGENDADO (CELERY BEAT)")
    logger.info("=" * 80)
    
    # Buscar últimos arquivos enviados
    uploaded_files = {
        'porta_vozes': str(settings.INPUTS_PERSISTENTES_DIR / 'porta_vozes.xlsx'),
        'jornalistas': str(settings.INPUTS_PERSISTENTES_DIR / 'jornalistas.xlsx'),
        'assuntos': str(settings.INPUTS_PERSISTENTES_DIR / 'assuntos.xlsx'),
        'metodologia': str(settings.INPUTS_PERSISTENTES_DIR / 'metodologia.xlsx')
    }
    
    # Validar arquivos
    missing_files = []
    for key, path in uploaded_files.items():
        if not Path(path).exists():
            logger.error(f"Arquivo obrigatório não encontrado: {key} em {path}")
            missing_files.append(key)
    
    if missing_files:
        error_msg = f"Arquivos faltando: {', '.join(missing_files)}"
        logger.error(error_msg)
        return {
            'status': 'FAILURE',
            'message': error_msg,
            'missing_files': missing_files
        }
    
    logger.info("Todos os arquivos obrigatórios encontrados")
    
    # Executar pipeline
    return _execute_pipeline(self, uploaded_files)