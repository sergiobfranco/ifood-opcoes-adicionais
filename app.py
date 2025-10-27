"""
Interface Streamlit para o pipeline de processamento iFood.
Com persistência de arquivos enviados.
"""

import streamlit as st
import json
import time
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from celery.result import AsyncResult

# Definir timezone de São Paulo
tz_sp = ZoneInfo("America/Sao_Paulo")

from celery_app import celery_app
from tasks import process_pipeline
from config.settings import USER_PREFERENCES_FILE, UPLOADS_DIR, PASTA_OUTPUT, INPUTS_PERSISTENTES_DIR

def get_recent_files(directory: Path, pattern: str, days: int = 5):
    """
    Retorna arquivos que correspondem ao padrão dos últimos N dias.
    
    Args:
        directory: Pasta onde buscar
        pattern: Padrão do nome (ex: 'Tabela_atualizacao_em_lote_limpo_*.xlsx')
        days: Número de dias para buscar
    
    Returns:
        Lista de tuplas (caminho_arquivo, data_modificacao) ordenada por data (mais recente primeiro)
    """
    cutoff_date = datetime.now() - timedelta(days=days)
    files = []
    
    if not directory.exists():
        return files
    
    for file in directory.glob(pattern):
        try:
            mtime = datetime.fromtimestamp(file.stat().st_mtime)
            if mtime >= cutoff_date:
                files.append((file, mtime))
        except Exception as e:
            logger.warning(f"Erro ao processar arquivo {file}: {e}")
    
    # Ordenar por data (mais recente primeiro)
    return sorted(files, key=lambda x: x[1], reverse=True)

# Configuração da página
st.set_page_config(
    page_title="Pipeline iFood - Análise de Notícias",
    page_icon="🍔",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #EA1D2C;
        font-weight: bold;
        margin-bottom: 1rem;
    }
    .step-header {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border: 2px solid #f5c6cb;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


# Funções auxiliares
def load_preferences():
    """Carrega preferências salvas."""
    if USER_PREFERENCES_FILE.exists():
        with open(USER_PREFERENCES_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"files": {}, "last_updated": None}


def save_preferences(prefs):
    """Salva preferências do usuário."""
    prefs['last_updated'] = datetime.now().isoformat()
    with open(USER_PREFERENCES_FILE, 'w', encoding='utf-8') as f:
        json.dump(prefs, f, indent=2, ensure_ascii=False)


def save_to_persistent(uploaded_file, file_key):
    """Salva arquivo na pasta persistente com metadata do nome original."""
    INPUTS_PERSISTENTES_DIR.mkdir(exist_ok=True)
    
    # Nome padronizado interno
    filename = f"{file_key}.xlsx"
    file_path = INPUTS_PERSISTENTES_DIR / filename
    
    # Salvar arquivo
    with open(file_path, 'wb') as f:
        f.write(uploaded_file.getbuffer())
    
    # Salvar metadata com nome original
    metadata_file = INPUTS_PERSISTENTES_DIR / f"{file_key}_metadata.json"
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump({
            'original_name': uploaded_file.name,
            'saved_at': datetime.now().isoformat(),
            'size_bytes': uploaded_file.size
        }, f, indent=2)
    
    return str(file_path)

def get_file_metadata(file_key):
    """Retorna metadata do arquivo salvo."""
    metadata_file = INPUTS_PERSISTENTES_DIR / f"{file_key}_metadata.json"
    if metadata_file.exists():
        with open(metadata_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

def get_persistent_file(file_key):
    """
    Retorna caminho do arquivo persistente SE EXISTIR fisicamente.
    Retorna None se não existir.
    """
    filename = f"{file_key}.xlsx"
    file_path = INPUTS_PERSISTENTES_DIR / filename
    
    # Só retorna se o arquivo REALMENTE existir
    if file_path.exists() and file_path.is_file():
        return file_path
    else:
        return None


def list_persistent_files():
    """Lista todos os arquivos persistentes."""
    if not INPUTS_PERSISTENTES_DIR.exists():
        return {}
    
    files = {}
    for file_path in INPUTS_PERSISTENTES_DIR.glob("*.xlsx"):
        files[file_path.stem] = {
            'path': str(file_path),
            'name': file_path.name,
            'size': file_path.stat().st_size,
            'modified': datetime.fromtimestamp(file_path.stat().st_mtime)
        }
    return files

def clean_orphaned_metadata():
    """
    Remove metadatas de arquivos que não existem mais fisicamente.
    """
    if not INPUTS_PERSISTENTES_DIR.exists():
        return
    
    for metadata_file in INPUTS_PERSISTENTES_DIR.glob("*_metadata.json"):
        file_key = metadata_file.stem.replace('_metadata', '')
        xlsx_file = INPUTS_PERSISTENTES_DIR / f"{file_key}.xlsx"
        
        if not xlsx_file.exists():
            # Arquivo não existe, remover metadata órfão
            metadata_file.unlink()
            logger.info(f"Removido metadata órfão: {metadata_file.name}")

# Estado da sessão
if 'task_id' not in st.session_state:
    st.session_state.task_id = None
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'result' not in st.session_state:
    st.session_state.result = None


# Header
st.markdown('<p class="main-header">🍔 Pipeline iFood - Análise de Notícias</p>', unsafe_allow_html=True)
st.markdown("---")

# Sidebar
with st.sidebar:
    st.header("ℹ️ Sobre")
    st.markdown("""
    Este sistema processa notícias do iFood e concorrentes, realizando:
    - Identificação de porta-vozes
    - Análise de protagonismo (LLM)
    - Detecção de notas oficiais
    - Consolidação de dados
    - Geração de relatórios
    """)
    
    st.markdown("---")
    st.markdown("### ⏰ Agendamento Automático")
    
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    # Usar timezone de São Paulo
    tz_sp = ZoneInfo('America/Sao_Paulo')

    now = datetime.now(tz_sp)
    next_run = now.replace(hour=0, minute=1, second=0, microsecond=0)

    if now.hour > 0 or (now.hour == 0 and now.minute >= 1):
        next_run += timedelta(days=1)

    st.info(f"Próxima execução:\n\n**{next_run.strftime('%d/%m/%Y às %H:%M')}**")

    st.info("⚠️ O processamento pode levar de 30 minutos a 2 horas dependendo da quantidade de notícias.")
    
    st.markdown("---")
    
    # Mostrar arquivos salvos
    persistent_files = list_persistent_files()
    if persistent_files:
        st.success(f"📁 {len(persistent_files)} arquivo(s) salvo(s)")
        with st.expander("Ver arquivos salvos"):
            for key, info in persistent_files.items():
                # Tentar pegar nome original
                metadata = get_file_metadata(key)
                if metadata:
                    st.text(f"• {metadata['original_name']}")
                else:
                    st.text(f"• {info['name']}")
                st.caption(f"  Modificado: {info['modified'].strftime('%d/%m/%Y %H:%M')}")


# Carregar preferências
prefs = load_preferences()

# Abas principais
tab1, tab2, tab3 = st.tabs(["📤 Upload de Arquivos", "▶️ Processamento", "📥 Resultados"])

# TAB 1: Upload de Arquivos
with tab1:
    st.markdown('<div class="step-header"><h3>Passo 1: Arquivos de Entrada</h3></div>', unsafe_allow_html=True)
    
    # Limpar metadatas órfãos
    clean_orphaned_metadata()    

    # Mapeamento de arquivos
    file_config = {
        'porta_vozes': {'label': 'Porta-vozes Cadastrados', 'filename': 'Ifood_porta_vozes_ID.xlsx', 'required': True},
        'jornalistas': {'label': 'Jornalistas', 'filename': 'Jornalistas_Ifood_ID.xlsx', 'required': True},
        'assuntos': {'label': 'Assuntos', 'filename': 'Assuntos_ID.xlsx', 'required': True},
        'metodologia': {'label': 'Metodologia de Assuntos', 'filename': 'METODOLOGIA_ASSUNTOS.xlsx', 'required': True},
        'nivel_protagonismo_id': {'label': 'Nível Protagonismo ID', 'filename': 'nivel_protagonismo_ID.xlsx', 'required': False},
        'esforco_id': {'label': 'Esforço ID', 'filename': 'esforco_ID.xlsx', 'required': False},
        'nota_id': {'label': 'Nota ID', 'filename': 'nota_ID.xlsx', 'required': False}
    }
    
    uploaded_files = {}
    
    st.markdown("### 📋 Arquivos Obrigatórios")
    col1, col2 = st.columns(2)

    # Processar arquivos obrigatórios
    for idx, (key, config) in enumerate([item for item in file_config.items() if item[1]['required']]):
        col = col1 if idx % 2 == 0 else col2
        
        with col:
            st.markdown(f"**{idx+1}. {config['label']}**")
            
            # Verificar se arquivo existe FISICAMENTE
            file_path = INPUTS_PERSISTENTES_DIR / f"{key}.xlsx"
            
            if file_path.exists() and file_path.is_file():
                # Arquivo existe
                metadata = get_file_metadata(key)
                
                st.markdown('<div class="info-box">', unsafe_allow_html=True)
                st.write(f"✅ Usando arquivo salvo:")
                
                if metadata:
                    st.code(metadata['original_name'])
                    saved_date = datetime.fromisoformat(metadata['saved_at'])
                    st.caption(f"Enviado em: {saved_date.strftime('%d/%m/%Y às %H:%M')}")
                else:
                    st.code(file_path.name)
                
                col_a, col_b = st.columns([1, 1])
                with col_a:
                    use_saved = st.checkbox("Usar este arquivo", value=True, key=f"use_{key}")
                with col_b:
                    if st.button("🗑️ Remover", key=f"del_{key}"):
                        file_path.unlink()
                        metadata_file = INPUTS_PERSISTENTES_DIR / f"{key}_metadata.json"
                        if metadata_file.exists():
                            metadata_file.unlink()
                        st.rerun()
                
                st.markdown('</div>', unsafe_allow_html=True)
                
                if use_saved:
                    uploaded_files[key] = str(file_path)
                else:
                    new_file = st.file_uploader(
                        f"Enviar novo {config['filename']}",
                        type=['xlsx'],
                        key=f"upload_new_{key}"
                    )
                    if new_file:
                        path = save_to_persistent(new_file, key)
                        uploaded_files[key] = path
                        st.success(f"✅ Novo arquivo salvo!")
                        st.rerun()
            
            else:
                # Arquivo NÃO existe
                metadata = get_file_metadata(key)
                
                if metadata:
                    # Tinha metadata mas arquivo sumiu
                    st.markdown('<div class="error-box">', unsafe_allow_html=True)
                    st.error("⚠️ **Arquivo anterior foi removido!**")
                    st.caption(f"Último arquivo: {metadata['original_name']}")
                    st.caption("É necessário enviar novamente.")
                    st.markdown('</div>', unsafe_allow_html=True)
                    
                    # Limpar metadata órfão
                    metadata_file = INPUTS_PERSISTENTES_DIR / f"{key}_metadata.json"
                    if metadata_file.exists():
                        metadata_file.unlink()
                
                # Mostrar campo de upload
                new_file = st.file_uploader(
                    config['filename'],
                    type=['xlsx'],
                    key=f"upload_{key}",
                    help=f"Arquivo: {config['filename']}"
                )
                if new_file:
                    path = save_to_persistent(new_file, key)
                    uploaded_files[key] = path
                    st.success(f"✅ Arquivo salvo!")
                    st.rerun()

    st.markdown("---")

    # Validação e alerta global
    required_keys = [k for k, v in file_config.items() if v['required']]
    todos_obrigatorios_ok = all(k in uploaded_files for k in required_keys)

    if todos_obrigatorios_ok:
        st.markdown('<div class="success-box">✅ Todos os arquivos obrigatórios estão prontos!</div>', unsafe_allow_html=True)
    else:
        missing = [file_config[k]['label'] for k in required_keys if k not in uploaded_files]
        
        st.markdown('<div class="error-box">', unsafe_allow_html=True)
        st.error("🚨 **ATENÇÃO: Processamento NÃO pode ser executado!**")
        st.markdown(f"**Arquivos faltando:** {', '.join(missing)}")
        st.markdown("""
        **O que fazer:**
        1. Clique em "Browse files" para cada arquivo faltante
        2. Selecione os arquivos corretos
        3. Aguarde o upload completar
        4. Após todos os arquivos carregados, o processamento será liberado
        """)
        st.markdown('</div>', unsafe_allow_html=True)


    col3, col4, col5 = st.columns(3)
    
    st.markdown("---")
    st.markdown("### 📋 Arquivos de Lookup (Opcionais)")

    st.info("""
    Os arquivos de **Nível de Protagonismo ID**, **Esforço ID** e **Nota ID** devem ser colocados nas pastas:
    - `config/lookups/nivel_protagonismo/`
    - `config/lookups/esforco/`
    - `config/lookups/nota/`

    O sistema usará automaticamente qualquer arquivo `.xlsx` encontrado nestas pastas.
    """)

    # Mostrar status dos arquivos
    from config.settings import get_lookup_file, NIVEL_PROTAGONISMO_DIR, ESFORCO_DIR, NOTA_DIR

    # Buscar arquivos dinamicamente
    arq_nivel_protagonismo_id = get_lookup_file(NIVEL_PROTAGONISMO_DIR)
    arq_esforco_id = get_lookup_file(ESFORCO_DIR)
    arq_nota_id = get_lookup_file(NOTA_DIR)

    col_s1, col_s2, col_s3 = st.columns(3)

    with col_s1:
        if arq_nivel_protagonismo_id:
            st.success(f"✅ Protagonismo")
            st.caption(arq_nivel_protagonismo_id.name)
        else:
            st.warning("⚠️ Protagonismo não encontrado")

    with col_s2:
        if arq_esforco_id:
            st.success(f"✅ Esforço")
            st.caption(arq_esforco_id.name)
        else:
            st.warning("⚠️ Esforço não encontrado")

    with col_s3:
        if arq_nota_id:
            st.success(f"✅ Nota")
            st.caption(arq_nota_id.name)
        else:
            st.warning("⚠️ Nota não encontrado")
    
    # Validação
    required_keys = [k for k, v in file_config.items() if v['required']]
    todos_obrigatorios_ok = all(k in uploaded_files for k in required_keys)
    
    if todos_obrigatorios_ok:
        st.markdown('<div class="success-box">✅ Todos os arquivos obrigatórios estão prontos!</div>', unsafe_allow_html=True)
    else:
        missing = [file_config[k]['label'] for k in required_keys if k not in uploaded_files]
        st.warning(f"⚠️ Faltam arquivos: {', '.join(missing)}")


# TAB 2: Processamento
with tab2:
    st.markdown('<div class="step-header"><h3>Passo 2: Processamento</h3></div>', unsafe_allow_html=True)
    
    if not todos_obrigatorios_ok:
        st.markdown('<div class="error-box">', unsafe_allow_html=True)
        st.error("🚨 **Não é possível processar!**")
        st.markdown("Carregue todos os arquivos obrigatórios na aba **'Upload de Arquivos'** antes de continuar.")
        
        missing = [file_config[k]['label'] for k in required_keys if k not in uploaded_files]
        st.markdown(f"**Faltando:** {', '.join(missing)}")
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Desabilitar tudo nesta aba
        st.stop()        
    else:
        # Informação sobre execução automática
        st.markdown("### ⏰ Execução Automática Diária")
        
        st.info("""
        **O sistema está configurado para processar automaticamente todos os dias às 00:01.**
        
        **Como funciona:**
        1. Durante o dia, atualize os arquivos obrigatórios na aba "Upload de Arquivos"
        2. Às 00:01, o sistema iniciará automaticamente o processamento
        3. Não é necessário deixar esta página aberta
        4. No dia seguinte, acesse a aba "Resultados" para baixar os arquivos processados
        
        Os arquivos utilizados serão sempre os **últimos enviados** por você.
        """)
        
        # Mostrar próxima execução agendada
        # Definir timezone de São Paulo
        # tz_sp = pytz.timezone('America/Sao_Paulo')
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo

        # Usar timezone de São Paulo
        tz_sp = ZoneInfo('America/Sao_Paulo')
        now = datetime.now(tz_sp)
        next_run = now.replace(hour=0, minute=1, second=0, microsecond=0)
        # Se já passou das 00:01 hoje, agendar para amanhã
        if now.hour > 0 or (now.hour == 0 and now.minute >= 1):
            next_run += timedelta(days=1)
        
        col_info1, col_info2 = st.columns(2)
        with col_info1:
            st.metric("Próxima execução automática", next_run.strftime("%d/%m/%Y às %H:%M"))
        with col_info2:
            time_until = (next_run - now).total_seconds() / 3600
            st.metric("Tempo até próxima execução", f"{time_until:.1f} horas")
        
        st.markdown("---")
        
        # Opção de execução manual
        st.markdown("### ▶️ Execução Manual (Opcional)")
        
        st.warning("""
        Se você precisa processar **agora** (sem esperar até 00:01), use o botão abaixo.
        
        Isso é útil para:
        - Testes
        - Reprocessamento de dados
        - Processamento fora do horário normal
        """)
        
        col_btn1, col_btn2 = st.columns([1, 3])
        
        with col_btn1:
            start_button = st.button(
                "▶️ Processar Agora",
                type="secondary",
                disabled=st.session_state.processing,
                use_container_width=True
            )
        
        with col_btn2:
            if st.session_state.processing:
                st.info("Processamento em andamento... Não feche esta página.")
        
        # Iniciar processamento manual
        if start_button and not st.session_state.processing:
            save_preferences({"files": {k: v for k, v in uploaded_files.items()}})
            
            # Execução manual imediata
            task = process_pipeline.apply_async(args=[uploaded_files])
            st.session_state.task_id = task.id
            st.session_state.processing = True
            st.rerun()
        
        # Monitorar progresso (código existente mantido)
        if st.session_state.processing and st.session_state.task_id:
            st.markdown("### 📊 Progresso do Processamento")
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            log_container = st.expander("📋 Log Detalhado", expanded=True)
            
            task = AsyncResult(st.session_state.task_id, app=celery_app)
            
            with log_container:
                log_text = st.empty()
                logs = []
            
            # Polling do status
            while True:
                if task.state == 'PENDING':
                    status_text.info("Aguardando início do processamento...")
                    time.sleep(2)
                
                elif task.state == 'PROGRESS':
                    info = task.info
                    percent = info.get('percent', 0)
                    status = info.get('status', 'Processando...')
                    current = info.get('current', 0)
                    total = info.get('total', 15)
                    
                    progress_bar.progress(percent / 100)
                    status_text.info(f"Etapa {current}/{total}: {status}")
                    
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {status}")
                    log_text.text("\n".join(logs[-20:]))
                    
                    time.sleep(2)
                
                elif task.state == 'SUCCESS':
                    progress_bar.progress(100)
                    result = task.result
                    
                    if result['status'] == 'SUCCESS':
                        status_text.success("Processamento concluído com sucesso!")
                        st.session_state.result = result
                        st.session_state.processing = False
                        
                        st.markdown('<div class="success-box">', unsafe_allow_html=True)
                        st.markdown("### 📊 Estatísticas")
                        col_s1, col_s2, col_s3 = st.columns(3)
                        with col_s1:
                            st.metric("Total de Notícias", result['stats']['total_noticias'])
                        with col_s2:
                            st.metric("Porta-vozes Encontrados", result['stats']['porta_vozes_encontrados'])
                        with col_s3:
                            st.metric("Registros Consolidados", result['stats']['registros_consolidados'])
                        st.markdown('</div>', unsafe_allow_html=True)
                        
                        st.info("Vá para a aba 'Resultados' para baixar os arquivos.")
                    else:
                        status_text.error(f"Erro: {result['message']}")
                        with st.expander("Detalhes do Erro"):
                            st.code(result.get('traceback', 'Sem detalhes'))
                        st.session_state.processing = False
                    
                    break
                
                elif task.state == 'FAILURE':
                    progress_bar.progress(0)
                    status_text.error("Processamento falhou!")
                    st.error(str(task.info))
                    st.session_state.processing = False
                    break
                
                else:
                    status_text.warning(f"Estado desconhecido: {task.state}")
                    time.sleep(2)


# TAB 3: Resultados - Últimos 5 dias
with tab3:
    st.markdown('<div class="step-header"><h3>Passo 3: Download dos Resultados</h3></div>', unsafe_allow_html=True)

    # Função para buscar arquivos dos últimos N dias
    def get_recent_files(directory: Path, pattern: str, days: int = 5):
        """Retorna arquivos dos últimos N dias ordenados por data (mais recente primeiro)."""
        cutoff_date = datetime.now() - timedelta(days=days)
        files = []
        
        if not directory.exists():
            return files
        
        for file in directory.glob(pattern):
            try:
                mtime = datetime.fromtimestamp(file.stat().st_mtime)
                if mtime >= cutoff_date:
                    files.append((file, mtime))
            except Exception as e:
                logger.warning(f"Erro ao processar arquivo {file}: {e}")
        
        return sorted(files, key=lambda x: x[1], reverse=True)

    # Buscar arquivos dos últimos 5 dias
    lote_files = get_recent_files(
        directory=PASTA_OUTPUT,
        pattern='Tabela_atualizacao_em_lote_limpo_*.xlsx',
        days=5
    )

    pv_files = get_recent_files(
        directory=PASTA_OUTPUT,
        pattern='Porta_Vozes_Ifood_Nao_Cadastrados_*.xlsx',
        days=5
    )

    # Se não há nada disponível
    if not lote_files and not pv_files:
        st.info("ℹ️ Nenhum resultado disponível nos últimos 5 dias. Execute o processamento primeiro.")
    else:
        st.success(f"✅ Arquivos disponíveis para download (últimos 5 dias)")

        # Criar duas colunas
        col_d1, col_d2 = st.columns(2)

        # Coluna 1: Lote Final
        with col_d1:
            st.markdown("### 📄 Lote Final")
            
            if lote_files:
                for idx, (file_path, file_date) in enumerate(lote_files):
                    # Primeiro arquivo expandido, demais colapsados
                    is_expanded = (idx == 0)
                    
                    with st.expander(
                        f"📅 {file_date.strftime('%d/%m/%Y %H:%M:%S')}" + (" 🆕" if idx == 0 else ""),
                        expanded=is_expanded
                    ):
                        st.caption(f"📝 {file_path.name}")
                        
                        with open(file_path, 'rb') as f:
                            st.download_button(
                                label="⬇️ Baixar Arquivo" if idx > 0 else "⬇️ Baixar (Mais Recente)",
                                data=f,
                                file_name=file_path.name,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                type="primary" if idx == 0 else "secondary",
                                use_container_width=True,
                                key=f"download_lote_{file_path.stem}"
                            )
                        
                        # Mostrar tamanho do arquivo
                        file_size = file_path.stat().st_size / 1024  # KB
                        st.caption(f"💾 Tamanho: {file_size:.1f} KB")
            else:
                st.info("📭 Nenhum arquivo de Lote Final nos últimos 5 dias")

        # Coluna 2: Porta-vozes Não Cadastrados
        with col_d2:
            st.markdown("### 📋 Porta-vozes Não Cadastrados")
            
            if pv_files:
                for idx, (file_path, file_date) in enumerate(pv_files):
                    is_expanded = (idx == 0)
                    
                    with st.expander(
                        f"📅 {file_date.strftime('%d/%m/%Y %H:%M:%S')}" + (" 🆕" if idx == 0 else ""),
                        expanded=is_expanded
                    ):
                        st.caption(f"📝 {file_path.name}")
                        
                        with open(file_path, 'rb') as f:
                            st.download_button(
                                label="⬇️ Baixar Arquivo" if idx > 0 else "⬇️ Baixar (Mais Recente)",
                                data=f,
                                file_name=file_path.name,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                type="primary" if idx == 0 else "secondary",
                                use_container_width=True,
                                key=f"download_pv_{file_path.stem}"
                            )
                        
                        file_size = file_path.stat().st_size / 1024
                        st.caption(f"💾 Tamanho: {file_size:.1f} KB")
            else:
                st.info("📭 Nenhum arquivo de Porta-vozes nos últimos 5 dias")

        # Mostrar status do upload para Drive (do último processamento)
        result = st.session_state.result if st.session_state.result is not None else None
        if result and 'drive_upload' in result:
            st.markdown("---")
            st.markdown("### ☁️ Google Drive")
            drive_info = result['drive_upload']
            if drive_info.get('success'):
                st.success("✅ Arquivo salvo no Google Drive!")
                if 'path' in drive_info:
                    st.info(f"📂 Localização: {drive_info['folder']}")
                    st.code(drive_info['path'])
            else:
                reason = drive_info.get('reason', drive_info.get('error', 'desconhecido'))
                st.warning(f"⚠️ Não foi possível salvar no Drive: {reason}")

        # Botões de ação
        st.markdown("---")
        col_btn1, col_btn2 = st.columns(2)
        
        with col_btn1:
            if st.button("🔄 Atualizar Lista", use_container_width=True):
                st.rerun()
        
        with col_btn2:
            if st.button("🆕 Novo Processamento", type="secondary", use_container_width=True):
                st.session_state.task_id = None
                st.session_state.processing = False
                st.session_state.result = None
                st.rerun()


# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 1rem;'>
        <p>Pipeline iFood - Análise de Notícias | Versão 1.0</p>
        <p>Powered by Streamlit + Celery + Redis</p>
    </div>
    """,
    unsafe_allow_html=True
)