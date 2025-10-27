"""
Módulo para upload de arquivos para Google Drive.
Usa Service Account para autenticação.
"""

import logging
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/drive']


def get_drive_service(credentials_path: Path):
    """
    Cria serviço autenticado do Google Drive.
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(
            str(credentials_path),
            scopes=SCOPES
        )
        service = build('drive', 'v3', credentials=credentials)
        logger.info("Serviço Google Drive autenticado com sucesso")
        return service
    except Exception as e:
        logger.error(f"Erro ao autenticar Google Drive: {e}")
        raise


def upload_file_to_drive(
    file_path: Path,
    folder_id: str,
    credentials_path: Path,
    mime_type: str = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
) -> dict:
    """
    Faz upload de arquivo para pasta específica do Google Drive.
    
    Args:
        file_path: Caminho do arquivo local
        folder_id: ID da pasta destino no Drive
        credentials_path: Caminho do JSON de credenciais
        mime_type: Tipo MIME do arquivo
    
    Returns:
        dict: Metadados do arquivo enviado (id, name, webViewLink)
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    if not credentials_path.exists():
        raise FileNotFoundError(f"Credenciais não encontradas: {credentials_path}")
    
    logger.info(f"Iniciando upload: {file_path.name} para pasta {folder_id}")
    
    service = get_drive_service(credentials_path)
    
    # Metadados do arquivo
    file_metadata = {
        'name': file_path.name,
        'parents': [folder_id]
    }
    
    # Upload
    media = MediaFileUpload(str(file_path), mimetype=mime_type, resumable=True)
    
    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink, createdTime'
        ).execute()
        
        logger.info(f"Upload concluído: {file.get('name')}")
        logger.info(f"Drive ID: {file.get('id')}")
        logger.info(f"Link: {file.get('webViewLink')}")
        
        return file
        
    except Exception as e:
        logger.error(f"Erro no upload: {e}")
        raise


def list_files_in_folder(folder_id: str, credentials_path: Path, max_results: int = 10):
    """
    Lista arquivos em uma pasta do Drive.
    """
    service = get_drive_service(credentials_path)
    
    query = f"'{folder_id}' in parents and trashed=false"
    
    try:
        results = service.files().list(
            q=query,
            pageSize=max_results,
            fields="files(id, name, createdTime, webViewLink)",
            orderBy="createdTime desc"
        ).execute()
        
        files = results.get('files', [])
        logger.info(f"Encontrados {len(files)} arquivos na pasta")
        
        return files
        
    except Exception as e:
        logger.error(f"Erro ao listar arquivos: {e}")
        raise

def find_or_create_subfolder(
    parent_folder_id: str,
    subfolder_name: str,
    credentials_path: Path
) -> str:
    """
    Busca uma subpasta pelo nome. Se não existir, cria.
    
    Args:
        parent_folder_id: ID da pasta pai
        subfolder_name: Nome da subpasta desejada
        credentials_path: Caminho das credenciais
    
    Returns:
        str: ID da subpasta
    """
    service = get_drive_service(credentials_path)
    
    # Buscar subpasta existente
    query = f"name='{subfolder_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    
    try:
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)'
        ).execute()
        
        files = results.get('files', [])
        
        if files:
            # Subpasta já existe
            subfolder_id = files[0]['id']
            logger.info(f"Subpasta '{subfolder_name}' encontrada: {subfolder_id}")
            return subfolder_id
        else:
            # Criar subpasta
            logger.info(f"Subpasta '{subfolder_name}' não encontrada. Criando...")
            return create_folder(parent_folder_id, subfolder_name, credentials_path)
            
    except Exception as e:
        logger.error(f"Erro ao buscar/criar subpasta: {e}")
        raise


def create_folder(
    parent_folder_id: str,
    folder_name: str,
    credentials_path: Path
) -> str:
    """
    Cria uma nova pasta no Google Drive.
    
    Args:
        parent_folder_id: ID da pasta pai
        folder_name: Nome da nova pasta
        credentials_path: Caminho das credenciais
    
    Returns:
        str: ID da pasta criada
    """
    service = get_drive_service(credentials_path)
    
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_folder_id]
    }
    
    try:
        folder = service.files().create(
            body=file_metadata,
            fields='id, name'
        ).execute()
        
        logger.info(f"Pasta '{folder_name}' criada com ID: {folder.get('id')}")
        return folder.get('id')
        
    except Exception as e:
        logger.error(f"Erro ao criar pasta: {e}")
        raise    