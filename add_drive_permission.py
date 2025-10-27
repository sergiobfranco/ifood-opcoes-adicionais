from google.oauth2 import service_account
from googleapiclient.discovery import build
import json

# Configurações
SERVICE_ACCOUNT_FILE = 'config/google_drive_credentials.json'
FOLDER_ID = input("Cole o ID da pasta AUTOMAÇÃO: ").strip()

# Carregar credenciais
with open(SERVICE_ACCOUNT_FILE, 'r') as f:
    creds_data = json.load(f)
    service_email = creds_data['client_email']

print(f"\nService Account: {service_email}")
print(f"Folder ID: {FOLDER_ID}")

# Criar serviço
SCOPES = ['https://www.googleapis.com/auth/drive']
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
service = build('drive', 'v3', credentials=credentials)

# Adicionar permissão
permission = {
    'type': 'user',
    'role': 'writer',
    'emailAddress': service_email
}

try:
    # Verificar se já tem acesso
    try:
        folder = service.files().get(fileId=FOLDER_ID, fields='name').execute()
        print(f"\n✅ Já tem acesso à pasta: {folder['name']}")
    except:
        print("\n❌ Sem acesso ainda. Tentando adicionar permissão...")
        
        # Adicionar permissão
        result = service.permissions().create(
            fileId=FOLDER_ID,
            body=permission,
            sendNotificationEmail=False,
            fields='id'
        ).execute()
        
        print(f"✅ Permissão adicionada! ID: {result['id']}")
        
        # Verificar novamente
        folder = service.files().get(fileId=FOLDER_ID, fields='name').execute()
        print(f"✅ Acesso confirmado à pasta: {folder['name']}")
        
except Exception as e:
    print(f"\n❌ Erro: {e}")
    print("\nIsso pode acontecer se você não tiver permissão para gerenciar compartilhamentos.")
    print("Tente as Soluções 1 ou 2.")