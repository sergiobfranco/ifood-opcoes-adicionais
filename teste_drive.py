from google.oauth2 import service_account
from googleapiclient.discovery import build

def list_accessible_folders():
    SERVICE_ACCOUNT_FILE = 'config/google_drive_credentials.json'
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build('drive', 'v3', credentials=credentials)
    
    print("📧 Email da conta de serviço:", credentials.service_account_email)
    print("🔍 Buscando pastas acessíveis...")
    
    # Busca pastas com nome 'automacao'
    results = service.files().list(
        q="name='automacao' and mimeType='application/vnd.google-apps.folder' and trashed=false",
        fields="files(id, name, parents)",
        pageSize=10
    ).execute()
    
    folders = results.get('files', [])
    
    if folders:
        print(f"✅ Encontradas {len(folders)} pasta(s) 'automacao':")
        for folder in folders:
            print(f"   📁 {folder['name']} - ID: {folder['id']}")
    else:
        print("❌ Nenhuma pasta 'automacao' encontrada")
        
        # Lista algumas pastas disponíveis para debug
        print("\n🔍 Listando algumas pastas disponíveis...")
        results = service.files().list(
            q="mimeType='application/vnd.google-apps.folder' and trashed=false",
            fields="files(id, name)",
            pageSize=5
        ).execute()
        
        available_folders = results.get('files', [])
        if available_folders:
            print("📂 Pastas disponíveis:")
            for folder in available_folders:
                print(f"   - {folder['name']}: {folder['id']}")
        else:
            print("   Nenhuma pasta encontrada")

if __name__ == '__main__':
    list_accessible_folders()