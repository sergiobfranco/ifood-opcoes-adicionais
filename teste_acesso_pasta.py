from google.oauth2 import service_account
from googleapiclient.discovery import build

credentials = service_account.Credentials.from_service_account_file(
    '/app/config/google_drive_credentials.json',
    scopes=['https://www.googleapis.com/auth/drive.file']
)

service = build('drive', 'v3', credentials=credentials)

try:
    folder = service.files().get(fileId='1870eoJP48o1qSu9TCiudksft_HKXe3nI', fields='id, name').execute()
    print(f"✅ ACESSO OK! Pasta: {folder['name']}")
except Exception as e:
    print(f"❌ ERRO: {e}")