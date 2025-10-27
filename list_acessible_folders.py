"""
Diagnóstico completo de acesso ao Google Drive.
"""
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

CREDENTIALS = Path("config/google_drive_credentials.json")
SCOPES = ['https://www.googleapis.com/auth/drive']
TARGET_FOLDER_ID = "1870eoJP48o1qSu9TCiudksft_HKXe3nI"

def diagnose():
    print("=" * 70)
    print("DIAGNÓSTICO DE ACESSO AO GOOGLE DRIVE")
    print("=" * 70)
    
    credentials = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS),
        scopes=SCOPES
    )
    service = build('drive', 'v3', credentials=credentials)
    
    # Teste 1: Listar pastas acessíveis
    print("\n[TESTE 1] Listando pastas acessíveis pela conta de serviço...")
    print("-" * 70)
    
    query = "mimeType='application/vnd.google-apps.folder' and trashed=false"
    
    try:
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, shared, owners, webViewLink)',
            pageSize=50
        ).execute()
        
        files = results.get('files', [])
        
        if not files:
            print("❌ Nenhuma pasta encontrada!")
            print("\n💡 A conta de serviço não tem acesso a NENHUMA pasta.")
            print("   Isso confirma que o compartilhamento não está funcionando.")
        else:
            print(f"✅ Encontradas {len(files)} pasta(s):\n")
            for i, file in enumerate(files, 1):
                print(f"{i}. 📁 {file['name']}")
                print(f"   ID: {file['id']}")
                print(f"   Compartilhada: {file.get('shared', False)}")
                if file['id'] == TARGET_FOLDER_ID:
                    print("   🎯 ESTA É A PASTA ALVO!")
                print()
        
    except Exception as e:
        print(f"❌ Erro ao listar pastas: {e}")
    
    # Teste 2: Tentar acessar a pasta alvo diretamente
    print("\n[TESTE 2] Tentando acessar a pasta alvo diretamente...")
    print("-" * 70)
    print(f"ID: {TARGET_FOLDER_ID}")
    
    try:
        file = service.files().get(
            fileId=TARGET_FOLDER_ID,
            fields='id, name, shared, owners, capabilities, permissions'
        ).execute()
        
        print(f"✅ SUCESSO! A conta de serviço TEM acesso!")
        print(f"\n📁 Nome: {file['name']}")
        print(f"🆔 ID: {file['id']}")
        print(f"📤 Compartilhada: {file.get('shared', False)}")
        print(f"\n🔑 Capacidades:")
        caps = file.get('capabilities', {})
        print(f"   - Pode adicionar filhos: {caps.get('canAddChildren', False)}")
        print(f"   - Pode listar filhos: {caps.get('canListChildren', False)}")
        print(f"   - Pode editar: {caps.get('canEdit', False)}")
        
    except HttpError as e:
        if e.resp.status == 404:
            print("❌ ERRO 404: Pasta não encontrada")
            print("\n💡 Possíveis causas:")
            print("   1. ID da pasta está incorreto")
            print("   2. Conta de serviço NÃO foi adicionada ao compartilhamento")
            print("   3. Restrições do Google Workspace bloqueando acesso externo")
        elif e.resp.status == 403:
            print("❌ ERRO 403: Acesso negado")
            print("\n💡 A pasta existe mas a conta não tem permissão")
        else:
            print(f"❌ Erro HTTP {e.resp.status}: {e}")
    except Exception as e:
        print(f"❌ Erro: {e}")
    
    # Teste 3: Verificar e-mail da conta de serviço
    print("\n[TESTE 3] Informações da conta de serviço")
    print("-" * 70)
    try:
        about = service.about().get(fields='user').execute()
        user = about.get('user', {})
        print(f"📧 E-mail: {user.get('emailAddress', 'N/A')}")
        print(f"👤 Nome: {user.get('displayName', 'N/A')}")
        print("\n⚠️ IMPORTANTE: Verifique se ESTE e-mail está na lista de")
        print("   compartilhamento da pasta no Google Drive!")
    except Exception as e:
        print(f"❌ Erro ao obter informações: {e}")
    
    print("\n" + "=" * 70)
    print("FIM DO DIAGNÓSTICO")
    print("=" * 70)

if __name__ == "__main__":
    diagnose()