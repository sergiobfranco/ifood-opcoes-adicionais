"""
Script para testar upload no Google Drive.
Execute: python test_drive_upload.py
"""
import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent))

from src.google_drive_uploader import upload_file_to_drive, find_or_create_subfolder
from config.settings import GOOGLE_DRIVE_CREDENTIALS, GOOGLE_DRIVE_FOLDER_ID

def test_upload():
    print("=" * 60)
    print("TESTE DE UPLOAD PARA GOOGLE DRIVE")
    print("=" * 60)
    
    # Validações
    print(f"\n📋 Configurações:")
    print(f"   Credenciais: {GOOGLE_DRIVE_CREDENTIALS}")
    print(f"   Existe e é arquivo: {GOOGLE_DRIVE_CREDENTIALS.exists() and GOOGLE_DRIVE_CREDENTIALS.is_file()}")
    print(f"   Pasta ID: {GOOGLE_DRIVE_FOLDER_ID}")
    
    if not (GOOGLE_DRIVE_CREDENTIALS.exists() and GOOGLE_DRIVE_CREDENTIALS.is_file()):
        print("\n❌ ERRO: Arquivo de credenciais não encontrado ou não é um arquivo!")
        return
    
    if not GOOGLE_DRIVE_FOLDER_ID:
        print("\n❌ ERRO: GOOGLE_DRIVE_FOLDER_ID não configurado!")
        return
    
    # Criar arquivo de teste
    test_file = Path("test_upload.txt")
    test_file.write_text(f"Teste de upload - {Path(__file__).name}")
    print(f"\n✅ Arquivo de teste criado: {test_file}")
    
    try:
        # Buscar/criar subpasta 'lotes'
        print(f"\n🔍 Buscando/criando subpasta 'lotes'...")
        subfolder_id = find_or_create_subfolder(
            parent_folder_id=GOOGLE_DRIVE_FOLDER_ID,
            subfolder_name='lotes',
            credentials_path=GOOGLE_DRIVE_CREDENTIALS
        )
        print(f"✅ Subpasta ID: {subfolder_id}")
        
        # Upload do arquivo
        print(f"\n📤 Fazendo upload do arquivo...")
        result = upload_file_to_drive(
            file_path=test_file,
            folder_id=subfolder_id,
            credentials_path=GOOGLE_DRIVE_CREDENTIALS,
            mime_type='text/plain'
        )
        
        print("\n" + "=" * 60)
        print("✅ TESTE BEM-SUCEDIDO!")
        print("=" * 60)
        print(f"📄 Nome: {result['name']}")
        print(f"🆔 ID: {result['id']}")
        print(f"🔗 Link: {result['webViewLink']}")
        print("=" * 60)
        
    except Exception as e:
        print("\n" + "=" * 60)
        print("❌ TESTE FALHOU!")
        print("=" * 60)
        print(f"Erro: {e}")
        import traceback
        traceback.print_exc()
        print("=" * 60)
    
    finally:
        # Limpar arquivo de teste
        if test_file.exists():
            test_file.unlink()
            print(f"\n🗑️ Arquivo de teste removido")

if __name__ == "__main__":
    test_upload()