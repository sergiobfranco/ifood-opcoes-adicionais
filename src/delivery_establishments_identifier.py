"""
Módulo para identificação de estabelecimentos que atendem via iFood/Rappi.
Utiliza LLM para analisar menções a estabelecimentos e seu relacionamento com plataformas.
"""

import pandas as pd
import requests
import time
import logging
from datetime import datetime
from pathlib import Path

from config.settings import DEEPSEEK_API_KEY

logger = logging.getLogger(__name__)

API_URL = "https://api.deepseek.com/v1/chat/completions"
HEADERS = {
    "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
    "Content-Type": "application/json"
}

CONTEXT_EXAMPLES = """
Exemplos de frases que indicam uso das plataformas iFood/Rappi:

ATENDIMENTO DIRETO:
- "O grupo possui unidades no Rio Vermelho e Alphaville, além de delivery via iFood."
- "Além do buffet, há pratos à la carte com entrega via iFood e Rappi."

CONTEXTO COMERCIAL/ESTRATÉGICO:
- "Em 2024, a empresa cresceu 20% e ampliou seus canais de venda com a entrada no iFood."
- "produtos também estarão à venda via e-commerce, na página da WeCoffee no iFood"

PARCERIAS/COLABORAÇÕES:
- "O Rappi também participa, surpreendendo usuários com ações especiais"
"""


def identify_establishments(
    df_news: pd.DataFrame,
    output_file: Path
) -> pd.DataFrame:
    """
    Processa todas as notícias identificando estabelecimentos.
    """
    logger.info("Identificando estabelecimentos que usam delivery...")
    
    resultados = []
    
    for _, row in df_news.iterrows():
        noticia_id = row['Id']
        titulo = str(row['Titulo']).strip()
        conteudo = str(row['Conteudo']).strip()
        
        if not titulo and not conteudo:
            continue
        
        texto_completo = f"Título: {titulo}\n\nConteúdo: {conteudo}"
        
        prompt = f"""
Você é um especialista em análise de notícias sobre estabelecimentos comerciais e serviços de delivery.

Sua tarefa é analisar a notícia fornecida e identificar se ela menciona um estabelecimento específico que utiliza ou oferece serviços através das plataformas iFood e/ou Rappi.

**CRITÉRIOS PARA IDENTIFICAÇÃO:**
1. A notícia deve focar em um estabelecimento específico (restaurante, lanchonete, cafeteria, farmácia, etc.)
2. Deve mencionar que o estabelecimento utiliza iFood e/ou Rappi de alguma forma
3. O estabelecimento deve ser o foco principal da notícia

**FORMATO DA RESPOSTA:**
Se a notícia atender aos critérios, responda:
SIM | [Nome do Estabelecimento] atende via [plataforma(s)]

Se NÃO atender:
NÃO | Não se enquadra nos critérios

**CONTEXTO ADICIONAL:**
{CONTEXT_EXAMPLES}

Texto da Notícia:
{texto_completo}
"""
        
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {
                    "role": "system",
                    "content": "Você é um especialista em análise de notícias sobre estabelecimentos comerciais e serviços de delivery."
                },
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.1
        }
        
        try:
            logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Avaliando notícia ID {noticia_id}")
            response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
            response.raise_for_status()
            resposta = response.json()['choices'][0]['message']['content'].strip()
            
            partes = resposta.split(' | ')
            if len(partes) == 2 and partes[0].strip().upper() == "SIM":
                assunto = partes[1].strip()
                resultados.append({
                    'Id': noticia_id,
                    'Assunto': assunto,
                    'Metodologia_Aplicada': "Estabelecimento Atende Delivery"
                })
        
        except Exception as e:
            logger.error(f"Erro ao processar notícia ID {noticia_id}: {e}")
            continue
        
        time.sleep(1)
    
    df_result = pd.DataFrame(resultados)
    
    if not df_result.empty:
        df_result.to_excel(output_file, index=False)
        logger.info(f"Resultado salvo: {output_file} ({len(df_result)} registros)")
    else:
        logger.warning("Nenhum estabelecimento identificado")
    
    return df_result