"""
Módulo para análise de notas oficiais nas notícias.
Identifica expressões como 'em nota', 'informou', 'disse' e extrai o texto da nota.
"""

import pandas as pd
import re
import logging
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)

NOTE_EXPRESSIONS = [
    r"em nota",
    r"disse em nota",
    r"informou em nota",
    r"afirmou em nota",
    r"comunicou em nota",
    r"declarou em nota",
    r"por meio de nota",
    r"o iFood informou",
    r"o Rappi informou",
    r"o DoorDash informou",
    r"o Meituan informou",
    r"a Keeta informou",
    r"a 99 informou",
    r"a 99Food informou",
    r"o iFood disse",
    r"o Rappi disse",
    r"o DoorDash disse",
    r"o Meituan disse",
    r"a Keeta disse",
    r"a 99 disse",
    r"a 99Food disse",
    r"segundo o iFood",
    r"segundo o Rappi",
    r"segundo o DoorDash",
    r"segundo o Meituan",
    r"segundo a Keeta",
    r"segundo a 99",
    r"segundo a 99Food",
    r"de acordo com o iFood",
    r"de acordo com o Rappi",
    r"de acordo com o DoorDash",
    r"de acordo com o Meituan",
    r"de acordo com a Keeta",
    r"de acordo com a 99",
    r"de acordo com a 99Food"
]


def analyze_notes(
    df_news: pd.DataFrame,
    output_file: Path,
    brands: List[str]
) -> pd.DataFrame:
    """
    Analisa notícias em busca de notas oficiais e identifica marcas associadas.
    """
    logger.info("Analisando notas nas notícias...")
    
    df_notas = pd.DataFrame(columns=['Id', 'Titulo', 'Midia', 'Veiculo', 'Marca', 'Texto_Nota'])
    
    for _, row in df_news.iterrows():
        noticia_id = row['Id']
        titulo = str(row['Titulo']).strip()
        conteudo = str(row['Conteudo']).strip()
        midia = row['Midia']
        veiculo = row['Veiculo']
        
        texto_completo = f"Título: {titulo}\n\nConteúdo: {conteudo}"
        
        encontrou_expressao = False
        texto_nota = ""
        marcas_encontradas = []
        
        for expressao in NOTE_EXPRESSIONS:
            match = re.search(expressao, texto_completo, re.IGNORECASE)
            if match:
                encontrou_expressao = True
                texto_apos = texto_completo[match.end():].strip()
                texto_nota = texto_apos[:500] + ('...' if len(texto_apos) > 500 else '')
                
                for marca in brands:
                    if re.search(r'\b' + re.escape(marca) + r'\b', texto_nota, re.IGNORECASE):
                        marcas_encontradas.append(marca)
                
                if marcas_encontradas:
                    for marca_nota in marcas_encontradas:
                        new_row = pd.DataFrame([{
                            'Id': noticia_id,
                            'Titulo': titulo,
                            'Midia': midia,
                            'Veiculo': veiculo,
                            'Marca': marca_nota,
                            'Texto_Nota': texto_nota
                        }])
                        df_notas = pd.concat([df_notas, new_row], ignore_index=True)
                    break
                elif encontrou_expressao:
                    new_row = pd.DataFrame([{
                        'Id': noticia_id,
                        'Titulo': titulo,
                        'Midia': midia,
                        'Veiculo': veiculo,
                        'Marca': "Outra Marca/Entidade",
                        'Texto_Nota': texto_nota
                    }])
                    df_notas = pd.concat([df_notas, new_row], ignore_index=True)
                    break
        
        if not encontrou_expressao:
            new_row = pd.DataFrame([{
                'Id': noticia_id,
                'Titulo': titulo,
                'Midia': midia,
                'Veiculo': veiculo,
                'Marca': "NÃO",
                'Texto_Nota': None
            }])
            df_notas = pd.concat([df_notas, new_row], ignore_index=True)
    
    if not df_notas.empty:
        df_notas = df_notas.drop_duplicates(subset=['Id', 'Marca'], keep='first')
    
    # Filtrar
    df_notas_filtrado = df_notas[
        (df_notas['Marca'] != "NÃO") &
        (df_notas['Marca'] != "Outra Marca/Entidade")
    ].copy()
    
    df_notas_filtrado.to_excel(output_file, index=False)
    logger.info(f"Resultado salvo: {output_file} ({len(df_notas_filtrado)} registros)")
    
    return df_notas_filtrado