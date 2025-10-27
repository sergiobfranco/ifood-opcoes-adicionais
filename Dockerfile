FROM python:3.11-slim

WORKDIR /app

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY . .

# Criar diretórios necessários
RUN mkdir -p uploads data/api data/partials data/output config

# Expor porta do Streamlit
EXPOSE 8501

# Comando padrão (será sobrescrito pelo docker-compose)
CMD ["streamlit", "run", "app.py"]