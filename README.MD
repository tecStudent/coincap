# Radar Cripto

Este projeto é uma dashboard de visualização e análise de criptomoedas construído com Python e Streamlit. Ele consome dados da API CoinCap, armazena-os em um banco PostgreSQL e exibe gráficos e tabelas interativas.

## Estrutura do Projeto

```
.
├── main.py
├── utils
│   ├── ingestion.py
│   ├── conection.py
│   └── view.py
├── .env
└── requirements.txt
```

- **main.py**: Orquestra o fluxo de ETL e inicia o dashboard.
- **utils/ingestion.py**: Funções de ETL para obter e transformar dados da API.
- **utils/conection.py**: Funções para criar, remover e ler tabelas no PostgreSQL.
- **utils/view.py**: Componentes Streamlit para exibição dos dados.

## Pré-requisitos

- Python 3.7 ou superior
- PostgreSQL (pode ser um serviço como Railway)
- Conta na CoinCap para obter API Key

## Instalação

1. Clone o repositório e acesse a pasta do projeto:
   ```bash
   git clone <URL-do-repositório>
   cd <nome-da-pasta>
   ```

2. Crie e ative um ambiente virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate    # Linux/MacOS
   venv\Scripts\activate     # Windows
   ```

3. Instale as dependências existentes:
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. (Opcional) Para gerar ou atualizar o arquivo `requirements.txt` com as versões atuais do seu ambiente:
   ```bash
   pip freeze > requirements.txt
   ```

## Configuração

Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```env
API_KEY='{"key":"<SEU_TOKEN_COINCAP>"}'
PGUSER=<USUARIO_POSTGRES>
POSTGRES_PASSWORD=<SENHA_POSTGRES>
RAILWAY_PRIVATE_DOMAIN=<HOST_DB>
PGPORT=<PORTA_DB>
PGDATABASE=<NOME_BANCO>
```

- **API_KEY**: String JSON com a chave de acesso à CoinCap.
- **Variáveis de conexão**: credenciais e endereço do seu banco PostgreSQL.

## Como Funciona

1. **ETL de Dados** (função `run_etl` em `main.py`):
   - `etl_rank_coin`: busca dados de ranking de criptomoedas e formata métricas diárias.
   - `get_top_5`: identifica as 5 maiores moedas por valor de mercado.
   - `etl_history_coin`: obtém histórico de preços diários das 5 principais moedas.
   - Remove (`drop_table`) e recria (`create_table`) as tabelas `rank_coin` e `history_coin` no banco.

2. **Dashboard Streamlit** (função `render_dashboard` em `utils/view.py`):
   - **Evolução de Preço**: gráfico de linha para o histórico de preços.
   - **% Não Emitida**: barra mostrando o percentual de moedas não emitidas.
   - **Tabela de Métricas**: DataFrame com principais indicadores financeiros.

## Executando a Aplicação

Basta rodar o comando:

```bash
streamlit run main.py
```

O Streamlit iniciará um servidor local e abrirá o dashboard no seu navegador.

---
