# Pandora Joias — Price Scraper Pipeline

Solução de coleta, limpeza e visualização de dados de preços do varejo nacional para o site [pandorajoias.com.br](https://www.pandorajoias.com.br).

---

## O que foi feito

O sistema coleta automaticamente os preços de produtos do site da Pandora Joias, organiza os dados em camadas auditáveis e os disponibiliza em um dashboard interativo para acompanhamento pelos economistas.

A solução contempla:

- **Scraping resiliente** — requisições HTTP com retry automático e rotação de User-Agent
- **Arquitetura em medalhão** — Raw → Parsed → Trusted, cada camada reprocessável de forma independente
- **Detecção de mudanças no site** — monitor estrutural que grava alertas quando seletores CSS, formatos de preço ou padrões de URL mudam
- **Histórico de preços** — cada execução gera um snapshot, permitindo rastrear a variação de preço por produto ao longo do tempo
- **Dashboard interativo** — visualização em Streamlit com filtros, KPIs e gráfico de evolução por produto

---

## Arquitetura

```
pandora_scraper/
├── extractor.py      # Requisições HTTP → salva HTML bruto + metadados (camada Raw)
├── parser.py         # HTML → DataFrame via BeautifulSoup (camada Parsed)
├── transformer.py    # Limpeza e validação → salva em Parquet (camada Trusted)
├── monitor.py        # Detecta mudanças estruturais no HTML → grava alerts.log
├── main.py           # Orquestrador CLI — encadeia as etapas acima
├── dashboard.py      # Dashboard Streamlit — lê direto do Parquet
├── requirements.txt
└── data/
    ├── raw/          # HTML bruto + .meta.json (hash SHA-256, timestamp, status HTTP)
    ├── parsed/       # CSV + Parquet intermediários por execução
    └── trusted/      # Parquet final (snappy) + quality_report JSON por execução
```

### Fluxo de dados

```
Site Pandora
    │  HTTP GET com retry (tenacity)
    ▼
[Raw]     HTML + .meta.json  (imutável, auditável)
    │  BeautifulSoup + seletores com fallback
    ▼
[Parsed]  CSV + Parquet por run_id
    │  Polars — limpeza, validação, colunas derivadas
    ▼
[Trusted] Parquet (snappy) por run_id  ←── Dashboard lê aqui
```

---

## Como foi feito

### Extração (`extractor.py`)
- Usa `requests` com `tenacity` para retry exponencial em falhas de rede
- Salva cada página como `page_NNNN.html` com um `.meta.json` correspondente (URL, hash SHA-256, timestamp UTC, status HTTP)
- Escrita segura via arquivo `.tmp` — evita arquivos corrompidos em caso de interrupção
- Paginação detectada automaticamente: para quando o conteúdo da página seguinte é idêntico ao da anterior (comparação por hash)

### Parsing (`parser.py`)
- Seletores CSS definidos em dicionário central (`SELECTORS`) com múltiplos fallbacks — se o seletor primário falhar após uma mudança no site, alternativas mais genéricas são tentadas
- Cada campo é extraído de forma independente: falha em um campo não descarta o produto inteiro
- Após cada página parseada, aciona o `monitor.py` para verificação estrutural

### Transformação (`transformer.py`)
- Usa `Polars` para limpeza de alta performance
- Normaliza nomes de produtos (Title Case), padroniza metais para valores canônicos (ex: `"revestido a ouro"` → `"Banhado a Ouro 14k"`)
- Valida preços (descarta negativos e outliers acima de R$ 100.000)
- Adiciona colunas derivadas: `extraction_date`, `price_category`, `sizes_str`, `run_id`
- Gera `quality_report_{run_id}.json` com estatísticas de completude e distribuição de preços

### Monitor (`monitor.py`)
Após cada página processada, verifica:

| Check | Dispara alerta quando |
|---|---|
| Produtos zerados | 0 produtos encontrados na página |
| Mínimo absoluto | Menos de 10 produtos |
| Queda histórica | Mais de 30% abaixo da média das últimas 5 execuções |
| Padrão de link | Nenhum href terminando em `/p` |
| Formato de preço | Nenhum `R$ X.XXX,XX` no HTML |
| Título da página | Título diferente do registrado anteriormente |

Todos os alertas vão para `logs/structure_alerts.log` (arquivo acumulativo entre execuções).

### Orquestrador (`main.py`)
- CLI via `argparse` — sem inputs manuais, compatível com Airflow, Prefect, Cron
- Suporta duas frequências pré-configuradas:

| Frequência | Páginas | Delay | Indicado para |
|---|---|---|---|
| `daily` | todas (999) | 1,5 s | Execução diária completa |
| `hourly` | 2 por categoria | 1,0 s | Atualização horária rápida |

- Cada etapa pode rodar de forma independente (`--mode extract / parse / transform`)
- `run_id` baseado em timestamp UTC garante rastreabilidade entre camadas

---

## Como rodar

### 1. Requisitos de ambiente

| | Versão |
|---|---|
| **Python** | `>= 3.10` (testado com **3.13**) |

> O código usa sintaxe de anotação de tipos moderna (`str | None`, `list[str]`, `tuple[X, Y]`) introduzida no Python 3.10. Versões anteriores **não são suportadas**.

Verifique sua versão antes de instalar:

```bash
python --version
```

### 2. Instalar dependências

```bash
pip install -r requirements.txt
```


### 3. Executar a pipeline

```bash
# Coleta completa diária (todas as categorias, todas as páginas)
python main.py --frequency daily

# Coleta horária rápida (primeiras páginas)
python main.py --frequency hourly

# Categorias específicas
python main.py --frequency daily --categories colares aneis braceletes

# Etapas individualmente
python main.py --mode extract
python main.py --mode parse
python main.py --mode transform
```

### 4. Abrir o dashboard

```bash
python -m streamlit run dashboard.py
```

Acesse em: **http://localhost:8501**

### 5. Automatizar (Cron / Orquestrador)

```bash
# Diário às 6h
0 6 * * * cd /caminho/para/pandora_scraper && python main.py --frequency daily

# A cada hora
0 * * * * cd /caminho/para/pandora_scraper && python main.py --frequency hourly
```

---

## Dashboard

O dashboard lê os arquivos Parquet da camada Trusted e oferece:

- **Filtros** por categoria, material e faixa de preço
- **KPIs** — total de produtos, preço médio, mediana, menor e maior preço
- **Distribuição de preços** — histograma
- **Por material** — quantidade e preço médio por tipo de metal
- **Faixas de preço** — distribuição em gráfico de pizza
- **Scatter de preços** — um ponto por produto com hover interativo
- **Variação de preço por produto** — selecione qualquer produto e veja a evolução histórica de preço com delta absoluto e percentual entre execuções
- **Alertas estruturais** — painel com o conteúdo do `structure_alerts.log`

O histórico de variação por produto acumula automaticamente a cada nova execução da pipeline.

---
