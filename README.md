# Sentinela_Card — Sistema Antifraude em Tempo Real

[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/)
[![PySpark 3.5+](https://img.shields.io/badge/pyspark-3.5%2B-orange.svg)](https://spark.apache.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Sistema antifraude financeiro em tempo real para transações de cartão de crédito e débito construído com **Python**, **PySpark Structured Streaming** e **Spark SQL**.

Projetado com padrões arquiteturais de nível bancário e de fintechs, o **Sentinela_Card** monitora streams de transações, calcula features comportamentais com janelas deslizantes temporais, avalia regras de fraude configuráveis, combina scores de risco ponderados e persiste eventos de forma idempotente em **Parquet particionado**, permitindo auditoria e replay determinístico.

---

## Arquitetura do Sistema

```
                      [ Transações em Tempo Real ]
                 (Socket Streaming / Simulação Visa Direct)
                                   │
                                   ▼
                   ┌───────────────────────────────┐
                   │  PySpark Structured Streaming  │ (Watermark 10min)
                   │  - Batch + History Alignment  │
                   └───────────────┬───────────────┘
                                   │
                ┌──────────────────┴──────────────────┐
                ▼                                     ▼
   ┌───────────────────────────┐         ┌───────────────────────────┐
   │    Features de Janela     │         │    Features de Estado     │
   │  - velocity (1m/5m/1h/24h)│         │  - new_device / terminal  │
   │  - amount z-score (90d)   │         │  - new_city               │
   │  - haversine + speed_kmh  │         │  - chip_only_history      │
   │  - bin_denials (15m)      │         │  - dormant_days           │
   └────────────┬──────────────┘         └────────────┬──────────────┘
                └──────────────────┬──────────────────┘
                                   │
                                   ▼
                   ┌───────────────────────────────┐
                   │    Motor de Regras Puras      │
                   │  - card_cloning               │
                   │  - impossible_travel          │
                   │  - unusual_amount             │
                   │  - new_device_high_amount     │
                   │  - dormant_card_wake          │
                   │  - bin_attack                 │
                   │  - unusual_hour_and_place     │
                   └───────────────┬───────────────┘
                                   │
                                   ▼
                   ┌───────────────────────────────┐
                   │  Scoring Combiner & Threshold │
                   │  - score = Σ(pesos disparados)│
                   │  - APPROVE (<30)              │
                   │  - REVIEW (30..69)            │
                   │  - BLOCK (>=70)               │
                   └───────────────┬───────────────┘
                                   │
         ┌─────────────────────────┼─────────────────────────┐
         ▼                         ▼                         ▼
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│   Persistência   │      │ Console Stream   │      │ Spark SQL Engine │
│  - data/raw/     │      │ [APPROVE]        │      │ - Fraud Rate MCC │
│  - data/enriched/│      │ [REVIEW]         │      │ - Rule Precision │
│  - data/alerts/  │      │ [BLOCK]          │      │ - Client Ranking │
│  - data/audit/   │      │ com score & motivo│     │ - Backtest Tuning│
└──────────────────┘      └──────────────────┘      └──────────────────┘
```

---

## Schemas de Dados

### 1. Transação Bruta (Entrada)
| Campo | Tipo | Descrição |
| :--- | :--- | :--- |
| `transaction_id` | String | Identificador único da transação (`tx_...`) |
| `customer_id` | String | Identificador do cliente (`C...`) |
| `card_id` | String | Número do cartão (com BIN nos primeiros 6 dígitos) |
| `timestamp` | Timestamp | Data e hora UTC do evento |
| `amount` | Decimal(12,2) | Valor da transação em BRL |
| `currency` | String | Moeda (`BRL`) |
| `merchant_id` | String | Código do estabelecimento |
| `merchant_category` | String | Categoria MCC (ex: `5411`, `5732`, `7995`) |
| `merchant_city` | String | Cidade do estabelecimento no Brasil |
| `merchant_country` | String | País (`BR`) |
| `terminal_id` | String | Identificador do POS/Terminal |
| `entry_mode` | String | `chip`, `contactless`, `magnetic`, `manual`, `ecommerce` |
| `device_id` | String (null) | Identificador do dispositivo móvel/computador |
| `channel` | String | `pos`, `atm`, `online` |
| `auth_result` | String | `approved` ou `declined` |

### 2. Transação Enriquecida (Saída do Motor)
Contém todos os campos originais mais:
- **Velocidade**: `velocity_1min`, `velocity_5min`, `velocity_1h`, `velocity_24h`, `count_10min`, `sum_10min`, `min_10min`
- **Geografia**: `distance_from_last_km`, `travel_speed_kmh`, `minutes_since_last`, `new_city`
- **Perfil de Valor**: `amount_zscore`, `amount_vs_avg_ratio`, `amount_mean_90d`, `amount_std_90d`
- **Dispositivo & Terminal**: `new_terminal`, `new_device`, `entry_mode_chip_only_history`
- **Tempo & Comportamento**: `dormant_days`, `hour_of_day`, `bin_denials_15min`
- **Decisão & Risco**:
  - `risk_score`: Score combinado (0 a 100)
  - `risk_level`: `approve`, `review` ou `block`
  - `triggered_rules`: Lista de regras acionadas
  - `rule_reasons`: Justificativas textuais para cada regra disparada
  - `decision_timestamp`: Timestamp da tomada de decisão
  - `model_version`: Versão das regras (`1.0.0`)

---

## Regras de Detecção & Pesos

Configuradas dinamicamente via [rules.yaml](rules.yaml):

| Regra | Peso | Limiar / Gatilho | Assinatura Antifraude |
| :--- | :---: | :--- | :--- |
| **`impossible_travel`** | **80** | Velocidade implícita > 900 km/h e distância > 50 km | Duas compras em cidades distantes em minutos (ex: SP e Manaus em 10min). |
| **`bin_attack`** | **75** | >= 3 recusas em 15min no mesmo BIN | Ataque de força bruta / teste de numerações de cartão. |
| **`card_cloning`** | **55** | Teste <= R$10 seguido de compra >= 5x em < 10min ou tarja em cartão chip-only | Teste inicial de validade seguido de compra de alto valor em eletrônicos/joalherias. |
| **`unusual_hour_and_place`**| **50** | Compra entre 02h e 05h da madrugada em cidade nunca visitada | Uso noturno atípico fora da praça habitual do cliente. |
| **`new_device_high_amount`**| **45** | Dispositivo nunca visto com valor > 3x a média histórica | Invasão de conta / takeover em app ou e-commerce. |
| **`dormant_card_wake`** | **40** | Cartão inativo > 180 dias com gasto repentino > 2x a média | Reativação suspeita de cartão esquecido. |
| **`unusual_amount`** | **35** | Z-score >= 3.0 e Razão >= 1.5x contra histórico de 90 dias | Desvio estocástico extremo de ticket médio. |

### Matriz de Decisão:
- **`APPROVE`** : Score < 30 (Transação liberada imediatamente)
- **`REVIEW`**  : 30 <= Score < 70 (Encaminhada para esteira de análise / step-up auth)
- **`BLOCK`**   : Score >= 70 (Transação negada e cartão bloqueado)

---

## Guia de Instalação e Execução

### 1. Pré-requisitos
- Python 3.12+
- Java JDK 17+ (para execução do Apache Spark local)

Instale as dependências:
```bash
pip install -e ".[dev]"
```

---

### 2. Geração de Dados Sintéticos Realistas
Gera transações normais coerentes no tempo com perfis de gasto diário e rotas plausíveis, além de sequências de clonagem, impossible travel, ataques ao BIN e reativação dormante:
```bash
python -m app generate --transactions 100000 --output data/
```
Artefatos gerados:
- `data/seed_transactions.parquet`
- `data/metadata/fraud_labels.parquet` (Ground truth para backtest)
- `data/metadata/merchants.json` (Catálogo de nomes reais de estabelecimentos)

---

### 3. Processamento em Lote (Backfill)
Processa a base histórica com PySpark, calculando janelas deslizantes e particionando em Parquet por data (`dt=YYYY-MM-DD`):
```bash
python -m app backfill --source data/seed_transactions.parquet
```

---

### 4. Simulação de Streaming em Tempo Real
Inicia a simulação com socket local integrado e visualização colorida das decisões conforme as transações chegam:
```bash
# Simulação rápida
python -m app stream --source data/seed_transactions.parquet --speed fast --limit 50

# Simulação em tempo real com cadência natural
python -m app stream --source data/seed_transactions.parquet --speed realtime
```

**Exemplo de Saída no Console:**
```text
[APPROVE ] tx_000109283921 R$ 32,50 Padaria Central — score 0
[APPROVE ] tx_000109284102 R$ 115,00 Supermercado Bom Preço — score 0
[REVIEW  ] tx_000109284550 R$ 1.850,00 Eletrônicos X — score 45 (new_device_high_amount)
[BLOCK   ] tx_000109284910 R$ 4.200,00 Joia Ouro — score 90 (card_cloning, unusual_amount)
[BLOCK   ] tx_000109285320 R$ 7.900,00 Joalheria Brilhante — score 80 (impossible_travel)

=== Estatísticas da sessão ===
Processadas      : 50
Aprovadas        : 45
Revisão          : 2
Bloqueadas       : 3
Alertas          : 5
Score médio      : 9.4
Regras disparadas:
  - card_cloning: 2x
  - impossible_travel: 1x
  - unusual_amount: 2x
  - new_device_high_amount: 2x
```

---

### 5. Consultas Analíticas Spark SQL
Execute backtests e métricas de efetividade das regras:

```bash
# Taxa de fraude por categoria de comerciante (MCC)
python -m app analyze --query fraud_rate_by_category

# Ranking dos clientes com maior volume de alertas
python -m app analyze --query ranking_clients_by_alerts

# Efetividade e precisão por regra contra ground truth
python -m app analyze --query rule_effectiveness

# Falsos positivos por regra em transações normais
python -m app analyze --query false_positives_by_rule

# Distribuição de volume e alertas por hora do dia
python -m app analyze --query volume_by_hour

# Comparativo de cenários antes/depois de alteração de threshold
python -m app analyze --query threshold_comparison
```

---

### 6. Auditoria e Replay Idempotente
Verifica se o reprocessamento de uma transação contra o histórico produz a mesma decisão determinística:
```bash
python -m app replay --transaction-id tx_000109284910
```

---

### 7. Ajuste de Limiares (Threshold Tuning)
Ajuste os limiares de decisão diretamente no arquivo `rules.yaml`:
```bash
python -m app tune --threshold-review 40 --threshold-block 75
```

---

### 8. Exportação de Dados
Exporte tabelas consolidadas para integração com ferramentas de BI (PowerBI, Metabase) ou Data Lakes:
```bash
python -m app export --target alerts --format parquet --output exports/
python -m app export --target enriched --format csv --output exports/
```

---

## Suíte de Testes Automatizados

Execução dos testes unitários e end-to-end com `pytest`:
```bash
python -m pytest tests/ -v
```

### Cobertura de Testes:
1. **Features**: Testes unitários para Haversine, velocidade implícita, Z-score, razão de médias, janelas de velocidade e detecção de cartões inativos.
2. **Regras**: Validação isolada de cada uma das 7 regras com cenários positivos e negativos.
3. **Scoring & Thresholds**: Teste de soma ponderada, teto de score (100) e limiares de decisão.
4. **Idempotência**: Garantia de que reprocessar lotes duplicados não duplica alertas e que o replay é 100% determinístico.
5. **End-to-End**: Injeção de sequências completas de fraude com bloqueio imediato e validação de que falsos positivos em transações normais ficam **abaixo de 2%**.

---

## Limitações Documentadas

- **Abordagem Heurística / Regras de Negócio**: O sistema atual opera com regras determinísticas auditáveis e explicáveis. Não substitui modelos supervisionados contínuos de pontuação de crédito.
- **Integração de Bureaus**: Não realiza consultas síncronas a bureaus externos de dados cadastrais (ex: Serasa, Boa Vista).
- **Evolução para Machine Learning**: A arquitetura e as features calculadas (`amount_zscore`, `velocity_*`, `travel_speed_kmh`) foram projetadas com interfaces prontas para plugar modelos como **Isolation Forest** (anomalias não supervisionadas) ou **XGBoost / LightGBM** (classificação supervisionada com probabilidades contínuas calibradas).

---

## Escalando para Produção

Para transicionar da demonstração local para um cluster corporativo de alta volumetria:

1. **Mensageria**: Substituir o socket local por um cluster **Apache Kafka** ou **Redpanda** utilizando o conector `org.apache.spark:spark-sql-kafka-0-10`.
2. **Schema Registry**: Adicionar validação de contrato de dados com Confluent Schema Registry (Avro/Protobuf).
3. **State Store Distribuído**: Utilizar **RocksDB State Store** nativo do PySpark Structured Streaming para gerenciar o perfil acumulado por cliente em janelas arbitrárias sem gargalo de memória.
4. **Delta Lake / Iceberg**: Substituir o Parquet padrão por **Delta Lake** habilitando transações ACID, Time Travel e operações de `MERGE` de baixa latência.
5. **Notificações em Tempo Real**: Conectar o sink de alertas a filas de Webhooks/Push (SNS/SQS, Kafka topics de bloqueio síncrono com o autorizador do cartão).
