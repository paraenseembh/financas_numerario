# Numerário — Plataforma de Dados de Caixa e Cofres

Pipeline de dados para a área de **numerário** de instituições financeiras. Centraliza
movimentações de caixa, conferências de cofre, abastecimentos de ATMs e custódia diária
em uma arquitetura medalhão (Bronze → Prata → Ouro) sobre PostgreSQL.

---

## Arquitetura

```
Fontes                     Bronze              Prata              Ouro
──────────────────────     ──────────────────  ─────────────────  ──────────────────────
Planilhas Excel (.xlsx) ──▶ movimentacao_raw ──▶ movimentacao ──▶ fato_movimentacao
Sistema Core (JDBC)     ──▶ conferencia_raw  ──▶ conferencia  ──▶ fato_conferencia_cofre
CSV Transportadoras     ──▶ abastecimento_   ──▶ abastecimento──▶ fato_abastecimento_atm
Relatórios ATM          ──▶   atm_raw        ──▶   _atm       ──▶ fato_custodia_diaria
                        ──▶ custodia_raw     ──▶ custodia     ──▶ dim_agencia
                                                               ──▶ dim_denominacao
                                                               ──▶ dim_equipamento
                                                               ──▶ dim_tempo
                                                               ──▶ dim_operacao
                                                               ──▶ dim_transportadora
       [Ingestão bruta]     [TEXT, sem tipo]  [Tipado, limpo]  [Modelo estrela → Power BI]
```

---

## Estrutura de Pastas

```
numerario/
├── .env.example                 # Variáveis de ambiente necessárias
├── requirements.txt
├── README.md
├── infra/
│   └── init_db.sql              # Cria os schemas e permissões
├── bronze/
│   ├── ingestao_planilhas.py    # Ingere arquivos .xlsx
│   ├── ingestao_core.py         # Extrai do sistema core bancário
│   └── ingestao_transportadora.py  # Ingere CSV/TXT das transportadoras
├── prata/
│   ├── limpeza_movimentacao.py
│   ├── limpeza_conferencia.py
│   └── limpeza_atm.py
├── ouro/
│   └── carga_dimensional.sql    # Popula dimensões e fatos
├── sql/
│   ├── bronze_ddl.sql
│   ├── prata_ddl.sql
│   └── ouro_ddl.sql
├── dags/
│   └── dag_numerario_diaria.py  # DAG Airflow
└── tests/
    ├── test_bronze.py
    └── test_prata.py
```

---

## Setup

### 1. Pré-requisitos

- PostgreSQL 14+
- Python 3.11+
- (Opcional) Apache Airflow 2.8+

### 2. Banco de dados

```bash
# Criar banco e usuário
psql -U postgres -c "CREATE DATABASE numerario_db;"
psql -U postgres -c "CREATE USER numerario_user WITH PASSWORD 'sua_senha';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE numerario_db TO numerario_user;"

# Criar schemas
psql -U postgres -d numerario_db -f infra/init_db.sql

# Criar tabelas (ordem importa: bronze → prata → ouro)
psql -U numerario_user -d numerario_db -f sql/bronze_ddl.sql
psql -U numerario_user -d numerario_db -f sql/prata_ddl.sql
psql -U numerario_user -d numerario_db -f sql/ouro_ddl.sql
```

### 3. Dependências Python

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4. Variáveis de ambiente

```bash
cp .env.example .env
# Edite .env com as credenciais do banco e caminhos das pastas
```

---

## Execução Manual dos Pipelines

### Bronze — Ingestão de planilhas Excel

```bash
python bronze/ingestao_planilhas.py --pasta /data/entrada
```

### Bronze — Extração do sistema core

```bash
python bronze/ingestao_core.py --data-referencia 2024-01-15
# Para tabelas específicas:
python bronze/ingestao_core.py --data-referencia 2024-01-15 --tabelas movimentacao
```

### Bronze — Ingestão de transportadoras

```bash
python bronze/ingestao_transportadora.py --pasta /data/transportadora --separador ";"
```

### Prata — Limpeza

```bash
python prata/limpeza_movimentacao.py
python prata/limpeza_conferencia.py
python prata/limpeza_atm.py
```

### Ouro — Carga dimensional

```bash
psql -U numerario_user -d numerario_db \
  -v data_inicio='2024-01-01' \
  -v data_fim='2024-12-31' \
  -f ouro/carga_dimensional.sql
```

---

## Testes

```bash
# Da raiz do projeto
PYTHONPATH=. pytest tests/ -v
```

---

## Tabelas Principais

| Tabela | Schema | Descrição |
|---|---|---|
| `movimentacao_raw` | bronze | Movimentações de caixa/cofre brutas |
| `conferencia_cofre_raw` | bronze | Conferências manuais de cofre |
| `abastecimento_atm_raw` | bronze | Abastecimentos de ATMs |
| `custodia_diaria_raw` | bronze | Saldos físicos diários |
| `movimentacao` | prata | Movimentações limpas e tipadas |
| `conferencia_cofre` | prata | Conferências limpas |
| `abastecimento_atm` | prata | Abastecimentos limpos |
| `custodia_diaria` | prata | Custódia limpa |
| `fato_movimentacao_numerario` | ouro | Fato de movimentação (modelo estrela) |
| `fato_conferencia_cofre` | ouro | Fato de conferência |
| `fato_abastecimento_atm` | ouro | Fato de abastecimento de ATMs |
| `fato_custodia_diaria` | ouro | Fato de custódia diária |
| `dim_agencia` | ouro | Dimensão agência |
| `dim_denominacao` | ouro | Dimensão denominação (cédulas e moedas) |
| `dim_tempo` | ouro | Dimensão tempo com atributos calendário |

---

## Decisões de Design

### Por que Bronze / Prata / Ouro (Medalhão)?

A separação em camadas isola responsabilidades: o bronze preserva os dados exatamente como
chegaram (rastreabilidade e reprocessamento sem perda), a prata aplica limpeza e tipagem
(único ponto de verdade para transformações), e o ouro expõe um modelo otimizado para
consultas analíticas. Erros numa camada não propagam automaticamente para as seguintes.

### Por que PostgreSQL em vez de um data lake?

O volume de dados de numerário de uma instituição de médio porte cabe confortavelmente em
PostgreSQL. Isso evita a complexidade operacional de Spark/Delta Lake e mantém toda a stack
em SQL padrão, o que facilita auditoria, integração com Power BI via DirectQuery e operação
pela equipe de dados sem infraestrutura adicional.

### Por que todos os campos TEXT no Bronze?

Planilhas e arquivos legados chegam com formatações imprevisíveis (datas como "01/01/2024",
"01-01-2024" ou "44927", valores como "1.500,00" ou "1500.00"). Forçar tipos na ingestão
causaria falhas silenciosas. A tipagem e validação ficam encapsuladas na camada prata.

### Por que watermark por id_raw em vez de timestamp?

`id_raw SERIAL` é monotônico e imune a problemas de fuso horário ou clock skew entre
servidores. O watermark salvo em `prata.controle_processamento` permite reprocessamento
idempotente: basta resetar o `ultimo_id_raw` para zero.

### Por que ON CONFLICT DO NOTHING em vez de UPSERT (UPDATE)?

Dados de numerário são imutáveis após o fechamento do dia. Atualizações legítimas chegam
como novos registros. Usar `DO NOTHING` é mais seguro do que sobrescrever silenciosamente
dados já validados.
