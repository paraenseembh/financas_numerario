"""
DAG Airflow — Pipeline diário de numerário.

Execução: dias úteis às 6h (schedule: "0 6 * * 1-5").
Fluxo: ingerir_planilhas >> ingerir_core >> limpar_movimentacao
       >> limpar_conferencia >> limpar_atm >> carregar_ouro >> validar_ouro
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

PASTA_PLANILHAS = os.getenv("PASTA_PLANILHAS", "/data/entrada")


def _alerta_falha(context: dict) -> None:
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exc     = context.get("exception")
    logger.error(
        "FALHA no pipeline | DAG: %s | Task: %s | Erro: %s",
        dag_id, task_id, exc,
    )


default_args = {
    "owner": "numerario",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _alerta_falha,
}


# ── Tasks ────────────────────────────────────────────────────────────────────

def _ingerir_planilhas(**_):
    from pathlib import Path
    import subprocess, sys
    script = Path(__file__).parent.parent / "bronze" / "ingestao_planilhas.py"
    result = subprocess.run(
        [sys.executable, str(script), "--pasta", PASTA_PLANILHAS],
        capture_output=True, text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"ingestao_planilhas falhou (código {result.returncode})")


def _ingerir_core(**context):
    from pathlib import Path
    import subprocess, sys
    data_ref = str(context["data_interval_start"].date())
    script   = Path(__file__).parent.parent / "bronze" / "ingestao_core.py"
    result   = subprocess.run(
        [sys.executable, str(script), "--data-referencia", data_ref],
        capture_output=True, text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"ingestao_core falhou (código {result.returncode})")


def _limpar_movimentacao(**_):
    from pathlib import Path
    import subprocess, sys
    script = Path(__file__).parent.parent / "prata" / "limpeza_movimentacao.py"
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"limpeza_movimentacao falhou (código {result.returncode})")


def _limpar_conferencia(**_):
    from pathlib import Path
    import subprocess, sys
    script = Path(__file__).parent.parent / "prata" / "limpeza_conferencia.py"
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"limpeza_conferencia falhou (código {result.returncode})")


def _limpar_atm(**_):
    from pathlib import Path
    import subprocess, sys
    script = Path(__file__).parent.parent / "prata" / "limpeza_atm.py"
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"limpeza_atm falhou (código {result.returncode})")


def _carregar_ouro(**_):
    import os
    from pathlib import Path
    import subprocess
    from datetime import date

    script = Path(__file__).parent.parent / "ouro" / "carga_dimensional.sql"
    data_inicio = str(date.today() - timedelta(days=1))
    data_fim    = str(date.today())

    cmd = [
        "psql",
        f"--host={os.environ['DB_HOST']}",
        f"--port={os.environ.get('DB_PORT', '5432')}",
        f"--dbname={os.environ['DB_NAME']}",
        f"--username={os.environ['DB_USER']}",
        f"-v", f"data_inicio={data_inicio}",
        f"-v", f"data_fim={data_fim}",
        "-f", str(script),
    ]
    env = os.environ.copy()
    env["PGPASSWORD"] = os.environ["DB_PASSWORD"]

    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"carga_dimensional.sql falhou (código {result.returncode})")


def _validar_ouro(**context):
    import os
    from sqlalchemy import create_engine, text

    ontem = str(context["data_interval_start"].date())
    url = (
        f"postgresql+psycopg2://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ.get('DB_PORT', 5432)}"
        f"/{os.environ['DB_NAME']}"
    )
    engine = create_engine(url)

    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT COUNT(*)
                FROM ouro.fato_movimentacao_numerario f
                JOIN ouro.dim_tempo t ON t.sk_tempo = f.sk_tempo
                WHERE t.data = :ontem
            """),
            {"ontem": ontem},
        ).fetchone()

    total = row[0] if row else 0
    logger.info("Registros em fato_movimentacao_numerario para %s: %d", ontem, total)

    if total == 0:
        raise ValueError(
            f"Validação falhou: nenhum registro em fato_movimentacao_numerario para {ontem}."
        )


# ── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="numerario_diaria",
    description="Pipeline ETL diário da área de numerário (Bronze → Prata → Ouro)",
    schedule_interval="0 6 * * 1-5",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["numerario", "etl"],
) as dag:

    t_planilhas = PythonOperator(
        task_id="ingerir_planilhas",
        python_callable=_ingerir_planilhas,
    )

    t_core = PythonOperator(
        task_id="ingerir_core",
        python_callable=_ingerir_core,
    )

    t_mov = PythonOperator(
        task_id="limpar_movimentacao",
        python_callable=_limpar_movimentacao,
    )

    t_conf = PythonOperator(
        task_id="limpar_conferencia",
        python_callable=_limpar_conferencia,
    )

    t_atm = PythonOperator(
        task_id="limpar_atm",
        python_callable=_limpar_atm,
    )

    t_ouro = PythonOperator(
        task_id="carregar_ouro",
        python_callable=_carregar_ouro,
    )

    t_valida = PythonOperator(
        task_id="validar_ouro",
        python_callable=_validar_ouro,
    )

    t_planilhas >> t_core >> t_mov >> t_conf >> t_atm >> t_ouro >> t_valida
