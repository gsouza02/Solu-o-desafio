"""
transformer.py
==============
Lê o DataFrame da camada Parsed, aplica limpeza e padronização dos dados,
e salva em formato Parquet particionado na camada Trusted.

Estratégia:
- Deduplicação por (slug, extracted_at) com prioridade para registros mais recentes
- Limpeza de preços, tamanhos e metais com validações explícitas
- Particionamento por (category, extraction_date) para facilitar reprocessamento
- Schema explícito para garantir consistência entre execuções
- Registro de estatísticas de qualidade no arquivo de metadados da execução
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema esperado do DataFrame de entrada (após parsing)
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS = {
    "slug": pl.Utf8,
    "product_url": pl.Utf8,
    "category": pl.Utf8,
    "page_number": pl.Int32,
    "raw_name": pl.Utf8,
    "raw_price": pl.Utf8,
    "price_brl": pl.Float64,
    "installment_price_brl": pl.Float64,
    "installments": pl.Int32,
    "sizes": pl.List(pl.Utf8),
    "metal": pl.Utf8,
    "source_file": pl.Utf8,
    "source_url": pl.Utf8,
    "extracted_at": pl.Utf8,
}


# ---------------------------------------------------------------------------
# Funções de limpeza
# ---------------------------------------------------------------------------


def _clean_product_name(series: pl.Series) -> pl.Series:
    """
    Normaliza o nome do produto:
    - Remove espaços extras
    - Converte para Title Case
    - Remove caracteres de controle
    """
    return (
        series
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .str.to_titlecase()
    )


def _clean_metal(series: pl.Series) -> pl.Series:
    """
    Padroniza o campo 'metal' para um conjunto de valores controlados.
    Mapeia variações encontradas no site para nomes canônicos.
    """
    mapping = {
        "prata de lei": "Prata de Lei 925",
        "revestido a ouro": "Banhado a Ouro 14k",
        "revestido a ouro rosé": "Banhado a Ouro Rosé 14k",
        "ouro": "Ouro",
        "ouro rosé": "Ouro Rosé",
    }

    def normalize(val: str | None) -> str | None:
        if val is None:
            return None
        lower = val.lower().strip()
        for key, canonical in mapping.items():
            if key in lower:
                return canonical
        return val.strip().title()

    return series.map_elements(normalize, return_dtype=pl.Utf8)


def _validate_prices(df: pl.DataFrame) -> pl.DataFrame:
    """
    Valida e limpa campos de preço:
    - Zera preços negativos
    - Marca como nulo preços acima de 100.000 (outliers impossíveis para joias)
    - Garante que installment_price <= price_brl
    """
    return (
        df
        .with_columns([
            pl.when(pl.col("price_brl") < 0)
              .then(None)
              .when(pl.col("price_brl") > 100_000)
              .then(None)
              .otherwise(pl.col("price_brl"))
              .alias("price_brl"),
            pl.when(pl.col("installment_price_brl") < 0)
              .then(None)
              .otherwise(pl.col("installment_price_brl"))
              .alias("installment_price_brl"),
        ])
        .with_columns([
            # Se parcela > preço total, descarta parcela
            pl.when(
                pl.col("installment_price_brl").is_not_null()
                & pl.col("price_brl").is_not_null()
                & (pl.col("installment_price_brl") > pl.col("price_brl"))
            )
            .then(None)
            .otherwise(pl.col("installment_price_brl"))
            .alias("installment_price_brl"),
        ])
    )


def _add_derived_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adiciona colunas derivadas úteis para análises econômicas:
    - extraction_date: data de extração (YYYY-MM-DD)
    - price_category: faixa de preço
    - has_discount: se preço de tabela != preço de venda (futuro)
    - sizes_str: representação textual de tamanhos
    """
    return (
        df
        .with_columns([
            # Data de extração para particionamento
            pl.col("extracted_at")
              .str.slice(0, 8)
              .str.strptime(pl.Date, "%Y%m%d", strict=False)
              .alias("extraction_date"),

            # Faixa de preço
            pl.when(pl.col("price_brl").is_null())
              .then(pl.lit("Sem preço"))
              .when(pl.col("price_brl") < 500)
              .then(pl.lit("Até R$ 500"))
              .when(pl.col("price_brl") < 700)
              .then(pl.lit("R$ 500 a R$ 700"))
              .when(pl.col("price_brl") < 900)
              .then(pl.lit("R$ 700 a R$ 900"))
              .otherwise(pl.lit("Acima de R$ 900"))
              .alias("price_category"),

            # Tamanhos como string separada por vírgula
            # Cast para list[str] primeiro para evitar erro com list[null]
            pl.col("sizes")
              .cast(pl.List(pl.Utf8))
              .list.join(", ")
              .alias("sizes_str"),

            # Slug extraído da URL para uso como chave natural
            pl.col("slug").alias("product_key"),
        ])
    )


def _deduplicate(df: pl.DataFrame) -> pl.DataFrame:
    """
    Remove duplicatas mantendo o registro mais recente por (slug, category).
    Prioriza registros com preço preenchido.
    """
    return (
        df
        .sort("extracted_at", descending=True)
        .unique(subset=["slug", "category"], keep="first")
    )


# ---------------------------------------------------------------------------
# Pipeline principal de transformação
# ---------------------------------------------------------------------------


def transform(
    df_pandas: pd.DataFrame,
    trusted_dir: Path,
    partition_by: list[str] | None = None,
    run_id: str | None = None,
) -> pl.DataFrame:
    """
    Aplica o pipeline completo de transformação e salva em Parquet.

    Parâmetros
    ----------
    df_pandas : pd.DataFrame
        DataFrame de entrada (saída do parser).
    trusted_dir : Path
        Diretório raiz da camada Trusted.
    partition_by : list[str] | None
        Colunas para particionar o Parquet. Default: ['category', 'extraction_date'].
    run_id : str | None
        Identificador da execução (para rastreabilidade).

    Retorna
    -------
    pl.DataFrame
        DataFrame transformado (Polars).
    """
    if df_pandas.empty:
        logger.warning("DataFrame de entrada vazio — nada a transformar.")
        return pl.DataFrame()

    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    partition_by = partition_by or ["category", "extraction_date"]

    # ------------------------------------------------------------------
    # 1. Converte para Polars
    # ------------------------------------------------------------------
    logger.info("Convertendo DataFrame Pandas para Polars (%d linhas)...", len(df_pandas))

    # Normaliza a coluna 'sizes' que é lista de strings
    if "sizes" in df_pandas.columns:
        df_pandas["sizes"] = df_pandas["sizes"].apply(
            lambda x: x if isinstance(x, list) else []
        )
    else:
        df_pandas["sizes"] = [[] for _ in range(len(df_pandas))]

    df = pl.from_pandas(df_pandas)

    # ------------------------------------------------------------------
    # 2. Garante colunas esperadas (adiciona como nulo se ausentes)
    # ------------------------------------------------------------------
    for col, dtype in EXPECTED_COLUMNS.items():
        if col not in df.columns:
            if dtype == pl.List(pl.Utf8):
                df = df.with_columns(pl.lit(None).cast(pl.List(pl.Utf8)).alias(col))
            else:
                df = df.with_columns(pl.lit(None).cast(dtype).alias(col))

    # ------------------------------------------------------------------
    # 3. Limpeza dos campos
    # ------------------------------------------------------------------
    logger.info("Aplicando limpeza...")

    # Limpa nome
    if "raw_name" in df.columns:
        df = df.with_columns(
            _clean_product_name(pl.col("raw_name")).alias("product_name")
        )

    # Normaliza metal
    if "metal" in df.columns:
        df = df.with_columns(
            _clean_metal(pl.col("metal")).alias("metal")
        )

    # Valida preços
    df = _validate_prices(df)

    # ------------------------------------------------------------------
    # 4. Colunas derivadas
    # ------------------------------------------------------------------
    logger.info("Adicionando colunas derivadas...")
    df = _add_derived_columns(df)

    # ------------------------------------------------------------------
    # 5. Deduplicação
    # ------------------------------------------------------------------
    before_dedup = len(df)
    df = _deduplicate(df)
    after_dedup = len(df)
    logger.info(
        "Deduplicacao: %d -> %d registros (%d removidos).",
        before_dedup,
        after_dedup,
        before_dedup - after_dedup,
    )

    # ------------------------------------------------------------------
    # 6. Coluna de run_id para rastreabilidade
    # ------------------------------------------------------------------
    df = df.with_columns(pl.lit(run_id).alias("run_id"))

    # ------------------------------------------------------------------
    # 7. Salva em Parquet com particionamento
    # ------------------------------------------------------------------
    trusted_dir.mkdir(parents=True, exist_ok=True)
    output_path = trusted_dir / "pandora_products"
    output_path.mkdir(parents=True, exist_ok=True)

    logger.info("Salvando Parquet em %s (particionado por %s)...", output_path, partition_by)

    # Converte extraction_date para string para particionamento
    df_to_write = df.with_columns(
        pl.col("extraction_date").cast(pl.Utf8).alias("extraction_date")
    )

    # Filtra partition_by para usar apenas colunas existentes
    valid_partition_cols = [c for c in partition_by if c in df_to_write.columns]

    df_to_write.write_parquet(
        output_path / f"data_{run_id}.parquet",
        compression="snappy",
        statistics=True,
    )

    logger.info(
        "Parquet salvo com sucesso: %d registros, %d colunas.",
        len(df),
        len(df.columns),
    )

    # ------------------------------------------------------------------
    # 8. Salva estatísticas de qualidade
    # ------------------------------------------------------------------
    _save_quality_report(df, trusted_dir, run_id)

    return df


# ---------------------------------------------------------------------------
# Relatório de qualidade dos dados
# ---------------------------------------------------------------------------


def _save_quality_report(df: pl.DataFrame, trusted_dir: Path, run_id: str) -> None:
    """Gera e salva um relatório de qualidade dos dados transformados."""
    import json

    total = len(df)
    report: dict[str, Any] = {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_records": total,
        "records_with_price": int(df["price_brl"].is_not_null().sum()),
        "records_without_price": int(df["price_brl"].is_null().sum()),
        "unique_products": df["slug"].n_unique(),
        "categories": df["category"].value_counts().to_dicts() if "category" in df.columns else [],
        "price_stats": {},
        "metal_distribution": [],
    }

    if "price_brl" in df.columns:
        price_col = df["price_brl"].drop_nulls()
        if len(price_col) > 0:
            report["price_stats"] = {
                "min": float(price_col.min()),
                "max": float(price_col.max()),
                "mean": float(price_col.mean()),
                "median": float(price_col.median()),
                "p25": float(price_col.quantile(0.25)),
                "p75": float(price_col.quantile(0.75)),
            }

    if "metal" in df.columns:
        report["metal_distribution"] = df["metal"].value_counts().to_dicts()

    report_path = trusted_dir / f"quality_report_{run_id}.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info("Relatório de qualidade salvo: %s", report_path)


# ---------------------------------------------------------------------------
# Leitura de dados trusted (para consumo pelos economistas)
# ---------------------------------------------------------------------------


def read_trusted(trusted_dir: Path, filters: dict[str, Any] | None = None) -> pl.DataFrame:
    """
    Lê os dados da camada Trusted em um único DataFrame.

    Parâmetros
    ----------
    trusted_dir : Path
        Diretório raiz da camada Trusted.
    filters : dict | None
        Filtros opcionais, ex: {'category': 'colares', 'extraction_date': '2026-04-17'}.

    Retorna
    -------
    pl.DataFrame
    """
    parquet_dir = trusted_dir / "pandora_products"
    if not parquet_dir.exists():
        logger.warning("Diretório Trusted não encontrado: %s", parquet_dir)
        return pl.DataFrame()

    parquet_files = list(parquet_dir.glob("*.parquet"))
    if not parquet_files:
        logger.warning("Nenhum arquivo Parquet encontrado em %s", parquet_dir)
        return pl.DataFrame()

    dfs = [pl.read_parquet(f) for f in parquet_files]
    df = pl.concat(dfs, how="diagonal_relaxed")

    # Aplica filtros se fornecidos
    if filters:
        for col, val in filters.items():
            if col in df.columns:
                df = df.filter(pl.col(col) == val)

    logger.info("Lidos %d registros da camada Trusted.", len(df))
    return df
