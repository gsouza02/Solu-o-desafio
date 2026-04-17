"""
main.py
=======
Ponto de entrada da pipeline de coleta de dados da Pandora Joias.
Recebe parâmetros via CLI (argparse) e orquestra extractor -> parser -> transformer.

Frequências suportadas
----------------------
  daily   — Extrai todas as páginas de todas as categorias (cobertura total).
             Indicado para execução uma vez ao dia (ex: 06h).
             Defaults: max_pages=999, delay=1.5s

  hourly  — Extrai apenas as primeiras páginas (novidades/preços recentes).
             Indicado para execução a cada hora.
             Defaults: max_pages=2, delay=1.0s

Exemplos de uso
---------------
# Execução diária completa
    python main.py --frequency daily

# Execução horária (rápida)
    python main.py --frequency hourly

# Execução completa manual
    python main.py --mode full

# Apenas extração (salva HTML bruto)
    python main.py --mode extract

# Apenas parsing do Raw já existente
    python main.py --mode parse

# Apenas transformação do Parsed já existente
    python main.py --mode transform

# Categorias específicas
    python main.py --mode full --categories colares aneis braceletes

# Limitar páginas manualmente
    python main.py --mode full --max-pages 3

# Especificar data de referência para auditoria
    python main.py --mode full --run-date 2026-04-17

# Log em arquivo customizado
    python main.py --mode full --log-file logs/run.log

Cron / Orquestrador
-------------------
# Diário às 6h
    0 6 * * * cd /path/to/pandora_scraper && python main.py --frequency daily

# A cada hora
    0 * * * * cd /path/to/pandora_scraper && python main.py --frequency hourly
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuração de caminhos padrão
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PARSED_DIR = DATA_DIR / "parsed"
TRUSTED_DIR = DATA_DIR / "trusted"
LOGS_DIR = BASE_DIR / "logs"

# ---------------------------------------------------------------------------
# Configuração por frequência de execução
# ---------------------------------------------------------------------------

FREQUENCY_CONFIG = {
    "daily": {
        "max_pages": 999,           # Todas as páginas
        "delay": 1.5,               # Delay conservador
        "description": "Coleta completa de todas as paginas e categorias.",
    },
    "hourly": {
        "max_pages": 2,             # Apenas as primeiras páginas (novidades)
        "delay": 1.0,               # Delay ligeiramente menor
        "description": "Coleta rapida das primeiras paginas (novidades e atualizacoes recentes).",
    },
}


# ---------------------------------------------------------------------------
# Setup de logging
# ---------------------------------------------------------------------------


def setup_logging(log_level: str = "INFO", log_file: str | None = None) -> None:
    """Configura logging para console e opcionalmente para arquivo."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if log_file:
        file_path = Path(log_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(file_path, encoding="utf-8"))
    else:
        # Log automático por run
        run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        auto_log = LOGS_DIR / f"run_{run_ts}.log"
        handlers.append(logging.FileHandler(auto_log, encoding="utf-8"))

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=handlers,
    )


# ---------------------------------------------------------------------------
# Parser de argumentos CLI
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pandora_scraper",
        description="Pipeline de coleta de dados de precos da Pandora Joias.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # ----- Frequência (atalho de alto nível) -----
    parser.add_argument(
        "--frequency",
        choices=["daily", "hourly"],
        default=None,
        help=(
            "Frequencia de execucao: "
            "'daily' = coleta completa (todas as paginas, 1x ao dia); "
            "'hourly' = coleta rapida (primeiras paginas, a cada hora). "
            "Define automaticamente --max-pages e --delay. "
            "Pode ser sobrescrito por --max-pages explícito."
        ),
    )

    # Modo de execução
    parser.add_argument(
        "--mode",
        choices=["full", "extract", "parse", "transform"],
        default="full",
        help=(
            "Modo de execucao: "
            "'full' executa todas as etapas; "
            "'extract' apenas coleta HTML; "
            "'parse' apenas parseia o Raw existente; "
            "'transform' apenas transforma o Parsed existente."
        ),
    )

    # Categorias a extrair
    parser.add_argument(
        "--categories",
        nargs="+",
        default=None,
        metavar="CATEGORY",
        help=(
            "Categorias a extrair (ex: colares aneis braceletes). "
            "Se omitido, extrai todas as categorias configuradas."
        ),
    )

    # Limite de páginas (sobrescreve o definido pela frequência)
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Numero maximo de paginas a extrair por categoria. "
            "Se omitido, usa o default da frequencia (daily=999, hourly=2)."
        ),
    )

    # Delay entre requests (sobrescreve o definido pela frequência)
    parser.add_argument(
        "--delay",
        type=float,
        default=None,
        metavar="SECONDS",
        help="Delay em segundos entre requests ao site. Default depende da frequencia.",
    )

    # Diretórios
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=RAW_DIR,
        metavar="PATH",
        help=f"Diretorio da camada Raw (default: {RAW_DIR}).",
    )
    parser.add_argument(
        "--trusted-dir",
        type=Path,
        default=TRUSTED_DIR,
        metavar="PATH",
        help=f"Diretorio da camada Trusted (default: {TRUSTED_DIR}).",
    )

    # Data de referência
    parser.add_argument(
        "--run-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Data de referencia da execucao (para auditoria). Default: data atual.",
    )

    # Run ID (identificador único)
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        metavar="ID",
        help="Identificador unico da execucao. Default: timestamp UTC.",
    )

    # Nível de log
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Nivel de log (default: INFO).",
    )

    # Arquivo de log
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        metavar="PATH",
        help="Caminho para arquivo de log (opcional).",
    )

    return parser


def _resolve_frequency_defaults(args: argparse.Namespace) -> argparse.Namespace:
    """
    Aplica os defaults de frequência quando --frequency é especificado.
    Parâmetros explícitos (--max-pages, --delay) sempre têm precedência.
    """
    freq = args.frequency or "daily"  # sem --frequency, comportamento é daily
    cfg = FREQUENCY_CONFIG[freq]

    if args.max_pages is None:
        args.max_pages = cfg["max_pages"]
    if args.delay is None:
        args.delay = cfg["delay"]

    return args


# ---------------------------------------------------------------------------
# Etapas da pipeline
# ---------------------------------------------------------------------------


def run_extract(args: argparse.Namespace, run_id: str) -> None:
    """Etapa de extração: baixa HTML do site e salva na camada Raw."""
    from extractor import CATEGORIES, extract_all_categories

    logger = logging.getLogger("main.extract")
    logger.info("=== ETAPA: EXTRACAO ===")
    logger.info("Run ID: %s", run_id)
    logger.info("Raw Dir: %s", args.raw_dir)

    # Filtra categorias se especificadas
    cats_to_extract = CATEGORIES.copy()
    if args.categories:
        cats_to_extract = {
            name: slug
            for name, slug in CATEGORIES.items()
            if name in args.categories
        }
        if not cats_to_extract:
            logger.error(
                "Nenhuma categoria valida encontrada em: %s. "
                "Categorias disponiveis: %s",
                args.categories,
                list(CATEGORIES.keys()),
            )
            sys.exit(1)

    logger.info("Frequencia     : %s", args.frequency or "manual")
    logger.info("Categorias     : %s", list(cats_to_extract.keys()))
    logger.info("Max paginas    : %d", args.max_pages)
    logger.info("Delay requests : %.1fs", args.delay)

    results = extract_all_categories(
        raw_dir=args.raw_dir,
        categories=cats_to_extract,
        max_pages=args.max_pages,
        delay_between_requests=args.delay,
    )

    total_pages = sum(len(pages) for pages in results.values())
    logger.info("Extracao concluida: %d paginas salvas em %d categorias.", total_pages, len(results))

    return results


def run_parse(args: argparse.Namespace, run_id: str) -> tuple[int, dict[str, int]]:
    """
    Etapa de parsing: lê o Raw e converte para DataFrame salvo como CSV intermediário.

    Retorna
    -------
    tuple[int, dict]
        (numero_alertas, {categoria: total_produtos})
    """
    import pandas as pd
    from parser import parse_all_category_pages

    logger = logging.getLogger("main.parse")
    logger.info("=== ETAPA: PARSING ===")

    df, total_alerts = parse_all_category_pages(
        raw_dir=args.raw_dir,
        run_id=run_id,
        logs_dir=LOGS_DIR,
        base_dir=BASE_DIR,
    )

    category_counts: dict[str, int] = {}
    if not df.empty:
        category_counts = df.groupby("category").size().to_dict()

    if df.empty:
        logger.warning("Nenhum dado extraido do parsing.")
        return total_alerts, category_counts

    # Salva CSV intermediário para auditoria (camada Parsed)
    parsed_dir = DATA_DIR / "parsed"
    parsed_dir.mkdir(parents=True, exist_ok=True)
    parsed_csv = parsed_dir / f"parsed_{run_id}.csv"
    df.to_csv(parsed_csv, index=False, encoding="utf-8-sig")
    logger.info("Parsed CSV salvo: %s (%d registros)", parsed_csv, len(df))

    # Também salva como Parquet para uso eficiente na próxima etapa
    parsed_parquet = parsed_dir / f"parsed_{run_id}.parquet"
    df.to_parquet(parsed_parquet, index=False)
    logger.info("Parsed Parquet salvo: %s", parsed_parquet)

    return total_alerts, category_counts


def run_transform(args: argparse.Namespace, run_id: str) -> None:
    """Etapa de transformação: lê o Parsed mais recente e salva na camada Trusted."""
    import pandas as pd
    from transformer import transform

    logger = logging.getLogger("main.transform")
    logger.info("=== ETAPA: TRANSFORMACAO ===")

    parsed_dir = DATA_DIR / "parsed"

    # Usa o arquivo Parquet mais recente
    parquet_files = sorted(parsed_dir.glob("parsed_*.parquet"), reverse=True)
    if not parquet_files:
        # Fallback: CSV
        csv_files = sorted(parsed_dir.glob("parsed_*.csv"), reverse=True)
        if not csv_files:
            logger.error(
                "Nenhum arquivo parsed encontrado em %s. Execute --mode parse primeiro.",
                parsed_dir,
            )
            sys.exit(1)
        logger.info("Lendo CSV: %s", csv_files[0])
        df = pd.read_csv(csv_files[0], encoding="utf-8-sig")
        import ast
        if "sizes" in df.columns:
            df["sizes"] = df["sizes"].apply(
                lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else []
            )
    else:
        logger.info("Lendo Parquet: %s", parquet_files[0])
        df = pd.read_parquet(parquet_files[0])

    logger.info("Registros lidos: %d", len(df))

    df_transformed = transform(
        df_pandas=df,
        trusted_dir=args.trusted_dir,
        run_id=run_id,
    )

    if df_transformed is not None:
        logger.info("Transformacao concluida: %d registros salvos.", len(df_transformed))


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    # Aplica defaults de frequência
    args = _resolve_frequency_defaults(args)

    # Setup de logging
    setup_logging(log_level=args.log_level, log_file=args.log_file)
    logger = logging.getLogger("main")

    # Run ID
    if args.run_id:
        run_id = args.run_id
    elif args.run_date:
        try:
            dt = datetime.strptime(args.run_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            run_id = dt.strftime("%Y%m%dT000000Z")
        except ValueError:
            logger.error("Formato de data invalido: %s (esperado: YYYY-MM-DD)", args.run_date)
            sys.exit(1)
    else:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    frequency = args.frequency or "manual"

    logger.info("=" * 60)
    logger.info("PANDORA SCRAPER - Iniciando execucao")
    logger.info("Run ID      : %s", run_id)
    logger.info("Frequencia  : %s", frequency)
    logger.info("Modo        : %s", args.mode)
    logger.info("Max paginas : %d", args.max_pages)
    logger.info("Delay       : %.1fs", args.delay)
    logger.info("Raw Dir     : %s", args.raw_dir)
    logger.info("Trusted Dir : %s", args.trusted_dir)
    logger.info("=" * 60)

    total_alerts = 0
    category_counts: dict[str, int] = {}
    total_products = 0

    try:
        if args.mode in ("full", "extract"):
            run_extract(args, run_id)

        if args.mode in ("full", "parse"):
            total_alerts, category_counts = run_parse(args, run_id)
            total_products = sum(category_counts.values())

        if args.mode in ("full", "transform"):
            run_transform(args, run_id)

        # ---- Resumo consolidado no structure_alerts.log ----
        if args.mode in ("full", "parse"):
            from monitor import write_run_summary
            write_run_summary(
                run_id=run_id,
                frequency=frequency,
                categories_extracted=category_counts,
                total_products=total_products,
                alerts_triggered=total_alerts,
                logs_dir=LOGS_DIR,
            )

        logger.info("=" * 60)
        logger.info("Pipeline finalizada com sucesso. Run ID: %s", run_id)
        if total_alerts > 0:
            logger.warning(
                "%d alerta(s) estrutural(is) detectado(s). "
                "Verifique logs/structure_alerts.log para detalhes.",
                total_alerts,
            )
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.warning("Execucao interrompida pelo usuario.")
        sys.exit(130)
    except Exception as e:
        logger.critical("Erro fatal na pipeline: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
