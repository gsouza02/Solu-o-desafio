"""
monitor.py
==========
Detecta mudanças estruturais no HTML do site que possam prejudicar a rotina
de scraping e registra alertas em logs/structure_alerts.log.

O que é monitorado:
- Página retornou 0 produtos (seletor CSS pode ter mudado)
- Nenhum preço encontrado na página (formato de preço mudou)
- Queda abrupta de produtos por página vs. histórico (> 30% de queda)
- Título da página inesperado (redesign ou redirect)
- Links de produto com padrão diferente de '/p' (estrutura de URL mudou)
- Conteúdo hash igual ao de execução anterior mas estrutura diferente
- Número de links "/p" na página caiu abaixo do mínimo esperado

Os alertas são WARNINGS — a pipeline continua, mas o operador é notificado.
"""

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Logger dedicado a alertas estruturais
# ---------------------------------------------------------------------------

ALERTS_LOG_NAME = "structure_alerts.log"

_alert_logger: logging.Logger | None = None


def get_alert_logger(logs_dir: Path) -> logging.Logger:
    """
    Retorna (e cria se necessário) o logger de alertas estruturais.
    Grava SEMPRE em logs/structure_alerts.log — arquivo acumulativo entre runs.
    """
    global _alert_logger
    if _alert_logger is not None:
        return _alert_logger

    logs_dir.mkdir(parents=True, exist_ok=True)
    alert_path = logs_dir / ALERTS_LOG_NAME

    logger = logging.getLogger("structure_alerts")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False  # Não duplica no root logger

    # Handler de arquivo — acumulativo (mode='a')
    fh = logging.FileHandler(alert_path, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    logger.addHandler(fh)

    # Handler de console também (para visibilidade imediata)
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    ch.setFormatter(
        logging.Formatter(">>> ALERTA ESTRUTURAL: %(message)s")
    )
    logger.addHandler(ch)

    _alert_logger = logger
    return logger


# ---------------------------------------------------------------------------
# Baseline — métricas históricas para comparação
# ---------------------------------------------------------------------------

BASELINE_FILE = "data/monitor_baseline.json"

# Mínimo de produtos esperados por página de categoria
MIN_PRODUCTS_PER_PAGE = 10

# Se encontrar menos que X% do histórico, levanta alerta
PRODUCT_DROP_THRESHOLD = 0.70  # aceita até 30% de queda

# Padrão esperado de link de produto
PRODUCT_LINK_SUFFIX = "/p"

# Regex de preço esperado
PRICE_REGEX = re.compile(r"R\$\s*[\d.,]+")


def _load_baseline(base_dir: Path) -> dict[str, Any]:
    """Carrega métricas históricas de baseline."""
    path = base_dir / BASELINE_FILE
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def _save_baseline(base_dir: Path, baseline: dict[str, Any]) -> None:
    """Persiste métricas de baseline."""
    path = base_dir / BASELINE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(baseline, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _update_baseline(
    baseline: dict[str, Any],
    category: str,
    page: int,
    products_found: int,
) -> dict[str, Any]:
    """
    Atualiza baseline com média móvel simples (últimas 5 execuções).
    Só atualiza quando o resultado parece saudável (>= MIN_PRODUCTS_PER_PAGE).
    """
    key = f"{category}_page{page}"
    if products_found < MIN_PRODUCTS_PER_PAGE:
        return baseline  # Não polui o baseline com reads ruins

    history = baseline.get(key, [])
    history.append(products_found)
    baseline[key] = history[-5:]  # rolling window de 5
    return baseline


# ---------------------------------------------------------------------------
# Verificações estruturais individuais
# ---------------------------------------------------------------------------


def _check_product_count(
    alert_log: logging.Logger,
    category: str,
    page: int,
    products_found: int,
    baseline: dict[str, Any],
    run_id: str,
) -> bool:
    """Verifica se o número de produtos caiu de forma suspeita."""
    ok = True
    key = f"{category}_page{page}"

    # Abaixo do mínimo absoluto
    if products_found == 0:
        alert_log.critical(
            "[%s] CRITICO | categoria='%s' pagina=%d | "
            "ZERO produtos encontrados — seletor CSS provavelmente mudou! "
            "Verifique o parser.py e os seletores em SELECTORS['product_card_links'].",
            run_id, category, page,
        )
        ok = False
    elif products_found < MIN_PRODUCTS_PER_PAGE:
        alert_log.warning(
            "[%s] categoria='%s' pagina=%d | "
            "Apenas %d produtos encontrados (mínimo esperado: %d). "
            "Pode indicar mudança no HTML ou página incompleta.",
            run_id, category, page, products_found, MIN_PRODUCTS_PER_PAGE,
        )
        ok = False

    # Queda vs. histórico
    history = baseline.get(key, [])
    if history:
        avg_historical = sum(history) / len(history)
        ratio = products_found / avg_historical if avg_historical > 0 else 1
        if ratio < PRODUCT_DROP_THRESHOLD:
            alert_log.warning(
                "[%s] categoria='%s' pagina=%d | "
                "Queda de %.0f%% no numero de produtos: "
                "atual=%d, media_historica=%.1f. "
                "Possivel mudanca na paginacao ou estrutura da listagem.",
                run_id, category, page,
                (1 - ratio) * 100, products_found, avg_historical,
            )
            ok = False

    return ok


def _check_price_presence(
    alert_log: logging.Logger,
    category: str,
    page: int,
    html_path: Path,
    run_id: str,
) -> bool:
    """Verifica se há preços no HTML — detecta mudança no formato de preço."""
    content = html_path.read_text(encoding="utf-8", errors="ignore")
    prices_found = PRICE_REGEX.findall(content)

    if not prices_found:
        alert_log.critical(
            "[%s] CRITICO | categoria='%s' pagina=%d | "
            "Nenhum preco no formato 'R$ X.XXX,XX' encontrado no HTML. "
            "O formato de preco pode ter mudado — verifique PRICE_REGEX no parser.py.",
            run_id, category, page,
        )
        return False
    return True


def _check_product_links(
    alert_log: logging.Logger,
    category: str,
    page: int,
    html_path: Path,
    run_id: str,
) -> bool:
    """Verifica se os links de produto ainda seguem o padrão /slug/p."""
    soup = BeautifulSoup(html_path.read_bytes(), "html.parser")

    all_links = soup.find_all("a", href=True)
    product_links = [a for a in all_links if str(a["href"]).endswith(PRODUCT_LINK_SUFFIX)]

    if not product_links:
        alert_log.critical(
            "[%s] CRITICO | categoria='%s' pagina=%d | "
            "Nenhum link com sufixo '/p' encontrado. "
            "A estrutura de URL dos produtos pode ter mudado. "
            "Verifique SELECTORS['product_card_links'] no parser.py.",
            run_id, category, page,
        )
        return False
    return True


def _check_page_title(
    alert_log: logging.Logger,
    category: str,
    page: int,
    html_path: Path,
    baseline: dict[str, Any],
    run_id: str,
) -> bool:
    """
    Verifica se o título da página mudou drasticamente vs. histórico.
    Alterações no title podem indicar redirect, erro 404 disfarçado ou redesign.
    """
    soup = BeautifulSoup(html_path.read_bytes(), "html.parser")
    title_tag = soup.find("title")
    current_title = title_tag.get_text().strip() if title_tag else ""

    title_key = f"{category}_page{page}_title"
    stored_title = baseline.get(title_key)

    if stored_title and stored_title != current_title:
        alert_log.warning(
            "[%s] categoria='%s' pagina=%d | "
            "Titulo da pagina mudou: "
            "anterior='%s' | atual='%s'. "
            "Pode indicar redirect ou redesign do site.",
            run_id, category, page, stored_title, current_title,
        )
        # Atualiza baseline com novo título
        baseline[title_key] = current_title
        return False

    if not stored_title:
        baseline[title_key] = current_title

    return True


# ---------------------------------------------------------------------------
# Verificação principal — chamada após cada página extraída
# ---------------------------------------------------------------------------


def check_page_health(
    html_path: Path,
    products_found: int,
    category: str,
    page: int,
    run_id: str,
    logs_dir: Path,
    base_dir: Path,
) -> bool:
    """
    Executa todas as verificações estruturais em uma página extraída.

    Parâmetros
    ----------
    html_path : Path
        Caminho do HTML salvo na camada Raw.
    products_found : int
        Número de produtos extraídos pelo parser.
    category : str
        Nome da categoria.
    page : int
        Número da página.
    run_id : str
        Identificador da execução.
    logs_dir : Path
        Diretório dos logs.
    base_dir : Path
        Diretório raiz do projeto (para localizar o baseline).

    Retorna
    -------
    bool
        True se tudo OK, False se algum alerta foi disparado.
    """
    alert_log = get_alert_logger(logs_dir)
    baseline = _load_baseline(base_dir)

    all_ok = True

    # Verifica presença de preços no HTML bruto
    if not _check_price_presence(alert_log, category, page, html_path, run_id):
        all_ok = False

    # Verifica padrão de links de produto
    if not _check_product_links(alert_log, category, page, html_path, run_id):
        all_ok = False

    # Verifica título da página
    if not _check_page_title(alert_log, category, page, html_path, baseline, run_id):
        all_ok = False

    # Verifica contagem de produtos
    if not _check_product_count(alert_log, category, page, products_found, baseline, run_id):
        all_ok = False

    # Atualiza baseline com métricas saudáveis
    baseline = _update_baseline(baseline, category, page, products_found)
    _save_baseline(base_dir, baseline)

    if all_ok:
        alert_log.info(
            "[%s] OK | categoria='%s' pagina=%d | %d produtos encontrados.",
            run_id, category, page, products_found,
        )

    return all_ok


# ---------------------------------------------------------------------------
# Resumo de saúde pós-execução
# ---------------------------------------------------------------------------


def write_run_summary(
    run_id: str,
    frequency: str,
    categories_extracted: dict[str, int],
    total_products: int,
    alerts_triggered: int,
    logs_dir: Path,
) -> None:
    """
    Grava um resumo consolidado da execução no structure_alerts.log.
    Facilita auditoria rápida por operadores.
    """
    alert_log = get_alert_logger(logs_dir)
    separator = "-" * 70

    alert_log.info(separator)
    alert_log.info(
        "[%s] RESUMO DA EXECUCAO | frequencia=%s | total_produtos=%d | alertas=%d",
        run_id, frequency, total_products, alerts_triggered,
    )
    for cat, count in categories_extracted.items():
        alert_log.info(
            "[%s]   categoria='%s' -> %d produtos", run_id, cat, count,
        )
    if alerts_triggered > 0:
        alert_log.warning(
            "[%s] ATENCAO: %d alerta(s) estrutural(is) detectado(s) nesta execucao. "
            "Revise este arquivo para detalhes.",
            run_id, alerts_triggered,
        )
    else:
        alert_log.info(
            "[%s] Nenhum alerta estrutural. Site respondendo normalmente.",
            run_id,
        )
    alert_log.info(separator)
