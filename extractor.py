"""
extractor.py
============
Responsável por bater no site pandorajoias.com.br e salvar o HTML bruto na camada Raw.

Estratégia de robustez:
- Retries com backoff exponencial (tenacity)
- Rotação de User-Agent para evitar bloqueios suaves
- Salvamento atômico: escreve em .tmp e renomeia para evitar arquivos corrompidos
- Metadados de extração armazenados junto ao HTML (arquivo .meta.json)
- Hash do conteúdo para detectar mudanças entre execuções
"""

import hashlib
import json
import logging
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

BASE_URL = "https://www.pandorajoias.com.br"

# Categorias principais do site
CATEGORIES = {
    "charms": "/charms",
    "braceletes": "/braceletes",
    "aneis": "/aneis",
    "colares": "/colares",
    "brincos": "/brincos",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
}


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------


def _get_session() -> requests.Session:
    """Cria uma sessão HTTP com headers padrão e User-Agent aleatório."""
    session = requests.Session()
    headers = DEFAULT_HEADERS.copy()
    headers["User-Agent"] = random.choice(USER_AGENTS)
    session.headers.update(headers)
    return session


def _content_hash(content: bytes) -> str:
    """Retorna SHA-256 do conteúdo para detecção de mudanças."""
    return hashlib.sha256(content).hexdigest()


def _safe_write(path: Path, content: bytes) -> None:
    """Escrita segura: escreve em .tmp e substitui o destino."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(content)
    # No Windows, rename falha se o destino existe; remove antes
    if path.exists():
        path.unlink()
    tmp.rename(path)


def _write_meta(meta_path: Path, meta: dict[str, Any]) -> None:
    """Salva metadados de extração em JSON."""
    _safe_write(meta_path, json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8"))


# ---------------------------------------------------------------------------
# Função de fetch com retry
# ---------------------------------------------------------------------------


@retry(
    retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    reraise=True,
)
def _fetch_url(session: requests.Session, url: str, timeout: int = 30) -> requests.Response:
    """Faz GET em uma URL com retry automático em falhas transitórias."""
    logger.debug("GET %s", url)
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp


# ---------------------------------------------------------------------------
# Extração de páginas de categoria (listagem + paginação)
# ---------------------------------------------------------------------------


def extract_category(
    category_slug: str,
    raw_dir: Path,
    max_pages: int = 999,
    delay_between_pages: float = 1.5,
) -> list[Path]:
    """
    Extrai todas as páginas de listagem de uma categoria e salva na camada Raw.

    Parâmetros
    ----------
    category_slug : str
        Slug relativo da categoria (ex: '/colares').
    raw_dir : Path
        Diretório raiz da camada Raw.
    max_pages : int
        Limite de páginas a extrair (proteção contra loops infinitos).
    delay_between_pages : float
        Aguarda N segundos entre requests para não sobrecarregar o servidor.

    Retorna
    -------
    list[Path]
        Lista dos arquivos .html salvos.
    """
    session = _get_session()
    saved_paths: list[Path] = []
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    # Remove barra inicial para nome de pasta
    category_name = category_slug.strip("/").replace("/", "_")
    category_dir = raw_dir / "categories" / category_name
    category_dir.mkdir(parents=True, exist_ok=True)

    page = 1
    while page <= max_pages:
        if page == 1:
            url = f"{BASE_URL}{category_slug}"
        else:
            url = f"{BASE_URL}{category_slug}?page={page}"

        try:
            resp = _fetch_url(session, url)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                logger.info("Categoria %s, página %d retornou 404 — sem mais páginas.", category_slug, page)
            else:
                logger.error("Erro HTTP ao buscar %s: %s", url, e)
            break
        except Exception as e:
            logger.error("Falha ao buscar %s: %s", url, e)
            break

        content = resp.content
        content_hash = _content_hash(content)

        # Verifica se o conteúdo mudou em relação à última extração
        html_path = category_dir / f"page_{page:04d}.html"
        meta_path = category_dir / f"page_{page:04d}.meta.json"

        # Detecta última página: conteúdo idêntico à página anterior indica loop
        if page > 1 and saved_paths:
            prev_meta_path = category_dir / f"page_{page-1:04d}.meta.json"
            if prev_meta_path.exists():
                prev_meta = json.loads(prev_meta_path.read_bytes())
                if prev_meta.get("content_hash") == content_hash:
                    logger.info(
                        "Conteúdo idêntico à página anterior para %s/%d — fim da paginação.",
                        category_slug,
                        page,
                    )
                    break

        _safe_write(html_path, content)
        meta = {
            "url": url,
            "category_slug": category_slug,
            "page": page,
            "status_code": resp.status_code,
            "content_hash": content_hash,
            "content_length_bytes": len(content),
            "extracted_at": run_ts,
            "headers": dict(resp.headers),
        }
        _write_meta(meta_path, meta)

        logger.info("Salvo: %s (hash=%s)", html_path, content_hash[:12])
        saved_paths.append(html_path)

        page += 1
        if page <= max_pages:
            time.sleep(delay_between_pages)

    return saved_paths


# ---------------------------------------------------------------------------
# Extração de página de produto individual
# ---------------------------------------------------------------------------


def extract_product(
    product_url: str,
    raw_dir: Path,
) -> Path | None:
    """
    Extrai a página de detalhe de um produto e salva na camada Raw.

    Parâmetros
    ----------
    product_url : str
        URL completa do produto (ex: 'https://www.pandorajoias.com.br/colar-xyz/p').
    raw_dir : Path
        Diretório raiz da camada Raw.

    Retorna
    -------
    Path | None
        Caminho do arquivo salvo, ou None em caso de erro.
    """
    session = _get_session()
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # Deriva nome de arquivo a partir do slug do produto
    slug = product_url.rstrip("/").split("/")[-2] if product_url.endswith("/p") else product_url.rstrip("/").rsplit("/", 1)[-1]
    products_dir = raw_dir / "products"
    products_dir.mkdir(parents=True, exist_ok=True)

    html_path = products_dir / f"{slug}.html"
    meta_path = products_dir / f"{slug}.meta.json"

    try:
        resp = _fetch_url(session, product_url)
    except Exception as e:
        logger.error("Falha ao buscar produto %s: %s", product_url, e)
        return None

    content = resp.content
    content_hash = _content_hash(content)

    _safe_write(html_path, content)
    meta = {
        "url": product_url,
        "slug": slug,
        "status_code": resp.status_code,
        "content_hash": content_hash,
        "content_length_bytes": len(content),
        "extracted_at": run_ts,
        "headers": dict(resp.headers),
    }
    _write_meta(meta_path, meta)

    logger.info("Produto salvo: %s (hash=%s)", html_path, content_hash[:12])
    return html_path


# ---------------------------------------------------------------------------
# Extração em lote de múltiplas categorias
# ---------------------------------------------------------------------------


def extract_all_categories(
    raw_dir: Path,
    categories: dict[str, str] | None = None,
    max_pages: int = 999,
    delay_between_requests: float = 1.5,
) -> dict[str, list[Path]]:
    """
    Extrai todas as categorias configuradas.

    Parâmetros
    ----------
    raw_dir : Path
        Diretório raiz da camada Raw.
    categories : dict | None
        Dicionário {nome: slug}. Se None, usa CATEGORIES padrão.
    max_pages : int
        Limite de páginas por categoria.
    delay_between_requests : float
        Delay entre páginas.

    Retorna
    -------
    dict[str, list[Path]]
        Mapeamento {nome_categoria: [lista_de_paths]}.
    """
    cats = categories or CATEGORIES
    results: dict[str, list[Path]] = {}

    for name, slug in cats.items():
        logger.info("=== Extraindo categoria: %s (%s) ===", name, slug)
        paths = extract_category(
            category_slug=slug,
            raw_dir=raw_dir,
            max_pages=max_pages,
            delay_between_pages=delay_between_requests,
        )
        results[name] = paths
        logger.info("Categoria %s: %d páginas extraídas.", name, len(paths))

    return results
