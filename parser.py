"""
parser.py
=========
Lê os arquivos HTML da camada Raw, aplica lógica de extração com BeautifulSoup
e converte os dados para um DataFrame Pandas.

Estratégia de robustez:
- Múltiplos seletores CSS com fallback: se o seletor principal falhar, tenta alternativas.
- Cada campo é extraído de forma independente (falha isolada não descarta o produto).
- Dados brutos extraídos são preservados para auditoria (campo 'raw_*').
- Logs detalhados para rastreabilidade de erros de parsing.
"""

import json
import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Seletores CSS — definidos em um único lugar para facilitar manutenção
# ---------------------------------------------------------------------------
# A estrutura do site Pandora expõe os produtos em elementos de listagem com
# links na forma: /slug-do-produto/p
# O padrão observado nas páginas de categoria:
#   - Cada produto é um link <a href="/slug/p"> contendo nome, preço, tamanhos e metal
#   - Formato de preço: "NomeR$ X.XXX,XXou 10x de R$ X,XX"


SELECTORS = {
    # Seletor primário para cards de produto na listagem
    "product_card_links": [
        "a[href$='/p']",         # Padrão principal
        "a[href*='/p']",         # Fallback mais amplo
    ],
    # Página de produto individual
    "product_title": [
        "h1.vtex-store-components-3-x-productNameContainer",
        "h1[class*='productName']",
        "h1",
    ],
    "product_price": [
        "span[class*='sellingPrice']",
        "span[class*='price']",
        "[class*='sellingPrice']",
    ],
    "product_sku": [
        "span[class*='skuReference']",
        "[class*='skuReference']",
    ],
    "product_description": [
        "div[class*='productDescription']",
        "div[class*='description']",
        "[class*='description']",
    ],
    "breadcrumb": [
        "a[class*='breadcrumb']",
        "nav[aria-label='breadcrumb'] a",
        ".breadcrumb a",
    ],
    "meta_description": ["meta[name='description']"],
}

# Regex para extrair preço no formato "R$ 1.234,56"
PRICE_REGEX = re.compile(r"R\$\s*([\d.,]+)")

# Regex para extrair SKU da meta description (padrão: "Nome, SKU: 123456")
SKU_REGEX = re.compile(r"\b(\d{6,})\b")

# Regex para extrair tamanho da string de produto
SIZE_REGEX = re.compile(r"Tamanho(\d[\d\s]*)")

# Regex para extrair metal/material
METAL_REGEX = re.compile(r"Metal:\s*(.+?)(?=R\$|$|Tamanho|\n)", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------


def _get_soup(html_path: Path) -> BeautifulSoup:
    """Carrega e parseia HTML."""
    content = html_path.read_bytes()
    return BeautifulSoup(content, "html.parser")


def _try_select(soup: BeautifulSoup | Tag, selectors: list[str]) -> list[Tag]:
    """Tenta múltiplos seletores CSS e retorna os resultados do primeiro que funcionar."""
    for sel in selectors:
        results = soup.select(sel)
        if results:
            return results
    return []


def _try_select_one(soup: BeautifulSoup | Tag, selectors: list[str]) -> Tag | None:
    """Versão singular de _try_select."""
    for sel in selectors:
        result = soup.select_one(sel)
        if result:
            return result
    return None


def _clean_text(text: str | None) -> str | None:
    """Remove espaços extras e caracteres de controle."""
    if text is None:
        return None
    return re.sub(r"\s+", " ", text).strip() or None


def _parse_price(raw: str | None) -> float | None:
    """Converte string de preço 'R$ 1.234,56' para float 1234.56."""
    if not raw:
        return None
    match = PRICE_REGEX.search(raw)
    if not match:
        return None
    try:
        price_str = match.group(1).replace(".", "").replace(",", ".")
        return float(price_str)
    except ValueError:
        return None


def _parse_installment_price(raw: str | None) -> float | None:
    """Extrai preço da parcela (segundo valor de preço na string)."""
    if not raw:
        return None
    matches = PRICE_REGEX.findall(raw)
    if len(matches) >= 2:
        try:
            price_str = matches[1].replace(".", "").replace(",", ".")
            return float(price_str)
        except ValueError:
            return None
    return None


def _parse_sizes(raw: str | None) -> list[str]:
    """Extrai lista de tamanhos disponíveis."""
    if not raw:
        return []
    match = SIZE_REGEX.search(raw)
    if not match:
        return []
    sizes_str = match.group(1).strip()
    # Divide por espaços (tamanhos juntos), remove vazios
    sizes = [s.strip() for s in sizes_str.split() if s.strip()]
    return sizes


# ---------------------------------------------------------------------------
# Parser de página de listagem (categoria)
# ---------------------------------------------------------------------------


def parse_category_page(html_path: Path, category_name: str, page_number: int) -> list[dict[str, Any]]:
    """
    Extrai todos os produtos de uma página de categoria.

    Parâmetros
    ----------
    html_path : Path
        Caminho para o arquivo HTML da camada Raw.
    category_name : str
        Nome da categoria (ex: 'colares').
    page_number : int
        Número da página.

    Retorna
    -------
    list[dict]
        Lista de dicionários com os dados brutos de cada produto.
    """
    soup = _get_soup(html_path)
    products: list[dict[str, Any]] = []

    # Lê metadados da extração se disponíveis
    meta_path = html_path.with_suffix(".meta.json")
    extraction_ts = None
    source_url = None
    if meta_path.exists():
        meta = json.loads(meta_path.read_bytes())
        extraction_ts = meta.get("extracted_at")
        source_url = meta.get("url")

    # Seleciona todos os links de produto
    product_links = _try_select(soup, SELECTORS["product_card_links"])

    # Filtra apenas links de produto real (terminam em /p)
    seen_urls: set[str] = set()
    for link in product_links:
        href = link.get("href", "")
        if not href.endswith("/p"):
            continue

        # Monta URL absoluta
        if href.startswith("http"):
            product_url = href
        else:
            from extractor import BASE_URL
            product_url = BASE_URL + href

        if product_url in seen_urls:
            continue
        seen_urls.add(product_url)

        raw_text = _clean_text(link.get_text())
        if not raw_text:
            continue

        # Extrai campos do texto bruto do card
        price = _parse_price(raw_text)
        installment_price = _parse_installment_price(raw_text)
        sizes = _parse_sizes(raw_text)

        # Extrai metal/material
        metal_match = METAL_REGEX.search(raw_text or "")
        metal = _clean_text(metal_match.group(1)) if metal_match else None

        # Extrai nome: tudo antes do preço e antes de "Metal:"
        name_raw = re.split(r"R\$|Metal:", raw_text)[0] if raw_text else None
        name_raw = re.sub(r"Tamanho.*", "", name_raw or "").strip()

        # Slug do produto (penúltimo segmento da URL)
        slug = href.rstrip("/").rstrip("p").rstrip("/").rsplit("/", 1)[-1]

        product_record: dict[str, Any] = {
            # Identificação
            "slug": slug,
            "product_url": product_url,
            "category": category_name,
            "page_number": page_number,
            # Dados extraídos
            "raw_name": name_raw,
            "raw_price": _clean_text(PRICE_REGEX.search(raw_text).group(0)) if PRICE_REGEX.search(raw_text) else None,
            "raw_text": raw_text,
            "price_brl": price,
            "installment_price_brl": installment_price,
            "installments": 10 if installment_price else None,
            "sizes": sizes,
            "metal": metal,
            # Rastreabilidade
            "source_file": str(html_path),
            "source_url": source_url,
            "extracted_at": extraction_ts,
        }
        products.append(product_record)
        logger.debug("Produto extraído: %s | R$ %.2f", slug, price or 0)

    logger.info(
        "Categoria '%s', página %d: %d produtos extraídos de '%s'",
        category_name,
        page_number,
        len(products),
        html_path.name,
    )
    return products


# ---------------------------------------------------------------------------
# Parser de página de produto individual
# ---------------------------------------------------------------------------


def parse_product_page(html_path: Path) -> dict[str, Any] | None:
    """
    Extrai dados detalhados de uma página de produto individual.

    Parâmetros
    ----------
    html_path : Path
        Caminho para o arquivo HTML da camada Raw.

    Retorna
    -------
    dict | None
        Dicionário com os dados do produto, ou None se a página for inválida.
    """
    soup = _get_soup(html_path)

    meta_path = html_path.with_suffix(".meta.json")
    extraction_ts = None
    source_url = None
    slug = html_path.stem
    if meta_path.exists():
        meta = json.loads(meta_path.read_bytes())
        extraction_ts = meta.get("extracted_at")
        source_url = meta.get("url")
        slug = meta.get("slug", html_path.stem)

    # Título
    title_tag = _try_select_one(soup, SELECTORS["product_title"])
    title = _clean_text(title_tag.get_text()) if title_tag else None

    # Meta description contém SKU e descrição curta
    meta_desc_tag = _try_select_one(soup, SELECTORS["meta_description"])
    meta_desc = meta_desc_tag.get("content", "") if meta_desc_tag else ""

    # SKU: extraído da meta description ou da URL
    sku_match = SKU_REGEX.search(meta_desc or "")
    sku = sku_match.group(1) if sku_match else None

    # Breadcrumb para inferir categoria
    breadcrumb_tags = _try_select(soup, SELECTORS["breadcrumb"])
    breadcrumb = [_clean_text(b.get_text()) for b in breadcrumb_tags if _clean_text(b.get_text())]

    # Preço principal (tenta extrair do texto visível da página)
    price_tag = _try_select_one(soup, SELECTORS["product_price"])
    price = _parse_price(_clean_text(price_tag.get_text()) if price_tag else None)

    # Se não encontrou preço pelo seletor, tenta no texto completo visível
    if price is None:
        page_text = soup.get_text()
        price = _parse_price(page_text)

    # Descrição
    desc_tag = _try_select_one(soup, SELECTORS["product_description"])
    description = _clean_text(desc_tag.get_text()) if desc_tag else None

    # Fallback: usa meta description como descrição curta
    short_desc = _clean_text(meta_desc) if meta_desc else None

    return {
        "slug": slug,
        "product_url": source_url,
        "title": title,
        "sku": sku,
        "price_brl": price,
        "description": description,
        "short_description": short_desc,
        "breadcrumb": breadcrumb,
        "source_file": str(html_path),
        "extracted_at": extraction_ts,
    }


# ---------------------------------------------------------------------------
# Parsing em lote e conversão para DataFrame
# ---------------------------------------------------------------------------


def parse_all_category_pages(
    raw_dir: Path,
    run_id: str = "unknown",
    logs_dir: Path | None = None,
    base_dir: Path | None = None,
) -> tuple[pd.DataFrame, int]:
    """
    Lê todas as páginas de categoria da camada Raw e retorna um DataFrame unificado.
    Aciona o monitor de estrutura HTML após cada página.

    A estrutura esperada no raw_dir é:
        raw_dir/categories/{category_name}/page_NNNN.html

    Retorna
    -------
    tuple[pd.DataFrame, int]
        (DataFrame com todos os produtos, número de alertas estruturais disparados)
    """
    from monitor import check_page_health

    categories_dir = raw_dir / "categories"
    if not categories_dir.exists():
        logger.warning("Diretório de categorias não encontrado: %s", categories_dir)
        return pd.DataFrame(), 0

    all_records: list[dict[str, Any]] = []
    total_alerts = 0

    _logs_dir = logs_dir or raw_dir.parent.parent / "logs"
    _base_dir = base_dir or raw_dir.parent.parent

    for category_dir in sorted(categories_dir.iterdir()):
        if not category_dir.is_dir():
            continue
        category_name = category_dir.name

        html_files = sorted(category_dir.glob("page_*.html"))
        if not html_files:
            logger.warning("Nenhum HTML encontrado em %s", category_dir)
            continue

        for html_file in html_files:
            page_number = int(re.search(r"page_(\d+)", html_file.stem).group(1))
            try:
                records = parse_category_page(html_file, category_name, page_number)
                all_records.extend(records)

                # ---- Verificação estrutural (monitor) ----
                page_ok = check_page_health(
                    html_path=html_file,
                    products_found=len(records),
                    category=category_name,
                    page=page_number,
                    run_id=run_id,
                    logs_dir=_logs_dir,
                    base_dir=_base_dir,
                )
                if not page_ok:
                    total_alerts += 1

            except Exception as e:
                logger.error("Erro ao parsear %s: %s", html_file, e, exc_info=True)
                total_alerts += 1

    if not all_records:
        return pd.DataFrame(), total_alerts

    df = pd.DataFrame(all_records)
    logger.info("Total de registros parseados: %d", len(df))
    return df, total_alerts


def parse_all_product_pages(raw_dir: Path) -> pd.DataFrame:
    """
    Lê todas as páginas de produto individual da camada Raw.

    Retorna
    -------
    pd.DataFrame
    """
    products_dir = raw_dir / "products"
    if not products_dir.exists():
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for html_file in sorted(products_dir.glob("*.html")):
        try:
            record = parse_product_page(html_file)
            if record:
                records.append(record)
        except Exception as e:
            logger.error("Erro ao parsear produto %s: %s", html_file, e, exc_info=True)

    return pd.DataFrame(records) if records else pd.DataFrame()
