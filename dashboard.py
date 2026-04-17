"""
dashboard.py
============
Dashboard interativo de acompanhamento de preços da Pandora Joias.
Lê os dados diretamente da camada Trusted (Parquet) — sem modificar nenhum outro módulo.

Como executar:
    streamlit run dashboard.py
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Configuração da página
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Pandora · Monitor de Preços",
    page_icon="💍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS personalizado
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    /* Fundo e tipografia geral */
    [data-testid="stAppViewContainer"] {
        background: linear-gradient(135deg, #0f0f1a 0%, #1a1025 50%, #0d0d1f 100%);
        color: #e8e0f0;
    }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a0f2e 0%, #16082a 100%);
        border-right: 1px solid #3d2a5a;
    }
    [data-testid="stSidebar"] * {
        color: #d4c8e8 !important;
    }

    /* Cards de KPI */
    .kpi-card {
        background: linear-gradient(135deg, #2d1b4e 0%, #1e1040 100%);
        border: 1px solid #5c3d8a;
        border-radius: 16px;
        padding: 20px 24px;
        text-align: center;
        box-shadow: 0 8px 32px rgba(120, 60, 200, 0.2);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .kpi-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 12px 40px rgba(120, 60, 200, 0.35);
    }
    .kpi-label {
        font-size: 0.78rem;
        font-weight: 600;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #a78be0;
        margin-bottom: 8px;
    }
    .kpi-value {
        font-size: 1.9rem;
        font-weight: 800;
        color: #ffffff;
        line-height: 1.1;
    }
    .kpi-sub {
        font-size: 0.76rem;
        color: #7c5fa0;
        margin-top: 6px;
    }

    /* Título da seção */
    .section-title {
        font-size: 1.1rem;
        font-weight: 700;
        color: #c9a7f0;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        margin: 28px 0 12px 0;
        padding-bottom: 6px;
        border-bottom: 1px solid #3d2a5a;
    }

    /* Badge de alerta */
    .alert-badge {
        background: linear-gradient(90deg, #c0392b, #922b21);
        color: white;
        border-radius: 8px;
        padding: 6px 14px;
        font-size: 0.82rem;
        font-weight: 600;
        display: inline-block;
        margin-bottom: 10px;
    }
    .ok-badge {
        background: linear-gradient(90deg, #1e8449, #196f3d);
        color: white;
        border-radius: 8px;
        padding: 6px 14px;
        font-size: 0.82rem;
        font-weight: 600;
        display: inline-block;
        margin-bottom: 10px;
    }

    /* Tabela de produto */
    .product-link {
        color: #b07fef;
        text-decoration: none;
        font-weight: 500;
    }
    .product-link:hover {
        color: #d4aaff;
        text-decoration: underline;
    }

    /* Esconde rodapé do streamlit */
    footer {visibility: hidden;}
    #MainMenu {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Carregamento dos dados
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent
TRUSTED_DIR = BASE_DIR / "data" / "trusted" / "pandora_products"
ALERTS_LOG = BASE_DIR / "logs" / "structure_alerts.log"


@st.cache_data(ttl=300)  # Cache de 5 minutos — recarrega dados automaticamente
def load_data() -> pd.DataFrame:
    """
    Lê todos os Parquets da camada Trusted preservando o histórico completo.
    Cada linha = um produto em um momento específico (run_id).
    NÃO deduplica entre runs — isso permite rastrear variação de preço.
    """
    if not TRUSTED_DIR.exists():
        return pd.DataFrame()

    parquet_files = list(TRUSTED_DIR.glob("*.parquet"))
    if not parquet_files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)

    # Garante tipos corretos
    if "extraction_date" in df.columns:
        df["extraction_date"] = pd.to_datetime(df["extraction_date"], errors="coerce")
    if "price_brl" in df.columns:
        df["price_brl"] = pd.to_numeric(df["price_brl"], errors="coerce")

    # Remove apenas duplicatas exatas dentro do mesmo run (mesmo produto, mesmo run)
    dedup_cols = [c for c in ["slug", "category", "run_id"] if c in df.columns]
    df = df.drop_duplicates(subset=dedup_cols)

    return df.sort_values("run_id").reset_index(drop=True)


def load_alerts() -> list[str]:
    """Lê as últimas linhas do structure_alerts.log."""
    if not ALERTS_LOG.exists():
        return []
    lines = ALERTS_LOG.read_text(encoding="utf-8").strip().splitlines()
    return lines[-ALERTS_TAIL_LINES:]


# ---------------------------------------------------------------------------
# Helpers de formatação
# ---------------------------------------------------------------------------

PLOTLY_THEME = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#d4c8e8", family="Inter, sans-serif"),
    colorway=["#9b59b6", "#6c3483", "#d7bde2", "#7d3c98", "#4a235a", "#c39bd3"],
)

PURPLE_SCALE = [
    [0.0, "#2d1b4e"],
    [0.3, "#6c3483"],
    [0.7, "#9b59b6"],
    [1.0, "#d7bde2"],
]

# Paleta base para geração dinâmica de cores
PURPLE_PALETTE = ["#4a235a", "#7d3c98", "#9b59b6", "#c39bd3", "#2d1b4e",
                  "#6c3483", "#d7bde2", "#5b2c6f", "#a569bd", "#1a0f2e"]

# ---------------------------------------------------------------------------
# Constantes de UI (centralização para evitar magic numbers)
# ---------------------------------------------------------------------------

MAX_SCATTER_POINTS = 150    # Máximo de pontos no scatter strip
KPI_NAME_MAX_LEN   = 28     # Truncamento do nome do produto nos KPI cards
SCATTER_NAME_LEN   = 40     # Truncamento do nome no hover do scatter
ALERTS_TAIL_LINES  = 50     # Linhas exibidas do log de alertas


def make_colors(n: int) -> list[str]:
    """Gera lista de N cores ciclando pela paleta púrpura."""
    return [PURPLE_PALETTE[i % len(PURPLE_PALETTE)] for i in range(max(n, 1))]


def fmt_brl(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def apply_theme(fig: go.Figure) -> go.Figure:
    fig.update_layout(**PLOTLY_THEME)
    fig.update_xaxes(
        gridcolor="#2d1b4e",
        linecolor="#3d2a5a",
        tickfont=dict(color="#a78be0"),
        title_font=dict(color="#c9a7f0"),
    )
    fig.update_yaxes(
        gridcolor="#2d1b4e",
        linecolor="#3d2a5a",
        tickfont=dict(color="#a78be0"),
        title_font=dict(color="#c9a7f0"),
    )
    return fig


# ---------------------------------------------------------------------------
# Interface principal
# ---------------------------------------------------------------------------

# ── Header ──────────────────────────────────────────────────────────────────

st.markdown("""
<div style="text-align:center; padding: 32px 0 20px 0;">
    <div style="font-size:3rem; margin-bottom:8px;">💍</div>
    <h1 style="
        font-size: 2.4rem;
        font-weight: 900;
        background: linear-gradient(90deg, #c39bd3, #9b59b6, #7d3c98);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
        letter-spacing: -0.02em;
    ">Pandora · Monitor de Preços</h1>
    <p style="color:#7c5fa0; margin-top:8px; font-size:0.95rem;">
        Acompanhamento de preços do varejo nacional — dados da camada Trusted
    </p>
</div>
""", unsafe_allow_html=True)

# ── Carregamento ────────────────────────────────────────────────────────────

df_all = load_data()

if df_all.empty:
    st.error(
        "⚠️ Nenhum dado encontrado em `data/trusted/pandora_products/`. "
        "Execute primeiro: `python main.py --frequency daily`"
    )
    st.stop()

# ── Sidebar — Filtros ────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🔍 Filtros")

    # Categoria
    categorias_disponíveis = sorted(df_all["category"].dropna().unique().tolist())
    categorias_selecionadas = st.multiselect(
        "Categoria",
        options=categorias_disponíveis,
        default=categorias_disponíveis[:1],
    )

    st.divider()

    # Metal
    metais_disponiveis = sorted(df_all["metal"].dropna().unique().tolist())
    metais_selecionados = st.multiselect(
        "Material / Metal",
        options=metais_disponiveis,
        default=metais_disponiveis,
    )

    st.divider()

    # Faixa de preço
    price_min = float(df_all["price_brl"].min())
    price_max = float(df_all["price_brl"].max())
    # Passo dinâmico: divide o intervalo em ~50 passos, arredonda para multiplo de 10
    price_step = max(10.0, round((price_max - price_min) / 50, -1))
    preco_range = st.slider(
        "Faixa de Preço (R$)",
        min_value=price_min,
        max_value=price_max,
        value=(price_min, price_max),
        step=price_step,
        format="R$ %.0f",
    )

    st.divider()

    # Botão de reload
    if st.button("🔄 Recarregar dados", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    # Info da última execução
    if "run_id" in df_all.columns:
        last_run = df_all["run_id"].max()
        st.markdown(f"""
        <div style="margin-top:16px; font-size:0.75rem; color:#5a4a7a;">
            <b>Última execução</b><br>{last_run}
        </div>
        """, unsafe_allow_html=True)

# ── Aplica filtros ───────────────────────────────────────────────────────────

# df_history: histórico completo (todos os runs) — usado no gráfico de variação
df_history = df_all.copy()
if categorias_selecionadas:
    df_history = df_history[df_history["category"].isin(categorias_selecionadas)]

# df: preço mais recente por produto — usado em KPIs, distribuição e tabela
df = (
    df_history
    .sort_values("run_id", ascending=False)
    .drop_duplicates(subset=["slug", "category"], keep="first")
)

if metais_selecionados:
    df = df[df["metal"].isin(metais_selecionados) | df["metal"].isna()]

df = df[
    (df["price_brl"] >= preco_range[0]) &
    (df["price_brl"] <= preco_range[1])
]

if df.empty:
    st.warning("Nenhum produto encontrado com os filtros selecionados.")
    st.stop()

# ── KPIs ─────────────────────────────────────────────────────────────────────

st.markdown('<div class="section-title">📊 Visão Geral</div>', unsafe_allow_html=True)

k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Total de Produtos</div>
        <div class="kpi-value">{len(df)}</div>
        <div class="kpi-sub">{', '.join(categorias_selecionadas)}</div>
    </div>""", unsafe_allow_html=True)

with k2:
    avg = df["price_brl"].mean()
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Preço Médio</div>
        <div class="kpi-value">{fmt_brl(avg)}</div>
        <div class="kpi-sub">média simples</div>
    </div>""", unsafe_allow_html=True)

with k3:
    median = df["price_brl"].median()
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Mediana</div>
        <div class="kpi-value">{fmt_brl(median)}</div>
        <div class="kpi-sub">50° percentil</div>
    </div>""", unsafe_allow_html=True)

with k4:
    pmin = df["price_brl"].min()
    product_min = df.loc[df["price_brl"].idxmin(), "product_name"] if "product_name" in df.columns else ""
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Menor Preço</div>
        <div class="kpi-value">{fmt_brl(pmin)}</div>
        <div class="kpi-sub" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis"
             title="{product_min}">{str(product_min)[:KPI_NAME_MAX_LEN]}…</div>
    </div>""", unsafe_allow_html=True)

with k5:
    pmax = df["price_brl"].max()
    product_max = df.loc[df["price_brl"].idxmax(), "product_name"] if "product_name" in df.columns else ""
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Maior Preço</div>
        <div class="kpi-value">{fmt_brl(pmax)}</div>
        <div class="kpi-sub" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis"
             title="{product_max}">{str(product_max)[:KPI_NAME_MAX_LEN]}…</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Gráficos — linha 1 ───────────────────────────────────────────────────────

col_hist, col_metal = st.columns([3, 2])

with col_hist:
    st.markdown('<div class="section-title">📈 Distribuição de Preços</div>', unsafe_allow_html=True)
    # Bins adaptativos: usa raiz quadrada do número de produtos (Sturges adaptado)
    nbins = max(10, min(60, int(len(df) ** 0.5) * 2))
    fig_hist = px.histogram(
        df,
        x="price_brl",
        nbins=nbins,
        color_discrete_sequence=[PURPLE_PALETTE[2]],
        labels={"price_brl": "Preço (R$)", "count": "Qtd. Produtos"},
        template="plotly_dark",
    )
    fig_hist.update_traces(
        marker_line_width=0.5,
        marker_line_color="#d7bde2",
        opacity=0.85,
    )
    fig_hist.update_layout(
        **PLOTLY_THEME,
        bargap=0.05,
        showlegend=False,
        margin=dict(l=0, r=0, t=10, b=0),
        height=320,
        xaxis_tickprefix="R$ ",
        xaxis_tickformat=",.0f",
    )
    fig_hist = apply_theme(fig_hist)
    st.plotly_chart(fig_hist, use_container_width=True)

with col_metal:
    st.markdown('<div class="section-title">🥇 Por Material</div>', unsafe_allow_html=True)
    metal_df = (
        df.groupby(df["metal"].fillna("Não informado"))
        .agg(total=("slug", "count"), preco_medio=("price_brl", "mean"))
        .reset_index()
        .rename(columns={"metal": "Material"})
        .sort_values("total", ascending=True)
    )
    fig_metal = px.bar(
        metal_df,
        x="total",
        y="Material",
        orientation="h",
        color="preco_medio",
        color_continuous_scale=PURPLE_SCALE,
        text="total",
        labels={"total": "Produtos", "preco_medio": "Preço Médio (R$)"},
        template="plotly_dark",
    )
    fig_metal.update_traces(
        textposition="outside",
        textfont=dict(color="#d4c8e8", size=12),
    )
    fig_metal.update_layout(
        **PLOTLY_THEME,
        margin=dict(l=0, r=0, t=10, b=0),
        height=320,
        coloraxis_showscale=False,
        showlegend=False,
    )
    fig_metal = apply_theme(fig_metal)
    st.plotly_chart(fig_metal, use_container_width=True)

# ── Gráficos — linha 2 ───────────────────────────────────────────────────────

col_faixa, col_scatter = st.columns([2, 3])

with col_faixa:
    st.markdown('<div class="section-title">🏷️ Faixas de Preço</div>', unsafe_allow_html=True)

    # Faixas derivadas dos dados reais (não depende de lista hardcoded)
    if "price_category" in df.columns:
        faixa_df = (
            df["price_category"]
            .fillna("Sem preço")
            .value_counts()
            .reset_index()
        )
        faixa_df.columns = ["Faixa", "Produtos"]
        faixa_df = faixa_df.sort_values("Produtos", ascending=False)
    else:
        faixa_df = pd.DataFrame({"Faixa": ["Sem dados"], "Produtos": [0]})

    n_faixas = len(faixa_df)
    fig_faixa = px.pie(
        faixa_df,
        names="Faixa",
        values="Produtos",
        color_discrete_sequence=make_colors(n_faixas),
        template="plotly_dark",
        hole=0.45,
    )
    fig_faixa.update_traces(
        textinfo="label+percent",
        textfont=dict(color="#e8e0f0", size=11),
        marker=dict(line=dict(color="#1a0f2e", width=2)),
    )
    fig_faixa.update_layout(
        **PLOTLY_THEME,
        margin=dict(l=0, r=0, t=10, b=0),
        height=320,
        legend=dict(font=dict(color="#d4c8e8"), bgcolor="rgba(0,0,0,0)"),
        showlegend=False,
    )
    st.plotly_chart(fig_faixa, use_container_width=True)

with col_scatter:
    st.markdown('<div class="section-title">🔎 Preço × Produto (Scatter)</div>', unsafe_allow_html=True)

    name_col = "product_name" if "product_name" in df.columns else "raw_name"
    # Coluna de agrupamento dinâmica: usa metal se existir, senão categoria
    group_col = "metal" if ("metal" in df.columns and df["metal"].notna().any()) else "category"
    df_scatter = df[df["price_brl"].notna()].copy()
    df_scatter["nome_curto"] = (
        df_scatter[name_col].str[:SCATTER_NAME_LEN]
        if name_col in df_scatter.columns
        else df_scatter["slug"].str[:SCATTER_NAME_LEN]
    )
    df_scatter = df_scatter.sort_values("price_brl", ascending=False).head(MAX_SCATTER_POINTS)

    n_grupos = df_scatter[group_col].nunique() if group_col in df_scatter.columns else 1
    fig_scatter = px.strip(
        df_scatter,
        x="price_brl",
        y=group_col,
        color=group_col,
        hover_name="nome_curto",
        hover_data={"price_brl": ":,.2f"},
        labels={"price_brl": "Preço (R$)", group_col: group_col.capitalize()},
        color_discrete_sequence=make_colors(n_grupos),
        template="plotly_dark",
    )
    fig_scatter.update_traces(marker=dict(size=9, opacity=0.82))
    fig_scatter.update_layout(
        **PLOTLY_THEME,
        margin=dict(l=0, r=0, t=10, b=0),
        height=320,
        xaxis_tickprefix="R$ ",
        xaxis_tickformat=",.0f",
        legend=dict(font=dict(color="#d4c8e8"), bgcolor="rgba(0,0,0,0)"),
    )
    fig_scatter = apply_theme(fig_scatter)
    st.plotly_chart(fig_scatter, use_container_width=True)


# ── Histórico de Preço por Produto ────────────────────────────────────────────

st.markdown('<div class="section-title">🔍 Variação de Preço por Produto</div>', unsafe_allow_html=True)

name_col_h = "product_name" if "product_name" in df_history.columns else "raw_name"

# Produtos com ao menos 2 observações de preço (histórico real)
produtos_com_historico = (
    df_history[df_history["price_brl"].notna()]
    .groupby("slug")["run_id"]
    .nunique()
)
produtos_com_historico = produtos_com_historico[produtos_com_historico >= 2].index.tolist()

# Mapa slug -> nome legível
if name_col_h in df_history.columns:
    slug_to_name = (
        df_history[["slug", name_col_h]]
        .dropna(subset=[name_col_h])
        .drop_duplicates("slug")
        .set_index("slug")[name_col_h]
        .to_dict()
    )
else:
    slug_to_name = {s: s for s in df_history["slug"].unique()}

if not produtos_com_historico:
    st.info(
        "Histórico de preço por produto estará disponível após a segunda execução da pipeline. "
        "Execute `python main.py --frequency hourly` novamente para começar a acumular.",
        icon="⏳",
    )
else:
    # Selectbox de produto
    opcoes_produto = sorted(
        [(slug_to_name.get(s, s), s) for s in produtos_com_historico],
        key=lambda x: x[0],
    )
    nome_selecionado, slug_selecionado = st.selectbox(
        "Selecione o produto",
        options=opcoes_produto,
        format_func=lambda x: x[0],
        key="produto_historico",
    )

    df_prod = (
        df_history[
            (df_history["slug"] == slug_selecionado) &
            df_history["price_brl"].notna()
        ]
        .sort_values("extraction_date")
        .copy()
    )

    # ── Métricas de variação ──────────────────────────────────────────────
    preco_inicial = df_prod["price_brl"].iloc[0]
    preco_atual   = df_prod["price_brl"].iloc[-1]
    preco_min_h   = df_prod["price_brl"].min()
    preco_max_h   = df_prod["price_brl"].max()
    variacao_abs  = preco_atual - preco_inicial
    variacao_pct  = (variacao_abs / preco_inicial * 100) if preco_inicial else 0
    n_observacoes = len(df_prod)
    primeira_data = df_prod["extraction_date"].min()
    ultima_data   = df_prod["extraction_date"].max()

    hm1, hm2, hm3, hm4, hm5 = st.columns(5)
    with hm1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Preço Atual</div>
            <div class="kpi-value">{fmt_brl(preco_atual)}</div>
            <div class="kpi-sub">{str(ultima_data)[:10] if pd.notna(ultima_data) else ''}</div>
        </div>""", unsafe_allow_html=True)
    with hm2:
        sinal = "+" if variacao_abs >= 0 else ""
        cor = "#e74c3c" if variacao_abs > 0 else ("#2ecc71" if variacao_abs < 0 else "#7c5fa0")
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Variação Total</div>
            <div class="kpi-value" style="color:{cor}">{sinal}{fmt_brl(variacao_abs)}</div>
            <div class="kpi-sub" style="color:{cor}">{sinal}{variacao_pct:.1f}%</div>
        </div>""", unsafe_allow_html=True)
    with hm3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Menor Registrado</div>
            <div class="kpi-value">{fmt_brl(preco_min_h)}</div>
            <div class="kpi-sub">mínimo histórico</div>
        </div>""", unsafe_allow_html=True)
    with hm4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Maior Registrado</div>
            <div class="kpi-value">{fmt_brl(preco_max_h)}</div>
            <div class="kpi-sub">máximo histórico</div>
        </div>""", unsafe_allow_html=True)
    with hm5:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Observações</div>
            <div class="kpi-value">{n_observacoes}</div>
            <div class="kpi-sub">{str(primeira_data)[:10]} → {str(ultima_data)[:10]}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Gráfico de linha de preço ao longo do tempo ───────────────────────
    fig_prod = go.Figure()

    fig_prod.add_trace(go.Scatter(
        x=df_prod["extraction_date"],
        y=df_prod["price_brl"],
        mode="lines+markers",
        name="Preço",
        line=dict(color="#9b59b6", width=3),
        marker=dict(
            size=10,
            color=df_prod["price_brl"],
            colorscale=PURPLE_SCALE,
            line=dict(color="#d7bde2", width=1.5),
        ),
        hovertemplate="<b>%{x|%d/%m/%Y %H:%M}</b><br>Preço: R$ %{y:,.2f}<extra></extra>",
    ))

    # Linha de referência — preço inicial
    fig_prod.add_hline(
        y=preco_inicial,
        line_dash="dot",
        line_color="#5c3d8a",
        annotation_text=f"Inicial: {fmt_brl(preco_inicial)}",
        annotation_font_color="#a78be0",
    )

    fig_prod.update_layout(
        **PLOTLY_THEME,
        height=350,
        margin=dict(l=0, r=0, t=10, b=0),
        yaxis_tickprefix="R$ ",
        yaxis_tickformat=",.0f",
        xaxis_title="Data de extração",
        yaxis_title="Preço (R$)",
        showlegend=False,
    )
    fig_prod = apply_theme(fig_prod)
    st.plotly_chart(fig_prod, use_container_width=True)

    # ── Tabela de variação run-a-run ──────────────────────────────────────
    with st.expander("📋 Ver histórico completo de preços (linha a linha)", expanded=False):
        df_hist_view = df_prod[["extraction_date", "run_id", "price_brl"]].copy()
        df_hist_view["delta_abs"] = df_hist_view["price_brl"].diff()
        df_hist_view["delta_pct"] = df_hist_view["price_brl"].pct_change() * 100
        df_hist_view = df_hist_view.rename(columns={
            "extraction_date": "Data",
            "run_id": "Run ID",
            "price_brl": "Preço (R$)",
            "delta_abs": "Variação (R$)",
            "delta_pct": "Variação (%)",
        })
        df_hist_view["Data"] = df_hist_view["Data"].dt.strftime("%d/%m/%Y %H:%M")
        st.dataframe(
            df_hist_view,
            hide_index=True,
            use_container_width=True,
            column_config={
                "Preço (R$)": st.column_config.NumberColumn(format="R$ %.2f"),
                "Variação (R$)": st.column_config.NumberColumn(format="R$ %+.2f"),
                "Variação (%)": st.column_config.NumberColumn(format="%+.2f%%"),
            },
        )

# ── Tabela de Produtos ────────────────────────────────────────────────────────

st.markdown('<div class="section-title">📋 Catálogo de Produtos</div>', unsafe_allow_html=True)

col_search, col_sort = st.columns([3, 1])
with col_search:
    busca = st.text_input("🔍 Buscar por nome", placeholder="ex: anel coração, prata...")
with col_sort:
    sort_col = st.selectbox("Ordenar por", ["Preço (crescente)", "Preço (decrescente)", "Nome"])

name_col = "product_name" if "product_name" in df.columns else "raw_name"
df_table = df.copy()

if busca:
    mask = df_table[name_col].str.contains(busca, case=False, na=False)
    df_table = df_table[mask]

# Ordena
if sort_col == "Preço (crescente)":
    df_table = df_table.sort_values("price_brl", ascending=True)
elif sort_col == "Preço (decrescente)":
    df_table = df_table.sort_values("price_brl", ascending=False)
else:
    df_table = df_table.sort_values(name_col, ascending=True)

# Seleciona e formata colunas para exibição
display_cols = {}
if name_col in df_table.columns:
    display_cols["Nome"] = df_table[name_col].fillna(df_table.get("slug", ""))
display_cols["Categoria"] = df_table["category"].str.capitalize()
display_cols["Preço"] = df_table["price_brl"].apply(fmt_brl)
if "installment_price_brl" in df_table.columns:
    display_cols["Parcela (10x)"] = df_table["installment_price_brl"].apply(fmt_brl)
if "metal" in df_table.columns:
    display_cols["Material"] = df_table["metal"].fillna("—")
if "sizes_str" in df_table.columns:
    display_cols["Tamanhos"] = df_table["sizes_str"].fillna("—")
if "product_url" in df_table.columns:
    display_cols["Link"] = df_table["product_url"].where(df_table["product_url"].notna(), other=None)

df_display = pd.DataFrame(display_cols)

st.dataframe(
    df_display,
    use_container_width=True,
    height=420,
    hide_index=True,
    column_config={
        "Link": st.column_config.LinkColumn("Link", display_text="🔗 ver produto"),
        "Preço": st.column_config.TextColumn("Preço"),
    },
)

st.caption(f"Exibindo {len(df_display)} produto(s) · filtros aplicados")

# ── Log de Alertas Estruturais ────────────────────────────────────────────────

with st.expander("⚠️ Alertas Estruturais do Site (structure_alerts.log)", expanded=False):
    alerts = load_alerts()
    if not alerts:
        st.markdown('<span class="ok-badge">✅ Sem alertas registrados</span>', unsafe_allow_html=True)
    else:
        has_critical = any("CRITICAL" in a or "WARNING" in a for a in alerts)
        if has_critical:
            st.markdown(
                '<span class="alert-badge">⚠️ Alertas detectados — verifique abaixo</span>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown('<span class="ok-badge">✅ Site respondendo normalmente</span>', unsafe_allow_html=True)

        st.code("\n".join(alerts), language="text")

# ── Footer ────────────────────────────────────────────────────────────────────

st.markdown("""
<div style="text-align:center; margin-top:40px; padding:16px; color:#3d2a5a; font-size:0.75rem;">
    Pandora Price Monitor · Dados da camada Trusted (Parquet) · Atualiza a cada 5min
</div>
""", unsafe_allow_html=True)
