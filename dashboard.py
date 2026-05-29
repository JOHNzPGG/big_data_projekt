import streamlit as st
import pandas as pd
import glob
import os
import time

# ── PAGE CONFIG (musi być PIERWSZA komenda Streamlit) ─────────────────────────
st.set_page_config(
    page_title="WikiWatch",
    page_icon="⚔",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
#  MOTYW — domyślnie ciemny
# ─────────────────────────────────────────────
if "theme" not in st.session_state:
    st.session_state.theme = "dark"

DARK = {
    "bg":        "#0d0d14",
    "surface":   "#13131f",
    "surface2":  "#1a1a2e",
    "border":    "#2a2a45",
    "text":      "#e2e2f0",
    "muted":     "#7070a0",
    "accent":    "#e84545",
    "accent2":   "#00d4ff",
    "accent3":   "#f5a623",
    "good":      "#22c55e",
    "tab_bg":    "#1a1a2e",
    "tab_active":"#e84545",
    "sidebar":   "#0d0d14",
    "inp_bg":    "#1a1a2e",
    "metric_bg": "#13131f",
    "shadow":    "rgba(232,69,69,0.15)",
}

LIGHT = {
    "bg":        "#f4f4f8",
    "surface":   "#ffffff",
    "surface2":  "#eeeef6",
    "border":    "#d0d0e0",
    "text":      "#1a1a2e",
    "muted":     "#6060a0",
    "accent":    "#c0392b",
    "accent2":   "#0077aa",
    "accent3":   "#d97706",
    "good":      "#16a34a",
    "tab_bg":    "#eeeef6",
    "tab_active":"#c0392b",
    "sidebar":   "#eeeef6",
    "inp_bg":    "#f4f4f8",
    "metric_bg": "#ffffff",
    "shadow":    "rgba(192,57,43,0.12)",
}

T = DARK if st.session_state.theme == "dark" else LIGHT

# ─────────────────────────────────────────────
#  CSS INJECTION
# ─────────────────────────────────────────────
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');
@import url('https://fonts.googleapis.com/icon?family=Material+Icons');

/* ── ROOT ── */
html, body, .stApp {{
    background-color: {T['bg']} !important;
    color: {T['text']} !important;
    font-family: 'Syne', sans-serif;
}}

/* ── SIDEBAR COLLAPSE BUTTON (fix "keyboard_double_..." text) ── */
button[data-testid="collapsedControl"],
[data-testid="stSidebarCollapseButton"] button {{
    font-family: 'Material Icons' !important;
    font-size: 20px !important;
    color: {T['muted']} !important;
}}
button[data-testid="collapsedControl"]:hover,
[data-testid="stSidebarCollapseButton"] button:hover {{
    color: {T['text']} !important;
    background: {T['surface2']} !important;
}}

/* ── SIDEBAR ── */
[data-testid="stSidebar"] {{
    background: {T['sidebar']} !important;
    border-right: 1px solid {T['border']};
}}
[data-testid="stSidebar"] * {{
    color: {T['text']} !important;
    font-family: 'Syne', sans-serif !important;
}}

/* ── HEADER ── */
.wiki-header {{
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 1.2rem 0 0.4rem 0;
    border-bottom: 1px solid {T['border']};
    margin-bottom: 1.5rem;
}}
.wiki-logo {{
    font-size: 2.4rem;
    line-height: 1;
}}
.wiki-title {{
    font-size: 1.9rem;
    font-weight: 800;
    color: {T['text']};
    letter-spacing: -0.03em;
    line-height: 1.1;
}}
.wiki-subtitle {{
    font-size: 0.78rem;
    color: {T['muted']};
    font-family: 'JetBrains Mono', monospace;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-top: 0.1rem;
}}
.accent-dot {{
    color: {T['accent']};
}}

/* ── METRIC CARDS ── */
.metric-row {{
    display: flex;
    gap: 1rem;
    margin-bottom: 1.5rem;
    flex-wrap: wrap;
}}
.metric-card {{
    background: {T['metric_bg']};
    border: 1px solid {T['border']};
    border-radius: 10px;
    padding: 1rem 1.2rem;
    flex: 1;
    min-width: 120px;
    position: relative;
    overflow: hidden;
    transition: border-color 0.2s;
}}
.metric-card:hover {{
    border-color: {T['accent']};
}}
.metric-card::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: {T['accent']};
    border-radius: 10px 10px 0 0;
}}
.metric-card.blue::before  {{ background: {T['accent2']}; }}
.metric-card.amber::before {{ background: {T['accent3']}; }}
.metric-card.green::before {{ background: {T['good']}; }}

.metric-label {{
    font-size: 0.7rem;
    font-family: 'JetBrains Mono', monospace;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: {T['muted']};
    margin-bottom: 0.3rem;
}}
.metric-value {{
    font-size: 1.9rem;
    font-weight: 800;
    color: {T['text']};
    line-height: 1.1;
    font-variant-numeric: tabular-nums;
}}
.metric-delta {{
    font-size: 0.72rem;
    font-family: 'JetBrains Mono', monospace;
    color: {T['muted']};
    margin-top: 0.15rem;
}}

/* ── SECTION HEADER ── */
.section-header {{
    font-size: 1rem;
    font-weight: 700;
    color: {T['text']};
    letter-spacing: -0.01em;
    margin: 1.2rem 0 0.6rem 0;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}}
.section-header::after {{
    content: '';
    flex: 1;
    height: 1px;
    background: {T['border']};
    margin-left: 0.5rem;
}}

/* ── TABS ── */
[data-baseweb="tab-list"] {{
    background: {T['tab_bg']} !important;
    border-radius: 10px !important;
    padding: 4px !important;
    gap: 2px !important;
    border: 1px solid {T['border']} !important;
}}
[data-baseweb="tab"] {{
    border-radius: 7px !important;
    font-family: 'Syne', sans-serif !important;
    font-weight: 600 !important;
    font-size: 0.85rem !important;
    color: {T['muted']} !important;
    padding: 0.4rem 1rem !important;
    transition: all 0.15s !important;
}}
[aria-selected="true"][data-baseweb="tab"] {{
    background: {T['tab_active']} !important;
    color: #fff !important;
}}
[data-baseweb="tab-panel"] {{
    padding-top: 1.2rem !important;
}}

/* ── DATAFRAME ── */
[data-testid="stDataFrame"] {{
    border: 1px solid {T['border']} !important;
    border-radius: 10px !important;
    overflow: hidden;
}}
[data-testid="stDataFrame"] table {{
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.8rem !important;
}}

/* ── DIVIDER ── */
hr {{
    border-color: {T['border']} !important;
    margin: 1rem 0 !important;
}}

/* ── SELECT / RADIO / TOGGLE ── */
[data-testid="stRadio"] > label {{
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.8rem !important;
    color: {T['muted']} !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
}}
[data-testid="stToggle"] > label span {{
    color: {T['text']} !important;
}}

/* ── ALERT BOXES ── */
.stAlert {{
    border-radius: 10px !important;
    font-family: 'Syne', sans-serif !important;
    border: 1px solid {T['border']} !important;
    background: {T['surface2']} !important;
}}

/* ── STATUS BADGE ── */
.badge {{
    display: inline-block;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.7rem;
    font-weight: 500;
    padding: 2px 8px;
    border-radius: 20px;
    letter-spacing: 0.05em;
}}
.badge-red   {{ background: rgba(232,69,69,0.15);  color: {T['accent']}; border: 1px solid {T['accent']}; }}
.badge-blue  {{ background: rgba(0,212,255,0.1);   color: {T['accent2']}; border: 1px solid {T['accent2']}; }}
.badge-amber {{ background: rgba(245,166,35,0.12); color: {T['accent3']}; border: 1px solid {T['accent3']}; }}
.badge-green {{ background: rgba(34,197,94,0.12);  color: {T['good']}; border: 1px solid {T['good']}; }}

/* ── LIVE PULSE ── */
@keyframes pulse {{
    0%,100% {{ opacity: 1; }}
    50%      {{ opacity: 0.3; }}
}}
.live-dot {{
    display: inline-block;
    width: 8px; height: 8px;
    background: {T['good']};
    border-radius: 50%;
    animation: pulse 1.6s ease-in-out infinite;
    margin-right: 5px;
    vertical-align: middle;
}}

/* ── CHART AREA ── */
[data-testid="stArrowVegaLiteChart"],
[data-testid="stLineChart"],
[data-testid="stBarChart"] {{
    background: {T['surface']} !important;
    border: 1px solid {T['border']} !important;
    border-radius: 10px !important;
    padding: 0.5rem !important;
}}

/* ── MAP ── */
[data-testid="stDeckGlJsonChart"] {{
    border-radius: 12px !important;
    overflow: hidden;
    border: 1px solid {T['border']} !important;
}}

/* ── EXPANDER ── */
[data-testid="stExpander"] {{
    background: {T['surface']} !important;
    border: 1px solid {T['border']} !important;
    border-radius: 10px !important;
}}

/* ── SIDEBAR HEADING ── */
.sidebar-title {{
    font-size: 0.65rem;
    font-family: 'JetBrains Mono', monospace;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    color: {T['muted']};
    margin-bottom: 0.8rem;
    margin-top: 0.5rem;
}}

/* scrollbar */
::-webkit-scrollbar {{ width: 6px; height: 6px; }}
::-webkit-scrollbar-track {{ background: {T['bg']}; }}
::-webkit-scrollbar-thumb {{ background: {T['border']}; border-radius: 3px; }}
::-webkit-scrollbar-thumb:hover {{ background: {T['muted']}; }}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  ŚCIEŻKI DANYCH
# ─────────────────────────────────────────────
DIR_WARS    = "output_stats_history"
DIR_EDITORS = "output_super_editors"
DIR_TOPICS  = "output_topic_trends"
MAP_FILE    = os.path.join("output_live_map", "live_map.csv")

# ─────────────────────────────────────────────
#  ŁADOWANIE DANYCH
# ─────────────────────────────────────────────
@st.cache_data(ttl=5)
def load_wars() -> pd.DataFrame:
    files = glob.glob(os.path.join(DIR_WARS, "edit_wars_*.csv"))
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception:
            pass
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
    df["end_time"]   = pd.to_datetime(df["end_time"],   errors="coerce")
    return df.dropna(subset=["start_time", "end_time"]).copy()

@st.cache_data(ttl=5)
def load_editors() -> pd.DataFrame:
    files = glob.glob(os.path.join(DIR_EDITORS, "super_editors_*.csv"))
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception:
            pass
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
    return df.dropna(subset=["start_time"]).copy()

@st.cache_data(ttl=5)
def load_topics() -> pd.DataFrame:
    files = glob.glob(os.path.join(DIR_TOPICS, "topic_trends_*.csv"))
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception:
            pass
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
    return df.dropna(subset=["start_time"]).copy()

# ─────────────────────────────────────────────
#  HELPER: KARTA METRYKI
# ─────────────────────────────────────────────
def metric_card(label: str, value, delta: str = "", color: str = ""):
    cls = f"metric-card {color}"
    st.markdown(f"""
    <div class="{cls}">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {"<div class='metric-delta'>" + delta + "</div>" if delta else ""}
    </div>
    """, unsafe_allow_html=True)

def section_header(text: str):
    st.markdown(f'<div class="section-header">{text}</div>', unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="sidebar-title">WikiWatch v2</div>', unsafe_allow_html=True)

    # Przełącznik motywu
    cur_icon = "☀️" if st.session_state.theme == "dark" else "🌙"
    if st.button(f"{cur_icon}  Zmień motyw", use_container_width=True):
        st.session_state.theme = "light" if st.session_state.theme == "dark" else "dark"
        st.rerun()

    st.divider()

    st.markdown('<div class="sidebar-title">Przedział czasu</div>', unsafe_allow_html=True)
    time_filter = st.radio(
        "", 
        ["⚡ Ostatnie 15 min", "🕐 Ostatnia godzina", "📂 Cała sesja"],
        label_visibility="collapsed",
    )

    st.divider()

    st.markdown('<div class="sidebar-title">Odświeżanie</div>', unsafe_allow_html=True)
    auto_refresh = st.toggle("Live (co 10s)", value=True)

    st.divider()

    # Status plików
    st.markdown('<div class="sidebar-title">Status danych</div>', unsafe_allow_html=True)
    for label, path in [
        ("Wojny",    DIR_WARS),
        ("Edytorzy", DIR_EDITORS),
        ("Trendy",   DIR_TOPICS),
        ("Mapa",     MAP_FILE if os.path.exists(MAP_FILE) else ""),
    ]:
        has = (os.path.exists(path) and
               (os.path.isfile(path) or bool(glob.glob(os.path.join(path, "*.csv")))))
        dot  = "🟢" if has else "🔴"
        st.markdown(f"`{dot} {label}`")

# ─────────────────────────────────────────────
#  HELPER: FILTR CZASU
# ─────────────────────────────────────────────
def apply_filter(df: pd.DataFrame, col: str = "end_time") -> pd.DataFrame:
    if df.empty or col not in df.columns:
        return df
    latest = df[col].max()
    if "15" in time_filter:
        return df[df[col] >= latest - pd.Timedelta(minutes=15)]
    if "godzin" in time_filter:
        return df[df[col] >= latest - pd.Timedelta(hours=1)]
    return df

# ─────────────────────────────────────────────
#  GŁÓWNY NAGŁÓWEK
# ─────────────────────────────────────────────
now_str = time.strftime("%H:%M:%S")
df_wars_all = load_wars()

has_data = not df_wars_all.empty
live_html = (
    '<span class="live-dot"></span><span style="font-size:0.72rem;color:#22c55e;'
    'font-family:\'JetBrains Mono\',monospace;vertical-align:middle;">LIVE</span>'
    if has_data else
    '<span style="font-size:0.72rem;color:#f5a623;font-family:\'JetBrains Mono\','
    'monospace;">AWAITING DATA</span>'
)

st.markdown(f"""
<div class="wiki-header">
    <div class="wiki-logo">⚔</div>
    <div>
        <div class="wiki-title">Wiki<span class="accent-dot">Watch</span></div>
        <div class="wiki-subtitle">Edit Wars Intelligence Terminal &nbsp;·&nbsp; {now_str} &nbsp;·&nbsp; {live_html}</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  ZAKŁADKI
# ─────────────────────────────────────────────
tab_wars, tab_editors, tab_topics, tab_map = st.tabs([
    "⚔  Wojny Edycyjne",
    "🏆  Super-Edytorzy",
    "📊  Trendy Tematyczne",
    "🌍  Mapa na Żywo",
])

# ══════════════════════════════════════════════
#  TAB 1 — WOJNY EDYCYJNE
# ══════════════════════════════════════════════
with tab_wars:
    if df_wars_all.empty:
        st.info("⏳ Oczekiwanie na pierwsze dane. Pierwsze okno czasowe Sparka zamknie się po ~5 minutach.")
    else:
        filtered = apply_filter(df_wars_all)
        dedup = (
            filtered
            .sort_values(["total_edits", "end_time"], ascending=[False, False])
            .drop_duplicates(subset=["title"], keep="first")
            .copy()
        )

        # KPI
        st.markdown('<div class="metric-row">', unsafe_allow_html=True)
        col_m = st.columns(4)
        with col_m[0]:
            metric_card("Batche Sparka",
                        df_wars_all["batch_id"].nunique() if "batch_id" in df_wars_all.columns else "—")
        with col_m[1]:
            metric_card("Unikalne konflikty", len(dedup), color="blue")
        with col_m[2]:
            metric_card("Max edycji / art.",
                        int(dedup["total_edits"].max()) if not dedup.empty else 0, color="")
        with col_m[3]:
            metric_card("Łączne cofnięcia",
                        int(dedup["reverts_count"].sum()) if "reverts_count" in dedup.columns else 0,
                        color="amber")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── TABELA — pełna szerokość ──────────────────────────────
        section_header("🔥 Najgorętsze artykuły")
        if dedup.empty:
            st.success("W wybranym oknie nie wykryto konfliktów ✌")
        else:
            disp = dedup[[
                "title", "total_edits", "reverts_count",
                "unique_users", "combatants", "latest_comment",
                "start_time", "latest_revision",
            ]].copy()
            disp["url"]      = "https://en.wikipedia.org/wiki/" + disp["title"].str.replace(" ", "_", regex=False)
            disp["diff_url"] = "https://en.wikipedia.org/w/index.php?diff=" + disp["latest_revision"].astype(str)
            disp.rename(columns={
                "title":          "Artykuł",
                "total_edits":    "✏ Edycje",
                "reverts_count":  "↩ Cofnięcia",
                "unique_users":   "👤 Konta",
                "combatants":     "Walczący",
                "latest_comment": "Ostatni komentarz",
                "start_time":     "Start okna",
                "url":            "Wiki",
                "diff_url":       "Diff",
            }, inplace=True)
            disp.drop(columns=["latest_revision"], inplace=True)
            disp.reset_index(drop=True, inplace=True)
            st.dataframe(
                disp.style.background_gradient(cmap="Reds", subset=["✏ Edycje", "↩ Cofnięcia"]),
                use_container_width=True,
                hide_index=True,
                height=400,
                column_config={
                    "Wiki": st.column_config.LinkColumn("Wiki", display_text="🔗"),
                    "Diff": st.column_config.LinkColumn("Diff", display_text="🔍"),
                },
            )

        # ── WYKRES — pełna szerokość ──────────────────────────────
        section_header("📈 Natężenie wojen edycyjnych w czasie")
        trend = filtered.groupby("end_time")["total_edits"].sum().reset_index()
        trend.set_index("end_time", inplace=True)
        st.line_chart(trend, y="total_edits", color=T["accent"], use_container_width=True)

        # ── SUROWA HISTORIA — pełna szerokość ────────────────────
        section_header("📋 Surowa historia")
        with st.expander("Pokaż wszystkie okna (z nakładającymi się przedziałami)"):
            st.dataframe(
                filtered.sort_values("end_time", ascending=False),
                use_container_width=True,
                hide_index=True,
            )

# ══════════════════════════════════════════════
#  TAB 2 — SUPER-EDYTORZY
# ══════════════════════════════════════════════
with tab_editors:
    df_ed = load_editors()
    if df_ed.empty:
        st.info("⏳ Brak danych o edytorach. Poczekaj na kolejne batche Sparka.")
    else:
        filtered_ed = apply_filter(df_ed, col="start_time")
        dedup_ed = (
            filtered_ed
            .sort_values("edit_count", ascending=False)
            .drop_duplicates(subset=["user"], keep="first")
            .copy()
        )

        bots_n = int((dedup_ed["is_bot_flag"] == True).sum())
        susp_n = int((dedup_ed["is_suspicious_flag"] == True).sum())
        human_n = len(dedup_ed) - bots_n - susp_n

        col_m2 = st.columns(3)
        with col_m2[0]:
            metric_card("👤 Ludzie", human_n, color="green")
        with col_m2[1]:
            metric_card("🤖 Boty (znane)", bots_n, color="blue")
        with col_m2[2]:
            metric_card("🤖? Podejrzane", susp_n, color="amber")

        col_a, col_b = st.columns(2)

        with col_a:
            section_header("🏆 Top 20 aktywnych")
            top20 = dedup_ed.nlargest(20, "edit_count")[[
                "user", "edit_count", "is_bot_flag",
                "is_suspicious_flag", "avg_length_diff", "langs_edited",
            ]].copy()
            top20["typ"] = top20.apply(
                lambda r: "🤖?" if r["is_suspicious_flag"]
                else ("🤖" if r["is_bot_flag"] else "👤"), axis=1,
            )
            top20.drop(columns=["is_bot_flag", "is_suspicious_flag"], inplace=True)
            top20.rename(columns={
                "user": "Użytkownik", "edit_count": "Edycje",
                "typ": "Typ", "avg_length_diff": "Śr. ΔBajty",
                "langs_edited": "Języki",
            }, inplace=True)
            top20.reset_index(drop=True, inplace=True)
            st.dataframe(
                top20.style.background_gradient(cmap="Blues", subset=["Edycje"]),
                use_container_width=True, hide_index=True,
            )

        with col_b:
            section_header("🤖? Podejrzane konta")
            susp_df = dedup_ed[dedup_ed["is_suspicious_flag"] == True]
            if susp_df.empty:
                st.success("Brak podejrzanych kont w wybranym przedziale. ✅")
                st.markdown(
                    '<div class="badge badge-green">Kryterium: ≥10 edycji/60s</div>',
                    unsafe_allow_html=True,
                )
            else:
                s = susp_df[["user", "edit_count", "avg_length_diff", "langs_edited"]].copy()
                s.rename(columns={
                    "user": "Konto", "edit_count": "Edycje/okno",
                    "avg_length_diff": "Śr. ΔBajty", "langs_edited": "Języki",
                }, inplace=True)
                s.reset_index(drop=True, inplace=True)
                st.dataframe(
                    s.style.applymap(lambda _: f"background-color: rgba(232,69,69,0.07)"),
                    use_container_width=True, hide_index=True,
                )
                st.markdown(
                    '<div class="badge badge-amber">Kryterium: ≥10 edycji w 60s, nieoznaczony jako bot</div>',
                    unsafe_allow_html=True,
                )

# ══════════════════════════════════════════════
#  TAB 3 — TRENDY TEMATYCZNE
# ══════════════════════════════════════════════
with tab_topics:
    df_top = load_topics()
    if df_top.empty:
        st.info("⏳ Brak danych o trendach tematycznych.")
    else:
        filtered_top = apply_filter(df_top, col="start_time")
        dedup_top = (
            filtered_top
            .sort_values("total_edits", ascending=False)
            .drop_duplicates(subset=["topic"], keep="first")
            .head(20)
            .copy()
        )

        section_header("🔥 Top 20 tematów wg edycji")

        col_ch, col_tb = st.columns([1, 1])
        with col_ch:
            chart_data = dedup_top.set_index("topic")["total_edits"].sort_values(ascending=True)
            st.bar_chart(chart_data, color=T["accent3"], horizontal=True)
        with col_tb:
            show = dedup_top[["topic", "article_count", "total_edits", "sample_titles"]].rename(
                columns={
                    "topic": "Temat", "article_count": "Artykuły",
                    "total_edits": "Edycje", "sample_titles": "Przykłady",
                }
            )
            show.reset_index(drop=True, inplace=True)
            st.dataframe(show, use_container_width=True, hide_index=True)

        st.caption("Metodologia: temat = pierwsze słowo tytułu artykułu. Wyświetlane grupy z ≥2 artykułami.")

# ══════════════════════════════════════════════
#  TAB 4 — MAPA NA ŻYWO
# ══════════════════════════════════════════════
with tab_map:
    section_header("🌍 Edycje z niezalogowanych IP — geolokalizacja na żywo")

    if not os.path.exists(MAP_FILE):
        st.info(
            "Oczekiwanie na plik `live_map.csv`. "
            "Pojawi się, gdy Spark przetworzy pierwsze edycje z adresów IP. "
            "Większość edytorów jest zalogowana, więc może to chwilę potrwać."
        )
        st.markdown("""
        **Dlaczego mapa może być pusta?**
        - Wikipedia ma ~85% edycji od zalogowanych użytkowników (bez geolokalizacji)
        - ip-api.com działa z limitem ~42 req/min — pierwsze IP są sprawdzane sukcesywnie
        """)
    else:
        try:
            map_df = pd.read_csv(MAP_FILE).dropna(subset=["lat", "lon"])
            if map_df.empty:
                st.warning("🛰️ Plik mapy istnieje, ale nie zawiera punktów z współrzędnymi.")
            else:
                # Pasek z liczbą punktów i podziałem językowym
                col_stat = st.columns([1, 2])
                with col_stat[0]:
                    metric_card("Aktywnych punktów IP", len(map_df), color="blue")
                with col_stat[1]:
                    if "wiki_lang" in map_df.columns:
                        section_header("Edycje wg języka Wiki")
                        lang_counts = map_df["wiki_lang"].value_counts().head(10)
                        st.bar_chart(lang_counts, color=T["accent2"])

                st.map(map_df[["lat", "lon"]], color="#ff4b4b", size=300)
                mtime = os.path.getmtime(MAP_FILE)
                age_s = int(time.time() - mtime)
                st.caption(
                    f"Plik zaktualizowany {age_s}s temu · "
                    f"{len(map_df)} punktów · Odświeżanie Sparka co 5s"
                )
        except Exception as exc:
            st.error(f"Błąd ładowania mapy: {exc}")

# ─────────────────────────────────────────────
#  PĘTLA LIVE
# ─────────────────────────────────────────────
if auto_refresh:
    time.sleep(10)
    st.rerun()
