import streamlit as st
import pandas as pd
import glob
import os
import time

# 1. Konfiguracja wyglądu strony
st.set_page_config(page_title="Wiki Edit Wars Monitor", page_icon="🔴", layout="wide")

st.title("🔴 Wikipedia Live Edit Wars Monitor")
st.markdown("Dashboard śledzący masowe edycje i konflikty na Wikipedii w czasie rzeczywistym.")

OUTPUT_DIR = "output_stats_history"


# 2. Funkcja do ładowania i łączenia wszystkich CSV (zoptymalizowana cachem)
@st.cache_data(ttl=5)  # Streamlit trzyma dane w pamięci przez 5 sekund
def load_data():
    if not os.path.exists(OUTPUT_DIR):
        return pd.DataFrame()

    # Szukamy wszystkich plików z historią
    files = glob.glob(os.path.join(OUTPUT_DIR, "edit_wars_*.csv"))
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception:
            pass  # Czasem plik może być w ułamku sekundy zapisywany przez Sparka, ignorujemy go

    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        # Rzutowanie na format czasu
        df['start_time'] = pd.to_datetime(df['start_time'])
        df['end_time'] = pd.to_datetime(df['end_time'])
        return df

    return pd.DataFrame()


# Pobieramy dane
df = load_data()

# 3. Panel boczny (Sidebar)
st.sidebar.header("⚙️ Ustawienia")
auto_refresh = st.sidebar.toggle("Odświeżanie Live (co 10s)", value=True)
st.sidebar.markdown("---")
st.sidebar.info("Dashboard czyta dane generowane przez Apache Spark z okien czasowych EventStreams API.")

if df.empty:
    st.info(
        "⏳ Oczekiwanie na dane ze Sparka... Upewnij się, że w folderze `output_stats_history` pojawiły się pierwsze pliki CSV.")
else:
    # 4. Główne wskaźniki (KPI)
    latest_batch = df['batch_id'].max()
    latest_data = df[df['batch_id'] == latest_batch]

    col1, col2, col3 = st.columns(3)
    col1.metric("📦 Ostatni Batch ID", latest_batch)
    col2.metric("🔥 Gorące tematy (teraz)", len(latest_data))
    col3.metric("📊 Łączna liczba alertów", len(df))

    st.divider()

    # 5. Podział ekranu na Tabelę i Wykres
    col_left, col_right = st.columns([1.2, 1])

    with col_left:
        st.subheader("🔥 Najgorętsze artykuły (Bieżące okno)")

        # Pobieramy teraz nowe kolumny z wrogami i komentarzem
        display_df = latest_data[
            ['title', 'total_edits', 'unique_users', 'combatants', 'latest_comment', 'start_time']].copy()

        display_df['url'] = "https://en.wikipedia.org/wiki/" + display_df['title'].str.replace(' ', '_')

        display_df.rename(columns={
            'title': 'Tytuł Artykułu',
            'total_edits': 'Liczba Edycji',
            'unique_users': 'Konta',
            'combatants': 'Walczący (Kto)',  # <-- NOWE
            'latest_comment': 'Ostatni powód (O co)',  # <-- NOWE
            'start_time': 'Początek okna',
            'url': 'Link'
        }, inplace=True)

        display_df.reset_index(drop=True, inplace=True)
        if display_df.empty:
            st.success("W obecnym oknie czasowym panuje spokój. Brak wojen edycyjnych.")
        else:
            # Używamy st.column_config do sformatowania kolumny 'Link'
            st.dataframe(
                display_df.style.background_gradient(cmap='Reds', subset=['Liczba Edycji']),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Link": st.column_config.LinkColumn(
                        "Adres URL",
                        display_text="🔗 Otwórz Wiki"  # Tekst wyświetlany zamiast długiego linku
                    )
                }
            )

    with col_right:
        st.subheader("📈 Natężenie wojen edycyjnych w czasie")
        # Wykres liniowy pokazujący trend (suma wszystkich edycji ze wszystkich konfliktów per batch)
        trend_df = df.groupby('end_time')['total_edits'].sum().reset_index()
        trend_df.set_index('end_time', inplace=True)
        st.line_chart(trend_df, y='total_edits', color="#FF4B4B")

    # 6. Pełna historia na dole
    with st.expander("Zobacz surową historię wszystkich zdarzeń"):
        st.dataframe(df.sort_values('end_time', ascending=False), use_container_width=True)

# 7. Mechanizm Pętli Live
if auto_refresh:
    time.sleep(10)
    st.rerun()