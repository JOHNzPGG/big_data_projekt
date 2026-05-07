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


# 2. Funkcja do ładowania danych
@st.cache_data(ttl=5)
def load_data():
    files = glob.glob(os.path.join(OUTPUT_DIR, "edit_wars_*.csv"))
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception:
            pass

    if dfs:
        df = pd.concat(dfs, ignore_index=True)

        # ZMIANA: Parametr errors='coerce' sprawi, że Pandas zamiast crashować,
        # zamieni błędne wartości (jak "Q116...") na puste pole NaT (Not a Time).
        df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
        df['end_time'] = pd.to_datetime(df['end_time'], errors='coerce')

        # TARCZA: Usuwamy z tabeli wszystkie zepsute, przesunięte wiersze
        df = df.dropna(subset=['start_time', 'end_time']).copy()

        return df

    return pd.DataFrame()


df = load_data()

# 3. Panel boczny (Sidebar) - WEHIKUŁ CZASU
st.sidebar.header("⚙️ Ustawienia i Filtry")
auto_refresh = st.sidebar.toggle("Odświeżanie Live (co 10s)", value=True)
st.sidebar.markdown("---")

if df.empty:
    st.info("⏳ Oczekiwanie na dane ze Sparka. (Pamiętaj, pierwsze okno zamyka się po 5 minutach!)")
else:
    # WIDŻET: Wybór zakresu czasu
    time_filter = st.sidebar.radio(
        "🕰️ Pokaż konflikty z:",
        ["Ostatnie 15 minut", "Ostatnia godzina", "Cała historia sesji"]
    )

    # Filtracja danych na podstawie wyboru użytkownika
    latest_time = df['end_time'].max()
    if time_filter == "Ostatnie 15 minut":
        filtered_df = df[df['end_time'] >= latest_time - pd.Timedelta(minutes=15)]
    elif time_filter == "Ostatnia godzina":
        filtered_df = df[df['end_time'] >= latest_time - pd.Timedelta(hours=1)]
    else:
        filtered_df = df

    # DEDUPLIKACJA: Kluczowa naprawa problemu "podwójnych artykułów".
    # Sortujemy od największej liczby edycji, żeby zachować "szczyt" wojny, i usuwamy powtórki tytułów.
    dedup_df = filtered_df.sort_values(by=['total_edits', 'end_time'], ascending=[False, False])
    dedup_df = dedup_df.drop_duplicates(subset=['title'], keep='first').copy()

    # 4. Główne wskaźniki (KPI)
    col1, col2, col3 = st.columns(3)
    col1.metric("📦 Zbadane mikro-partie (Batche)", df['batch_id'].nunique())
    col2.metric(f"🔥 Unikalne konflikty ({time_filter.lower()})", len(dedup_df))
    col3.metric("📊 Łączna liczba zdarzeń (surowych)", len(filtered_df))

    st.divider()

    col_left, col_right = st.columns([1.2, 1])

    with col_left:
        st.subheader(f"🔥 Najgorętsze artykuły ({time_filter})")

        # Przygotowanie tabeli ze zdeduplikowanych danych
        display_df = dedup_df[['title', 'total_edits', 'unique_users', 'combatants', 'latest_comment', 'start_time',
                               'latest_revision']].copy()

        display_df['url'] = "https://en.wikipedia.org/wiki/" + display_df['title'].str.replace(' ', '_')
        display_df['diff_url'] = "https://en.wikipedia.org/w/index.php?diff=" + display_df['latest_revision'].astype(
            str)

        display_df.rename(columns={
            'title': 'Tytuł Artykułu',
            'total_edits': 'Liczba Edycji',
            'unique_users': 'Konta',
            'combatants': 'Walczący',
            'latest_comment': 'Ostatni powód',
            'start_time': 'Początek okna',
            'url': 'Link Wiki',
            'diff_url': 'Co zmieniono?'
        }, inplace=True)

        display_df.drop(columns=['latest_revision'], inplace=True)
        display_df.reset_index(drop=True, inplace=True)

        if display_df.empty:
            st.success("W wybranym oknie czasowym panuje spokój.")
        else:
            st.dataframe(
                display_df.style.background_gradient(cmap='Reds', subset=['Liczba Edycji']),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Link Wiki": st.column_config.LinkColumn("Adres URL", display_text="🔗 Wiki"),
                    "Co zmieniono?": st.column_config.LinkColumn("Podgląd", display_text="🔍 Zbadaj")
                }
            )

    with col_right:
        st.subheader("📈 Natężenie wojen edycyjnych (Trend)")
        # Wykres pozostaje na surowych danych, aby pokazywać płynną "falę" edycji w czasie
        trend_df = filtered_df.groupby('end_time')['total_edits'].sum().reset_index()
        trend_df.set_index('end_time', inplace=True)
        st.line_chart(trend_df, y='total_edits', color="#FF4B4B")

    # Sekcja dla wnikliwych
    with st.expander("Zobacz surową historię wszystkich zdarzeń (z nakładającymi się oknami)"):
        st.dataframe(filtered_df.sort_values('end_time', ascending=False), use_container_width=True)

# 7. Mechanizm Pętli Live
if auto_refresh:
    time.sleep(10)
    st.rerun()