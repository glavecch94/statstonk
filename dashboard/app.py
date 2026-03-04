"""
Punto di ingresso della dashboard Streamlit.
Avvia con: streamlit run dashboard/app.py
"""

import streamlit as st


@st.cache_resource
def _init_scheduler():
    """
    Avvia lo scheduler APScheduler in background thread.
    Eseguito una sola volta per processo Streamlit grazie a cache_resource.
    """
    try:
        from scheduler.runner import start_background_scheduler
        return start_background_scheduler()
    except Exception as e:
        # Non blocca la dashboard se lo scheduler fallisce
        import logging
        logging.getLogger(__name__).warning(f"Scheduler non avviato: {e}")
        return None


_init_scheduler()

st.set_page_config(
    page_title="Statstonk",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("⚽ Statstonk")
st.markdown("Piattaforma di analisi calcistica per il betting — Serie A.")

col1, col2, col3 = st.columns(3)
with col1:
    st.info("**📅 Prossime Partite**\nFixture, segnali pre-match, quote e formazioni.")
with col2:
    st.info("**⚡ Live**\nxG in tempo reale, momentum e statistiche partita.")
with col3:
    st.info("**📊 Storico**\nPick con esiti, hit rate, ROI e simulazione bankroll.")

st.markdown("---")
st.caption("Usa la barra laterale per navigare tra le sezioni.")
