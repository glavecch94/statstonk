"""
Componenti grafici condivisi tra le pagine della dashboard.

Esporta:
  half_pitch_svg(formation, players, is_home) -> str (SVG string)
  render_lineup(formation, players, is_home, team_label, badge)  -> None (Streamlit)
"""

from __future__ import annotations

import base64
from collections import defaultdict

import streamlit as st

# ── SVG campo (metà) con maglie giocatori ─────────────────────────────────────


def half_pitch_svg(
    formation: str | None,
    players: list[dict],
    is_home: bool,
) -> str:
    """
    Genera l'SVG di metà campo con i giocatori rappresentati come maglie.

    - Casa (is_home=True): portiere in basso, attaccanti in alto; maglia blu.
    - Ospite (is_home=False): portiere in alto, attaccanti in basso; maglia rossa.
    - Usa il campo `grid` ("riga:colonna") di SofaScore per posizionamento preciso.
    - SofaScore ordina i giocatori da destra a sinistra (prospettiva di attacco):
      per la casa invertiamo l'ordine di colonna, per l'ospite lo lasciamo.
    """
    W, H = 340, 480
    PX, PY = 10, 10
    PW, PH = W - 2 * PX, H - 2 * PY  # 320 × 460
    R = 17

    # ── Righe di giocatori ────────────────────────────────────────────────────

    def _rows(form: str | None, pl: list[dict]) -> list[list[dict]]:
        starters = [p for p in pl if p.get("is_starting", True)]
        with_grid = [p for p in starters if p.get("grid")]
        if with_grid:
            rd: dict[int, list[tuple[int, dict]]] = defaultdict(list)
            for p in with_grid:
                parts = str(p["grid"]).split(":")
                rn = int(parts[0])
                cn = int(parts[1]) if len(parts) > 1 else 1
                rd[rn].append((cn, p))
            return [[p for _, p in sorted(rd[rn])] for rn in sorted(rd)]
        # Fallback: distribuzione per stringa formazione
        sizes = (
            [1] + [int(x) for x in form.split("-")]
            if form and "-" in form
            else [1, 4, 3, 3]
        )
        rows: list[list[dict]] = []
        idx = 0
        for n in sizes:
            chunk = starters[idx : idx + n]
            if chunk:
                rows.append(chunk)
            idx += n
        return rows

    # ── Icona maglia ──────────────────────────────────────────────────────────

    def _jersey(cx: float, cy: float, number, name: str, fill: str) -> str:
        """SVG a forma di maglia calcistica con numero e cognome."""
        bw = R * 1.4    # larghezza corpo
        bh = R * 1.9    # altezza corpo
        sw = R * 0.55   # estensione manica oltre il corpo
        sh = R * 0.65   # altezza della parte a spalla (scollatura)
        bx = cx - bw / 2
        by_ = cy - bh / 2
        mey = by_ + sh      # punto di giunzione manica-corpo
        boty = by_ + bh     # fondo maglia

        # Path maglia (senso orario): manica sx → corpo → manica dx → colletto V
        path = " ".join([
            f"M {bx - sw:.0f},{by_:.0f}",            # manica sx: angolo esterno sup
            f"L {bx:.0f},{mey:.0f}",                  # giunzione manica sx - corpo
            f"L {bx:.0f},{boty:.0f}",                 # bordo sx fondo
            f"L {bx + bw:.0f},{boty:.0f}",            # bordo dx fondo
            f"L {bx + bw:.0f},{mey:.0f}",             # giunzione corpo - manica dx
            f"L {bx + bw + sw:.0f},{by_:.0f}",        # manica dx: angolo esterno sup
            f"L {cx + bw * 0.22:.0f},{by_:.0f}",      # colletto dx
            f"L {cx:.0f},{by_ + R * 0.4:.0f}",        # punta colletto V
            f"L {cx - bw * 0.22:.0f},{by_:.0f}",      # colletto sx
            "Z",
        ])

        short = (name or "?").split()[-1][:9]
        num_s = str(number) if number else ""
        return (
            f'<path d="{path}" fill="{fill}" stroke="rgba(255,255,255,0.85)" '
            f'stroke-width="1.5" stroke-linejoin="round"/>'
            f'<text x="{cx:.0f}" y="{cy + 5:.0f}" text-anchor="middle" '
            f'font-size="12" font-weight="900" fill="white">{num_s}</text>'
            f'<text x="{cx:.0f}" y="{boty + 13:.0f}" text-anchor="middle" '
            f'font-size="10" fill="rgba(255,255,255,0.95)">{short}</text>'
        )

    # ── Posizionamento giocatori ───────────────────────────────────────────────

    rows = _rows(formation, players)
    nr = len(rows)
    fill = "#1e56a0" if is_home else "#c0392b"
    elems: list[str] = []
    for i, row in enumerate(rows):
        yf = (0.87 - i / max(nr - 1, 1) * 0.72) if is_home else (0.13 + i / max(nr - 1, 1) * 0.72)
        y = PY + yf * PH
        n = len(row)
        for j, p in enumerate(row):
            # SS ordina da destra a sinistra (prospettiva di attacco):
            # casa → invertiamo l'indice; ospite → ordine originale.
            col_idx = (n - 1 - j) if is_home else j
            xf = 0.5 if n == 1 else 0.10 + col_idx / (n - 1) * 0.80
            x = PX + xf * PW
            elems.append(_jersey(x, y, p.get("shirt_number") or "", p.get("name", ""), fill))

    # ── Disegno campo ─────────────────────────────────────────────────────────

    cx_p = PX + PW / 2
    pw2, ph_pen = PW * 0.285, PH * 0.22
    gw2, gh = PW * 0.105, PH * 0.042
    if is_home:
        pen_y, goal_y, center_y = PY + PH - ph_pen, PY + PH - gh, PY
        spot_y = PY + PH * 0.77
    else:
        pen_y, goal_y, center_y = PY, PY, PY + PH
        spot_y = PY + PH * 0.23

    stripes = "".join(
        f'<rect x="{PX}" y="{PY + i * PH / 7:.1f}" width="{PW}" '
        f'height="{PH / 7:.1f}" fill="{"#2d9a46" if i % 2 == 0 else "#258c3c"}"/>'
        for i in range(7)
    )
    pitch = (
        f'<rect x="{PX}" y="{PY}" width="{PW}" height="{PH}" rx="3" fill="#258c3c"/>'
        + stripes
        + f'<rect x="{PX}" y="{PY}" width="{PW}" height="{PH}" rx="3" '
          f'fill="none" stroke="white" stroke-width="2"/>'
        + f'<line x1="{PX}" y1="{center_y:.1f}" x2="{PX + PW}" y2="{center_y:.1f}" '
          f'stroke="rgba(255,255,255,0.45)" stroke-width="1.5" stroke-dasharray="6,4"/>'
        + f'<rect x="{cx_p - pw2:.1f}" y="{pen_y:.1f}" '
          f'width="{pw2 * 2:.1f}" height="{ph_pen:.1f}" '
          f'fill="none" stroke="white" stroke-width="1.5"/>'
        + f'<rect x="{cx_p - gw2:.1f}" y="{goal_y:.1f}" '
          f'width="{gw2 * 2:.1f}" height="{gh:.1f}" '
          f'fill="none" stroke="white" stroke-width="1.5"/>'
        + f'<circle cx="{cx_p:.1f}" cy="{spot_y:.1f}" r="2.5" fill="white"/>'
    )
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
        f'width="100%" font-family="Arial,sans-serif">'
        f"{pitch}{''.join(elems)}</svg>"
    )


# ── Render Streamlit di una singola lineup ─────────────────────────────────────


def render_lineup(
    formation: str | None,
    players: list[dict],
    is_home: bool,
    team_label: str,
    badge: str,
) -> None:
    """
    Renderizza una lineup in Streamlit: caption + SVG campo + riga panchina.

    I player devono avere i campi: name, shirt_number, is_starting, grid (opzionale).
    """
    starters = [p for p in players if p.get("is_starting")]
    if not starters:
        st.caption("Non ancora disponibili.")
        return
    form_lbl = f"`{formation}`" if formation else ""
    st.caption(f"{badge} **{team_label}** {form_lbl}")
    svg = half_pitch_svg(formation, players, is_home)
    b64 = base64.b64encode(svg.encode()).decode()
    st.markdown(
        f'<img src="data:image/svg+xml;base64,{b64}" style="width:100%;border-radius:4px"/>',
        unsafe_allow_html=True,
    )
    bench = [p for p in players if not p.get("is_starting")]
    if bench:
        st.caption(
            "Panchina: "
            + "  ·  ".join(
                (f"{p.get('shirt_number', '')}. " if p.get("shirt_number") else "")
                + p.get("name", "")
                for p in bench
            )
        )
