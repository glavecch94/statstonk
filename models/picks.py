from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class Pick(Base):
    """
    Pick automatico generato dalla pagina Prossima Giornata.

    Creato quando un mercato ha ≥2 segnali ✅ e quota Pinnacle ≥1.2.
    Nessuna FK a Match: l'identificazione post-partita avviene per
    (home_team, away_team, match_date ± 4h).
    """

    __tablename__ = "picks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    league: Mapped[str] = mapped_column(String(50))
    season: Mapped[str] = mapped_column(String(10))
    matchday: Mapped[int | None] = mapped_column(Integer, nullable=True)
    home_team: Mapped[str] = mapped_column(String(100))
    away_team: Mapped[str] = mapped_column(String(100))
    match_date: Mapped[datetime] = mapped_column(DateTime)
    mercato: Mapped[str] = mapped_column(String(100))
    quota: Mapped[float | None] = mapped_column(Float, nullable=True)
    segnali: Mapped[int] = mapped_column(Integer)
    # True = vincente, False = perdente, None = in attesa
    esito: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint(
            "home_team", "away_team", "match_date", "mercato",
            name="uq_pick_match_mercato",
        ),
    )

    def __repr__(self) -> str:
        esito_str = {True: "✅", False: "❌", None: "⏳"}.get(self.esito, "?")
        return f"<Pick {self.home_team} vs {self.away_team} | {self.mercato} {esito_str}>"
