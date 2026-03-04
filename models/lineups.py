from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models.base import Base


class Lineup(Base):
    """
    Formazione per un lato (home/away) di una partita.
    Identificata univocamente da (tm_match_id, side).
    """

    __tablename__ = "lineups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tm_match_id: Mapped[str] = mapped_column(String(20), nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)  # "home" | "away"

    # Identificazione human-readable (nomi interni football-data.co.uk)
    home_team: Mapped[str] = mapped_column(String(100))
    away_team: Mapped[str] = mapped_column(String(100))
    match_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    league: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Stato formazione
    is_official: Mapped[bool] = mapped_column(Boolean, default=False)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    formation: Mapped[str | None] = mapped_column(String(20), nullable=True)
    scraped_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    players: Mapped[list[LineupPlayer]] = relationship(
        "LineupPlayer", back_populates="lineup", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("tm_match_id", "side", name="uq_lineup_match_side"),
        Index("ix_lineups_home_away_date", "home_team", "away_team", "match_date"),
    )

    def __repr__(self) -> str:
        status = "✅" if self.is_official else "🟡"
        return f"<Lineup {self.home_team} vs {self.away_team} | {self.side} {status}>"


class LineupPlayer(Base):
    """Singolo giocatore in una formazione (titolare o sostituto)."""

    __tablename__ = "lineup_players"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lineup_id: Mapped[int] = mapped_column(ForeignKey("lineups.id"))
    name: Mapped[str] = mapped_column(String(150))
    shirt_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    position: Mapped[str | None] = mapped_column(String(10), nullable=True)
    grid: Mapped[str | None] = mapped_column(String(10), nullable=True)
    is_starting: Mapped[bool] = mapped_column(Boolean, default=True)
    is_captain: Mapped[bool] = mapped_column(Boolean, default=False)

    lineup: Mapped[Lineup] = relationship("Lineup", back_populates="players")
