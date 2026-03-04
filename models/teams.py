from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models.base import Base


class Team(Base):
    __tablename__ = "teams"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    short_name: Mapped[str | None] = mapped_column(String(20))
    league: Mapped[str] = mapped_column(String(50), nullable=False)   # es. "serie_a"
    season: Mapped[str] = mapped_column(String(10), nullable=False)   # es. "2024-25"

    # ID sorgenti esterne
    fbref_id: Mapped[str | None] = mapped_column(String(20))
    sofascore_id: Mapped[int | None] = mapped_column(Integer)
    transfermarkt_id: Mapped[str | None] = mapped_column(String(20))
    api_football_id: Mapped[int | None] = mapped_column(Integer)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    home_matches: Mapped[list[Match]] = relationship(  # type: ignore[name-defined]
        "Match", foreign_keys="Match.home_team_id", back_populates="home_team"
    )
    away_matches: Mapped[list[Match]] = relationship(  # type: ignore[name-defined]
        "Match", foreign_keys="Match.away_team_id", back_populates="away_team"
    )
    players: Mapped[list[Player]] = relationship(  # type: ignore[name-defined]
        "Player", back_populates="team"
    )

    def __repr__(self) -> str:
        return f"<Team {self.name} ({self.league} {self.season})>"
