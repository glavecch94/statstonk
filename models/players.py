from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models.base import Base


class Player(Base):
    __tablename__ = "players"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100))
    team_id: Mapped[int | None] = mapped_column(ForeignKey("teams.id"))
    nationality: Mapped[str | None] = mapped_column(String(50))
    position: Mapped[str | None] = mapped_column(String(30))  # GK, DF, MF, FW
    birth_date: Mapped[date | None] = mapped_column(Date)

    # ID sorgenti esterne
    fbref_id: Mapped[str | None] = mapped_column(String(20))
    sofascore_id: Mapped[int | None] = mapped_column(Integer)
    understat_id: Mapped[int | None] = mapped_column(Integer)
    transfermarkt_id: Mapped[str | None] = mapped_column(String(20))

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    team: Mapped[Team] = relationship("Team", back_populates="players")  # type: ignore[name-defined]
    match_stats: Mapped[list[PlayerMatchStat]] = relationship(
        "PlayerMatchStat", back_populates="player"
    )

    def __repr__(self) -> str:
        return f"<Player {self.name}>"


class PlayerMatchStat(Base):
    """Statistiche di un giocatore in una singola partita."""

    __tablename__ = "player_match_stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.id"))
    match_id: Mapped[int] = mapped_column(ForeignKey("matches.id"))
    team_id: Mapped[int] = mapped_column(ForeignKey("teams.id"))

    # Partecipazione
    minutes_played: Mapped[int | None] = mapped_column(Integer)
    started: Mapped[bool] = mapped_column(Boolean, default=False)

    # Attacco
    goals: Mapped[int] = mapped_column(Integer, default=0)
    assists: Mapped[int] = mapped_column(Integer, default=0)
    xg: Mapped[float | None] = mapped_column(Float)
    xa: Mapped[float | None] = mapped_column(Float)
    npxg: Mapped[float | None] = mapped_column(Float)   # xG senza rigori
    shots: Mapped[int | None] = mapped_column(Integer)
    shots_on_target: Mapped[int | None] = mapped_column(Integer)

    # Disciplina
    yellow_cards: Mapped[int] = mapped_column(Integer, default=0)
    red_cards: Mapped[int] = mapped_column(Integer, default=0)
    fouls_committed: Mapped[int | None] = mapped_column(Integer)
    fouls_drawn: Mapped[int | None] = mapped_column(Integer)

    # Calci piazzati / rigori
    corners_taken: Mapped[int | None] = mapped_column(Integer)
    penalties_scored: Mapped[int | None] = mapped_column(Integer)
    penalties_attempted: Mapped[int | None] = mapped_column(Integer)

    # Difesa
    tackles: Mapped[int | None] = mapped_column(Integer)
    interceptions: Mapped[int | None] = mapped_column(Integer)

    player: Mapped[Player] = relationship("Player", back_populates="match_stats")
    match: Mapped[Match] = relationship("Match", back_populates="player_stats")  # type: ignore[name-defined]

    __table_args__ = (
        Index("ix_player_match_stats_player", "player_id"),
        Index("ix_player_match_stats_match", "match_id"),
    )
