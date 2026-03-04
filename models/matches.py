from __future__ import annotations

import enum
from datetime import datetime

from sqlalchemy import DateTime, Enum, Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models.base import Base


class MatchStatus(str, enum.Enum):
    SCHEDULED = "scheduled"
    LIVE = "live"
    FINISHED = "finished"
    POSTPONED = "postponed"
    CANCELLED = "cancelled"


class Match(Base):
    __tablename__ = "matches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    external_id: Mapped[str | None] = mapped_column(String(50))  # id sofascore / api-football
    league: Mapped[str] = mapped_column(String(50))
    season: Mapped[str] = mapped_column(String(10))
    matchday: Mapped[int | None] = mapped_column(Integer)
    date: Mapped[datetime] = mapped_column(DateTime)
    status: Mapped[MatchStatus] = mapped_column(
        Enum(MatchStatus), default=MatchStatus.SCHEDULED
    )

    home_team_id: Mapped[int] = mapped_column(ForeignKey("teams.id"))
    away_team_id: Mapped[int] = mapped_column(ForeignKey("teams.id"))

    # Risultato
    home_score: Mapped[int | None] = mapped_column(Integer)
    away_score: Mapped[int | None] = mapped_column(Integer)
    home_ht_score: Mapped[int | None] = mapped_column(Integer)
    away_ht_score: Mapped[int | None] = mapped_column(Integer)

    # xG (da FBref / Understat / SofaScore)
    home_xg: Mapped[float | None] = mapped_column(Float)
    away_xg: Mapped[float | None] = mapped_column(Float)

    # Stats aggregate post-partita
    home_shots: Mapped[int | None] = mapped_column(Integer)
    away_shots: Mapped[int | None] = mapped_column(Integer)
    home_shots_on_target: Mapped[int | None] = mapped_column(Integer)
    away_shots_on_target: Mapped[int | None] = mapped_column(Integer)
    home_corners: Mapped[int | None] = mapped_column(Integer)
    away_corners: Mapped[int | None] = mapped_column(Integer)
    home_fouls: Mapped[int | None] = mapped_column(Integer)
    away_fouls: Mapped[int | None] = mapped_column(Integer)
    home_yellow_cards: Mapped[int | None] = mapped_column(Integer)
    away_yellow_cards: Mapped[int | None] = mapped_column(Integer)
    home_red_cards: Mapped[int | None] = mapped_column(Integer)
    away_red_cards: Mapped[int | None] = mapped_column(Integer)
    home_offsides: Mapped[int | None] = mapped_column(Integer)
    away_offsides: Mapped[int | None] = mapped_column(Integer)
    home_possession: Mapped[float | None] = mapped_column(Float)
    away_possession: Mapped[float | None] = mapped_column(Float)

    source: Mapped[str | None] = mapped_column(String(50))  # quale scraper ha popolato
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    home_team: Mapped[Team] = relationship(  # type: ignore[name-defined]
        "Team", foreign_keys=[home_team_id], back_populates="home_matches"
    )
    away_team: Mapped[Team] = relationship(  # type: ignore[name-defined]
        "Team", foreign_keys=[away_team_id], back_populates="away_matches"
    )
    odds: Mapped[list[Odd]] = relationship(
        "Odd", back_populates="match", cascade="all, delete-orphan"
    )
    live_snapshots: Mapped[list[LiveSnapshot]] = relationship(
        "LiveSnapshot", back_populates="match", cascade="all, delete-orphan"
    )
    player_stats: Mapped[list[PlayerMatchStat]] = relationship(  # type: ignore[name-defined]
        "PlayerMatchStat", back_populates="match"
    )

    __table_args__ = (
        Index("ix_matches_date", "date"),
        Index("ix_matches_league_season", "league", "season"),
    )

    def __repr__(self) -> str:
        return f"<Match {self.home_team_id} vs {self.away_team_id} | {self.date:%Y-%m-%d}>"


class Odd(Base):
    """Quota per un mercato specifico in un determinato momento."""

    __tablename__ = "odds"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    match_id: Mapped[int] = mapped_column(ForeignKey("matches.id"))
    bookmaker: Mapped[str] = mapped_column(String(50))
    # Mercati: "1x2", "over_2.5", "under_2.5", "btts_yes", "btts_no",
    #           "corners_over_9.5", "multigol_2_3", ...
    market: Mapped[str] = mapped_column(String(60))
    # Selezione: "home", "draw", "away", "over", "under", "yes", "no"
    selection: Mapped[str] = mapped_column(String(50))
    odd: Mapped[float] = mapped_column(Float)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    match: Mapped[Match] = relationship("Match", back_populates="odds")

    __table_args__ = (Index("ix_odds_match_market", "match_id", "market"),)


class LiveSnapshot(Base):
    """Snapshot periodico dei dati live (ogni ~60s durante la partita)."""

    __tablename__ = "live_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    match_id: Mapped[int] = mapped_column(ForeignKey("matches.id"))
    minute: Mapped[int] = mapped_column(Integer)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    home_score: Mapped[int] = mapped_column(Integer, default=0)
    away_score: Mapped[int] = mapped_column(Integer, default=0)
    home_xg: Mapped[float | None] = mapped_column(Float)
    away_xg: Mapped[float | None] = mapped_column(Float)
    home_shots: Mapped[int | None] = mapped_column(Integer)
    away_shots: Mapped[int | None] = mapped_column(Integer)
    home_corners: Mapped[int | None] = mapped_column(Integer)
    away_corners: Mapped[int | None] = mapped_column(Integer)
    home_possession: Mapped[float | None] = mapped_column(Float)
    away_possession: Mapped[float | None] = mapped_column(Float)

    match: Mapped[Match] = relationship("Match", back_populates="live_snapshots")

    __table_args__ = (
        Index("ix_live_snapshots_match_minute", "match_id", "minute"),
    )
