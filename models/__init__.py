"""
Importa tutti i modelli in ordine per garantire la corretta registrazione
con SQLAlchemy (necessario per create_all e per risolvere le relazioni).
"""

from models.base import Base
from models.lineups import Lineup, LineupPlayer
from models.matches import LiveSnapshot, Match, MatchStatus, Odd
from models.picks import Pick
from models.players import Player, PlayerMatchStat
from models.teams import Team

__all__ = [
    "Base",
    "Team",
    "Match",
    "MatchStatus",
    "Odd",
    "LiveSnapshot",
    "Player",
    "PlayerMatchStat",
    "Pick",
    "Lineup",
    "LineupPlayer",
]
