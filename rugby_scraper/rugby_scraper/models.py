# -*- coding: utf-8 -*-

from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Enum, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

def create_tables(engine):
    """"""
    return Base.metadata.create_all(engine)

class Team(Base):
    __tablename__ = "teams"

    id = Column(Integer, primary_key = True)
    name = Column(String(80), nullable = False)

class Match(Base):
    __tablename__ = "matchs"

    id = Column(Integer, primary_key = True)
    home_team_id = Column(Integer, ForeignKey("teams.id"), nullable = False)
    away_team_id = Column(Integer, ForeignKey("teams.id"), nullable = False)
    ground_id = Column(Integer, nullable = False)
    won = Column(Boolean, nullable = False)
    date = Column(DateTime, nullable = False)
    home_team = relationship(Team, foreign_keys=home_team_id)
    away_team = relationship(Team, foreign_keys=away_team_id)

class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key = True)
    name = Column(String(80), nullable = False)
    full_name = Column(String(160), nullable = True)
    birthday = Column(DateTime, nullable = True)
    height = Column(String(40), nullable = True)
    weight = Column(String(40), nullable = True)

class MatchStats(Base):
    __tablename__ = "matchstats"

    id = Column(Integer, primary_key = True)
    match_id = Column(Integer, ForeignKey("matchs.id"), nullable = False)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable = False)
    scored = Column(Integer, nullable=False)
    conceded = Column(Integer, nullable=False)
    tries = Column(Integer, nullable=True)
    conversions = Column(Integer, nullable=True)
    penalties = Column(Integer, nullable=True)
    drops = Column(Integer, nullable=True)
    match = relationship(Match)
    team = relationship(Team)

class PlayerStats(Base):
    __tablename__ = "playerstats"

    id = Column(Integer, primary_key = True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable = False)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable = False)
    match_id = Column(Integer, ForeignKey("matchs.id"), nullable = False)
    position = Column(String(20), nullable = True)
    number = Column(Integer, nullable = True)
    first_team = Column(Boolean, nullable = False)
    tries = Column(Integer, nullable = True)
    cons = Column(Integer, nullable = True)
    pens = Column(Integer, nullable = True)
    drops = Column(Integer, nullable = True)
    player = relationship(Player)
    match = relationship(Match)
    team = relationship(Team)

class GameEvent(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key = True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable = False)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable = False)
    match_id = Column(Integer, ForeignKey("matchs.id"), nullable = False)
    time = Column(Integer, nullable = False)
    action_type = Column(Enum("tries", "cons", "pens", "drops"), nullable = False)
    extra_info = Column(Text, nullable = True)
    player = relationship(Player)
    match = relationship(Match)
    team = relationship(Team)
