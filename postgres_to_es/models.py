from dataclasses import dataclass, field
from typing import Set, Optional
from datetime import datetime
import uuid


@dataclass(frozen=True)
class NamedItem:
    id: uuid.UUID
    name: str


@dataclass(frozen=True)
class FilmWork:
    id: uuid.UUID
    title: Optional[str]
    description: Optional[str]
    type: Optional[str]
    rating: Optional[float]
    updated_at: datetime
    genres: Set[NamedItem] = field(default_factory=set)
    actors: Set[NamedItem] = field(default_factory=set)
    writers: Set[NamedItem] = field(default_factory=set)
    directors: Set[NamedItem] = field(default_factory=set)
