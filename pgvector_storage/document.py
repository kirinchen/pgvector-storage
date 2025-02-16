from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class Document:
    id: str
    content: str
    metadata: Optional[Dict] = None
