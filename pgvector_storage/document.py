from typing import List


class Document:
    uid: str
    content: str
    metadata: dict

    def __init__(self, uid: str, content: str, metadata: dict = None):
        self.uid = uid
        self.content = content
        self.metadata = metadata if metadata else {}


class DocEntity:
    uid: str
    content: str
    metadata_json: str
    embedding: List[float]

    def __init__(self, uid: str, content: str, metadata_json: str, embedding: List[float]):
        self.uid = uid
        self.content = content
        self.metadata_json = metadata_json
        self.embedding = embedding
