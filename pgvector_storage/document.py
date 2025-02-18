class Document:

    def __init__(self, uid: str, content: str, metadata: dict = None):
        self.uid = uid
        self.content = content
        self.metadata = metadata if metadata else {}
