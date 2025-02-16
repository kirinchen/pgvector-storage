class PgvectorStorageDao:
    connect_string: str

    def __init__(self, **kwargs):
        for fn, ft in self.__annotations__.items():
            setattr(self, fn, kwargs.get(fn, None))
