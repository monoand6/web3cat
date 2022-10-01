from fetcher.db import DB


class Repo:
    _db: DB

    def __init__(self, db: DB):
        self._db = db

    def commit(self):
        self._db.commit()

    def rollback(self):
        self._db.rollback()
