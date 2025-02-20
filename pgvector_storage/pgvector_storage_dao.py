import json
from typing import List, Iterator

import openai
import psycopg
from py_common_utility.utils import env_utils, comm_utils

from pgvector_storage import sql_syntax
from pgvector_storage.constant import EMBEDDING_MODEL, EMBEDDING_DIM
from pgvector_storage.document import Document, DocEntity


class PgvectorStorageDao:
    connect_string: str
    table_name: str

    def __init__(self, **kwargs):
        for fn, ft in self.__annotations__.items():
            setattr(self, fn, kwargs.get(fn, None))

    def create_table(self):
        create_table_query = sql_syntax.gen_create_table(self.table_name)
        with psycopg.connect(self.connect_string) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                conn.commit()
        print(f"Table '{self.table_name}' created (if not exists).")

    def save_documents(self, doc_iterator: Iterator[Document], batch_size=128):
        insert_query = sql_syntax.gen_insert_all_template(self.table_name)

        batch: List[DocEntity] = []

        with psycopg.connect(self.connect_string) as conn:
            with conn.cursor() as cur:
                for doc in doc_iterator:
                    metadata_json = json.dumps(doc.metadata) if doc.metadata else None
                    embedding = self.generate_embedding(doc.content)  # Implement this method
                    entity = DocEntity(uid=doc.uid, metadata_json=metadata_json, embedding=embedding,
                                       content=doc.content)
                    batch.append(entity)
                    if len(batch) >= batch_size:
                        self.insert_all(cur, batch)
                        conn.commit()
                        batch.clear()

                if batch:  # Insert any remaining records
                    self.insert_all(cur, batch)
                    conn.commit()
        print("Documents stored successfully!")

    def insert_all(self, cur: any, doc_list: List[DocEntity]):
        insert_query = sql_syntax.gen_insert_all_template(self.table_name)
        batch = []
        for doc in doc_list:
            batch.append((doc.uid, doc.content, doc.metadata_json, doc.embedding))
        cur.executemany(insert_query, batch)

    def generate_embedding(self, text: str) -> List[float]:
        """Generates an embedding for a given text (Dummy Implementation)."""
        response = openai.embeddings.create(
            input=text,
            model=EMBEDDING_MODEL
        )
        return response.data[0].embedding  # New response format


if __name__ == '__main__':
    import main
    from pathlib import Path

    documents = [
        Document(uid=comm_utils.random_chars(12), content="Elephants are the largest land animals.",
                 metadata={"source": "wildlife"}),
        Document(uid=comm_utils.random_chars(12), content="Climate change affects global temperatures.",
                 metadata={"source": "science"}),
        Document(uid=comm_utils.random_chars(12), content="bazar is a 150kg, 230cm tall polar bear",
                 metadata={"source": "books"}),
    ]

    base_dir = Path(main.__file__).parent
    env_utils.load_env(env_dir_path=str(base_dir))
    c_str = env_utils.env('PGVECTOR_TEST_CONNECTION')
    dao = PgvectorStorageDao(connect_string=c_str, table_name='test_kirin')
    dao.create_table()
    dao.save_documents(iter(documents), 2)
    print('Created table done and set data')
