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
                        self.save_all(cur, batch)
                        conn.commit()
                        batch.clear()

                if batch:  # Insert any remaining records
                    self.save_all(cur, batch)
                    conn.commit()
        print("Documents stored successfully!")

    def is_exist(self, cur: any, e: DocEntity) -> bool:
        check_query = sql_syntax.gen_exist_by_id(self.table_name)
        cur.execute(check_query, (e.uid,))
        existing_id = cur.fetchone()
        return existing_id

    def save_all(self, cur: any, doc_list: List[DocEntity]):
        insert_list = []
        update_list = []
        for doc in doc_list:
            if self.is_exist(cur, doc):
                update_list.append(doc)
            else:
                insert_list.append(doc)
        self.insert_all(cur, insert_list)
        self.update_all(cur, update_list)

    def update_all(self, cur: any, doc_list: List[DocEntity]):
        if not doc_list:
            return
        update_query = sql_syntax.gen_update(self.table_name)
        batch = []
        for doc in doc_list:
            batch.append((doc.content, doc.metadata_json, doc.embedding, doc.uid))
        cur.executemany(update_query, batch)

    def insert_all(self, cur: any, doc_list: List[DocEntity]):
        if not doc_list:
            return
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
