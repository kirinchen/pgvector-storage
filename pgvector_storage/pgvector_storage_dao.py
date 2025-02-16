import json
from typing import List, Iterator

import openai
import psycopg
from py_common_utility.utils import env_utils, comm_utils

from pgvector_storage.document import Document

EMBEDDING_MODEL = "text-embedding-ada-002"
EMBEDDING_DIM = 1536  # Change if using a different model


class PgvectorStorageDao:
    connect_string: str
    table_name: str

    def __init__(self, **kwargs):
        for fn, ft in self.__annotations__.items():
            setattr(self, fn, kwargs.get(fn, None))

    def create_table(self):
        """Creates a table for storing documents and embeddings in pgvector."""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS public.{self.table_name} (
            id VARCHAR(128) PRIMARY KEY, 
            text TEXT NOT NULL,
            metadata JSONB,
            embedding VECTOR({EMBEDDING_DIM})  -- Embedding dimension
        );
        """
        # Async PostgreSQL connection
        with psycopg.connect(self.connect_string) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                conn.commit()
        print(f"Table '{self.table_name}' created (if not exists).")

    def save_documents(self, doc_iterator: Iterator[Document]):
        """Stores documents with embeddings into PostgreSQL."""
        insert_query = f"""
        INSERT INTO public.{self.table_name} (id, text, metadata, embedding)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE 
        SET text = EXCLUDED.text, 
            metadata = EXCLUDED.metadata,
            embedding = EXCLUDED.embedding;
        """

        with psycopg.connect(self.connect_string) as conn:
            with conn.cursor() as cur:
                for doc in doc_iterator:
                    metadata_json = json.dumps(doc.metadata) if doc.metadata else None
                    embedding = self.generate_embedding(doc.content)  # Implement this method
                    cur.execute(insert_query, (doc.id, doc.content, metadata_json, embedding))
            conn.commit()
        print("Documents stored successfully!")

    def generate_embedding(self, text: str) -> List[float]:
        """Generates an embedding for a given text (Dummy Implementation)."""
        response = openai.embeddings.create(
            input=text,
            model=EMBEDDING_MODEL
        )
        return response.data[0].embedding  # New response format


if __name__ == '__main__':
    import main
    import asyncio
    from pathlib import Path

    documents = [
        Document(id=comm_utils.random_chars(12), content="Elephants are the largest land animals.",
                 metadata={"source": "wildlife"}),
        Document(id=comm_utils.random_chars(12), content="Climate change affects global temperatures.",
                 metadata={"source": "science"}),
        Document(id=comm_utils.random_chars(12), content="bazar is a 150kg, 230cm tall polar bear",
                 metadata={"source": "books"})
    ]


    async def m_task():
        base_dir = Path(main.__file__).parent
        env_utils.load_env(env_dir_path=str(base_dir))
        c_str = env_utils.env('PGVECTOR_TEST_CONNECTION')
        dao = PgvectorStorageDao(connect_string=c_str, table_name='test_kirin')
        dao.create_table()
        dao.save_documents(iter(documents))
        print('Created table done and set data')


    asyncio.run(m_task())
