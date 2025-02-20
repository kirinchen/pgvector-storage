from pgvector_storage import constant


def gen_create_table(table_name: str) -> str:
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS public.{table_name} (
        id VARCHAR(128) PRIMARY KEY, 
        text TEXT NOT NULL,
        metadata JSONB,
        embedding VECTOR({constant.EMBEDDING_DIM})  -- Embedding dimension
    );
    """
    return create_table_query


def gen_insert_all_template(table_name: str) -> str:
    insert_query = f"""
    INSERT INTO public.{table_name} (id, text, metadata, embedding)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE 
    SET text = EXCLUDED.text, 
        metadata = EXCLUDED.metadata,
        embedding = EXCLUDED.embedding;
    """
    return insert_query
