# /plugins/testes/client.py
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

class DBConnector:

    @staticmethod
    def execute(cls, query, params=None):
        """
        This method is just for UPDATE/DELETE/REPLACE operations
        Doesnt return anything
        """
        hook = PostgresHook(postgres_conn_id='postgres_testes')
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(query,params)
                conn.commit()
        finally:
            cls.get_pool().putconn(conn) 

    @staticmethod
    def query(cls,query,params=None):
        """
        For SELECT operations, returns results
        """
        hook = PostgresHook(postgres_conn_id='postgres_testes')
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(query,params)
                return cur.fetchall()   # <- Returns data
        finally:
            cls.get_pool().putconn(conn)

    @staticmethod
    def execute_returning(cls, query, params=None):
        """
        For INSERT operations with RETURNING clause
        Returns the result (e.g., newly created row)
        """
        hook = PostgresHook(postgres_conn_id='postgres_testes')
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                conn.commit()
                return result
        finally:
            cls.get_pool().putconn(conn)