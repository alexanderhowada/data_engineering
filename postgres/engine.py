import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import CursorResult

class PostgresConnection:
    """Connect to postgres database.

    Example:
        import os
        from dotenv import load_dotenv
        from postgres.engine import PostgresConnection


        load_dotenv()

        POSTGRES_USER = os.environ["POSTGRES_USER"]
        POSTGRES_PASS = os.environ["POSTGRES_PASS"]
        POSTGRES_DB = os.environ["POSTGRES_DB"]
        POSTGRES_PORT = os.environ["POSTGRES_PORT"]
        POSTGRES_ENDPOINT = os.environ["POSTGRES_ENDPOINT"]
        TIMEOUT = 10

        conn = PostgresConnection(
            POSTGRES_USER, POSTGRES_PASS, POSTGRES_DB,
            POSTGRES_PORT, POSTGRES_ENDPOINT,
            connect_args={"connect_timeout": TIMEOUT}
        )
        conn.execute("INSERT INTO public.first_table(value, description) VALUES(1, 'random description')")
        df = conn.get_dataframe("SELECT * FROM public.first_table")
        display(df)
    """

    def __init__(self, user: str, password: str, db: str,
                 port: str, endpoint: str, **engine_kwargs):
        """
        Args:
            user: postgres username.
            password: postgres password.
            db: postgres database.
            port: postgres port.
            endpoint: postgres endpoint.
            engine_kwargs: sqlalchemy engine kwargs.
        """
        self.user = user
        self.password = password
        self.db = db
        self.port = port
        self.endpoint = endpoint
        self.engine_kwargs = engine_kwargs

        self._build_url()
        self._create_engine()

    def _build_url(self):
        self._url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.endpoint}:{self.port}/{self.db}"
        return self._url

    def _create_engine(self):
        self._engine = create_engine(self._url, **self.engine_kwargs)
        return self._engine

    def execute(self, q: str, commit=True) -> CursorResult:
        """Executes a raw query.

        Args:
            q: the postgres query to execute.
            commit: If true, commits the query after its execution. Defaults to True.

        Returns:
            Result of the query (sqlalchemy.engine.CursorResult).
        """
        with self._engine.connect() as conn:
            r = conn.execute(text(q))
            if commit:
                conn.commit()
        return r

    def get_dataframe(self, q: str, **kwargs) -> pd.DataFrame:
        """Executes a query and return a pandas dataframe with its results.

        Args:
            q: Postgres query.
            kwargs: kwargs for pandas.read_sql.

        Returns:
            Pandas DataFrame with the results of the query.
        """

        return pd.read_sql(q, self._engine, **kwargs)

