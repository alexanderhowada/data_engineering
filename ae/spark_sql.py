from pyspark.sql import SparkSession


class AutoGroup:
    """Generate select query, group by and sum."""

    def __init__(self, spark: SparkSession, source_tb: str, target_tb: str):
        self.spark = spark
        self.source_tb = source_tb
        self.target_tb = target_tb

    def get_column_types(self, tb: str) -> list[tuple[str, str]]:
        """Get column names and types.

        Args:
            tb: target delta table.
        Returns:
            Example:
            [
                ("col1", "string"),
                ("col2", "double"),
                ("col3", "bigint"),
            ]
        """
        df = self.spark.read.format("delta").table(tb).limit(0)
        return df.dtypes

    def dict_select_group_view(self) -> dict[str, list[str]]:
        """Generates a dicionary with the SQL statement.

        Returns:
            Example
            {
                "SELECT": ["`col1`", "`col2`", "SUM(`col3`) AS `col3`"],
                "FROM": ["`source_tb`"],
                "GROUP BY": ["`col1`", "`col2`"]
            }
        """

        query_d = {
            "SELECT": [],
            "FROM": [self.source_tb],
            "GROUP BY": []
        }

        sum_l = []
        for c, t in self.get_column_types(self.source_tb):
            if ("double" not in t and "int" not in t
                    and "float" not in t):
                query_d['SELECT'].append(f"`{c}`")
                query_d['GROUP BY'].append(f"`{c}`")
            else:
                sum_l.append(f"SUM(`{c}`) AS `{c}`")
        query_d['SELECT'] += sum_l

        return query_d

    def sql_create_or_replace_view(self) -> str:
        """Generates the SQL query.

        Returns:
            Example
            CREATE OR REPLACE VIEW `target_tb` AS
            SELECT
                `col1`
                , `col2`
                , SUM(`col3`) AS `col3`
            FROM source_tb
            GROUP BY
                `col1`
                , `col2`
        """
        q_dict = self.dict_select_group_view()

        q = f"CREATE OR REPLACE VIEW {self.target_tb} AS\n"

        for ddl, columns in q_dict.items():
            q += ddl + "\n"
            q += "    " + "\n    , ".join(columns) + "\n"
        return q

    def create_or_replace_view(self, display=False):
        q = self.sql_create_or_replace_view()
        self.spark.sql(q)

        if display:
            print(q)

        return q