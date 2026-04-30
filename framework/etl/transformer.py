from pyspark.sql.functions import upper, col

class Transformer:
    """Reusable transformations."""

    @staticmethod
    def standardize_columns(df):
        """
        Convert string columns to uppercase.

        Parameters
        ----------
        df : pyspark.sql.DataFrame

        Returns
        -------
        pyspark.sql.DataFrame
        """
        for field in df.schema.fields:
            if field.dataType.simpleString() == "string":
                df = df.withColumn(
                    field.name,
                    upper(col(field.name))
                )
        return df