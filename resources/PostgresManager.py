# resources/postgres.py
from dagster import resource,EnvVar,ConfigurableResource
import psycopg2
from psycopg2.extensions import connection


class PostgresResource(ConfigurableResource):
    """
    Creates a reusable postgres connection resource

    """
    def get_connection(self)-> connection:
        """
        
        Returns a Postgres connection
        
        """
        return psycopg2.connect(
            host=EnvVar("DB_HOST").get_value(),
            database=EnvVar("DB_NAME").get_value(),
            user=EnvVar("DB_USER").get_value(),
            password=EnvVar("DB_PASSWORD").get_value()
        )
