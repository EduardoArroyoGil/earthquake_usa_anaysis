import sqlalchemy as alch
from sqlalchemy.exc import SQLAlchemyError


class Load:

    def __init__(self, db_name, password_db):

        self.db_name = db_name
        self.password_db = password_db

    def server_connection(self):
        connection = f"mysql+pymysql://root:{self.password_db}@localhost"
        return alch.create_engine(connection)

    def create_db(self):
        engine = self.server_connection()
        try:
            engine.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name};")

        except:
            print("DB already exists")

    def db_connection(self):

        connection2 = f"mysql+pymysql://root:{self.password_db}@localhost/{self.db_name}"
        return alch.create_engine(connection2)

    def create_insert_table(self, query):
        engine = self.db_connection()

        try:
            engine.execute(query)

        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            print('error: ', error)
            return error

    def get_id(self, link, col_id, column, table):

        engine = self.db_connection()

        try:
            query_get_id = f"SELECT {col_id} FROM {table} WHERE {column} = '{link}'"

            id_ = engine.execute(query_get_id).first()

            if not id_:
                return "id is not in DB"
            else:
                return engine.execute(query_get_id).first()[0]

        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            return error
