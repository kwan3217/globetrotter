"""

"""
import psycopg

from database import Database, Field


def _postgres_escape_string(s):
    """
    Escape a string for postgresql, from https://stackoverflow.com/a/17655999
    :param s:
    :return:
    """
    escaped = repr(s)
    if escaped[:1] == '"':
        escaped = escaped.replace("'", "''")
    elif escaped[:1] != "'":
        raise AssertionError("unexpected repr: %s", escaped)
    return "'%s'" % (escaped[1:-1],)


class PostgresDatabase(Database):
    def connect(self):
        conn=psycopg.connect(conninfo=f"dbname={self.database} user={self.user} password={self.password}",autocommit=True,**self.kwargs)
        conn.execute("SET TIMEZONE TO 'UTC';")
        return conn
    def transaction(self):
        return self._conn.transaction()
    def make_enum(self,table_name,field_type,drop:bool=True):
        enum_name = f"{table_name}_{field_type.__name__}"
        pre_sql=[f'DROP TYPE IF EXISTS {enum_name};'] if drop else []
        # Only create the type if it doesn't already exist
        # https://stackoverflow.com/a/48382296
        this_enum = f'DO $$ BEGIN CREATE TYPE {enum_name} as ENUM (\n'
        for member_name, member in field_type.__members__.items():
            # psql_type="ENUM('"+"','".join([member_name for member_name,member in field_type.__members__.items()])+"')"
            this_enum += f"  '{member_name}',\n"
        this_enum = this_enum[:-2] + "\n); EXCEPTION WHEN duplicate_object THEN NULL; END $$;"
        pre_sql.append(this_enum)
        psql_type = f'{enum_name}'
        return pre_sql,psql_type,[]
    def sql_field_type(self,table_name:str,field:Field, drop:bool=True):
        if field.python_type == bytes:
            return "bytea"
        else:
            return super().sql_field_type(table_name,field,drop)
    def insert_get_id(self,table_name,field_names,values)->int:
        sql = f'INSERT INTO {table_name} (' + ','.join(field_names) + ') VALUES (' + ','.join(
            ["%s"] * len(values)) + ") RETURNING id;"
        self.execute(sql, self.filter_enums(values))
        id = self._cur.fetchone()[0]
        return id

