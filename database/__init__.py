"""


"""
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from collections import namedtuple
from enum import Enum, EnumType
from itertools import chain
from typing import Iterable, Mapping



@dataclass
class Field:
    name:str
    python_type:type
    nullable:bool=True
    default:str=None
    comment:str=None
    dec_scale:int=None
    dec_precision:int=None
    def update_metadata(self, metadata:Mapping):
        from packet import make_comment
        self.comment = make_comment(metadata)
        if 'dec_scale' in metadata:
            self.dec_scale=metadata['dec_scale']
            self.dec_precision=metadata['dec_precision']
        elif 'scale' in metadata and type(metadata['scale'])== Decimal:
            self.dec_precision = {1: 3, 2: 5, 4: 10, 8: 19}[int(metadata['type'][1:])]
            self.dec_scale = -metadata['scale'].as_tuple().exponent
            if self.dec_scale > self.dec_precision:
                self.dec_precision = self.dec_scale
        return self




class Database:
    class Transaction:
        def __init__(self, database: 'Database'):
            raise NotImplemented
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_value, traceback):
            pass
    def transaction(self):
        """
        Return a context manager that handles transactions.

        :return: An object that is usable as a context manager. It should start a transaction
                 on entering the context, roll back the transaction on exit with an error,
                 and commit it on exit with no error. May be a subclass of Transaction or a class
                 provided by the underlying engine (psycopg3)
        """
        return self.Transaction(self)
    def __init__(self,database,user,password,**kwargs):
        self.database=database
        self.user = user
        self.password = password
        self.kwargs=kwargs
    def __enter__(self):
        self._conn=self.connect()
        self._cur=self._conn.cursor()
        return self
    def __exit__(self,exc_type,exc_value,traceback):
        self._cur.close()
        self._conn.close()
    def connect(self):
        """
        This should:
        * Open a connection to the server and return it
        :return: A database connection
        """
        raise NotImplemented
    def make_enum(self,table_name,field_type,drop:bool=False):
        raise NotImplemented
    def insert_get_id(self,table_name,field_names,values):
        raise NotImplemented
    def insert(self,table_name,field_names,values)->None:
        sql = f'INSERT INTO {table_name} (' + ','.join(field_names) + ') VALUES (' + ','.join(
            ["%s"] * len(values)) + ");"
        self.execute(sql, self.filter_enums(values))
    def execute(self,*args,**kwargs):
        return self._cur.execute(*args,**kwargs)
    def filter_enums(self,values):
        """
        Do whatever is necessary to reformat data for the connection. A good (like psycopg3) connection
        won't need anything. The hall of shame so far includes MySQL which rejects non-finite float and
        Decimal numbers, and doesn't handle Enums, wanting the string name instead.

        :param values: Tuple of field values which is about to be inserted
        :return: tuple of filtered field values. may be values if no changes are required.
        """
        return values
    def select_id(self,table,field_names,values):
        self.execute(f'SELECT id FROM {table} WHERE {" AND ".join([x+"=%s" for x in field_names])};', values)
        row=self._cur.fetchone()
        if row is not None:
            return row[0]
        return None
    def sql_field_type(self,table_name:str,field:Field, drop:bool=True):
        if field.python_type == Decimal:
            # Figure out the precision and scale from the 'scale' and 'type' of the metadata
            return f"NUMERIC({field.dec_precision},{field.dec_scale})"
        elif field.python_type == str:
            return "VARCHAR(256)"
        elif field.python_type == bool:
            return "boolean"
        elif field.python_type == datetime:
            return "timestamp(6)"
        elif isinstance(field.python_type, EnumType):
            raise RuntimeError("This should be handled in make_sql_field")
        elif field.python_type == float:
            return "float8"
        elif field.python_type == bytes:
            raise ValueError("Byte array type varies on a per-engine database. Update the driver to specify this.")
        else:
            # Integers. There is no such thing as "unsigned" in Postgres, so we do the Java trick and go to the next type up
            return 'bigint'
    def make_sql_field(self, table_name:str,field:Field, drop:bool=True)->tuple[Iterable[str], str, Iterable[str]]:
        """
        Generate the SQL for one field of a CREATE TABLE statement
        :param field: field object for this column
        :return: tuple of things necessary to create this class:
          0: SQL statements to run before the CREATE TABLE statement (possibly an empty list)
          1: SQL fragment to go inside a CREATE TABLE statement
          2: SQL statements to run after the CREATE TABLE statement (possibly empty)
        """
        pre_list=[]
        post_list=[]
        comment=None
        this_enum=None
        flags=''
        if not field.nullable:
            flags+=' NOT NULL'
        if field.default is not None:
            flags+=' DEFAULT '+field.default
        if isinstance(field.python_type, EnumType):
            pre_list,field_type,post_list=self.make_enum(table_name, field.python_type,drop=drop)
        else:
            field_type=self.sql_field_type(table_name,field,drop)
        sql_frag= f'{field.name} {field_type}'+flags
        return pre_list,sql_frag,post_list
    def make_table_header(self,table_name, table_comment=None):
        return [],f"create table if not exists {table_name} (\n  id serial primary key,\n",[]
    def make_table_footer(self,table_name, table_comment=None):
        return [],f");",[]
    def make_table(self,*,table_name:str,
                          fields:Iterable[Field],
                          unique_tuples:Iterable[tuple[str,...]]=None,
                          indexes:Iterable[str]=None,
                          table_comment: str = None,
                          drop:bool=False)->None:
        """

        :param table_name: Name of table to create
        :param fields: List of Field objects describing fields to create
        :param unique_tuples: List of tuples of field names which are required to be unique
        :param indexes: List of single fields that should be indexed
        :param table_comment: Comment for table
        :param drop: If True, delete the old table with this name first
        """
        pre_list,table_sql,post_list=self.make_table_header(table_name,table_comment)
        field_list=[]
        def list_append(this_pre,this_frag,this_post):
            nonlocal pre_list,post_list
            pre_list+=this_pre
            field_list.append(this_frag)
            post_list+=this_post
        if drop:
            pre_list.append(f"drop table if exists {table_name} cascade;")
        for field in fields:
            list_append(*self.make_sql_field(table_name,field,drop=drop))
        table_sql+="  "+",\n  ".join(field_list)+"\n"
        this_pre,footer,this_post=self.make_table_footer(table_name,table_comment)
        if unique_tuples is not None:
            for unique_tuple in unique_tuples:
                post_list.append(f"CREATE UNIQUE INDEX if not exists {'_'.join(unique_tuple)}_index ON {table_name} ({','.join(unique_tuple)});")
        pre_list+=this_pre
        table_sql+=footer
        post_list+=this_post
        for sql in chain(pre_list,(table_sql,),post_list):
            self.execute(sql)
