"""

"""
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from math import isnan, isfinite
from typing import Iterable

import mysql.connector

from database import Database, Field


class MysqlDatabase(Database):
    class Transaction:
        def __init__(self, database: Database):
            self.database=database
        def __enter__(self):
            self.database._conn.start_transaction()
            return self
        def __exit__(self, exc_type,exc_value,traceback):
            if exc_type is None:
                self.database._conn.commit()
            else:
                self.database._conn.rollback()
    def connect(self):
        self.profile={}
        self._conn=mysql.connector.connect(database=self.database,user=self.user,password=self.password,autocommit=True,time_zone='+00:00')
        self._cur=self._conn.cursor()
    def set_utc(self):
        pass
    def make_table_header(self,table_name, table_comment=None):
        return [],f"create table {table_name} (\n  id serial primary key,\n",[]
    def make_table_footer(self,table_name, table_comment=None):
        return [],f") ENGINE=MYISAM;",[]
    def sql_field_type(self,table_name:str,field:Field, drop:bool=True):
        if field.python_type == bytes:
            return "VARBINARY(256)"
        else:
            return super().sql_field_type(table_name,field,drop)
    def filter_enums(self,values):
        result=[]
        for x in values:
            if isinstance(x,Enum):
                result.append(x.name)
            elif isinstance(x,Decimal) and not isfinite(x):
                result.append(None)
            elif type(x)==float and not isfinite(x):
                result.append(None)
            else:
                result.append(x)
        return tuple(result)
    def make_enum(self,table_name,field_type,drop:bool=True):
        psql_type="ENUM('"+"','".join([member_name for member_name,member in field_type.__members__.items()])+"')"
        return [],psql_type,[]
    def execute(self,*args,**kwargs):
        if args[0] not in self.profile:
            self.profile[args[0]]=[0,timedelta(), []]
        t0=datetime.now()
        self._cur.execute(*args,**kwargs)
        t1=datetime.now()
        dt=t1-t0
        self.profile[args[0]][0]+=1
        self.profile[args[0]][1]+=dt
        self.profile[args[0]][2].append(dt)
    def insert_get_id(self,table_name,field_names,values)->int:
        self.insert(table_name,field_names,values)
        self.execute("SELECT LAST_INSERT_ID();")
        id = self._cur.fetchone()[0]
        return id
