import warnings
from dataclasses import dataclass, fields
from decimal import Decimal
from datetime import datetime
from enum import Enum
from os.path import basename
from typing import BinaryIO, Mapping

from database import Database, Field


class Packet:
    class CacheHasOldValue(Enum):
        NO_PREV=0
        PREV_SAME=1
        PREV_DIFF=2
    def cache_has_old_value(self, db):
        """

        :param db:
        :return: 0 if no previous record
                 1 if
        """
        sql=f"select {','.join(self.cache_fields)} from {self.get_table_name()} where {self.cache_index}=%s order by id desc limit 1"
        db.execute(sql,(getattr(self,self.cache_index),))
        row=db._cur.fetchone()
        if row is None:
            return self.CacheHasOldValue.NO_PREV
        for field,old_value in zip(self.cache_fields,row):
            new_value=getattr(self,field)
            if isinstance(new_value,Enum):
                new_value=new_value.name
            if old_value!=new_value:
                return self.CacheHasOldValue.PREV_DIFF
        return self.CacheHasOldValue.PREV_SAME
    def write(self,db,*,fileid:int,ofs:int,epochid:int=None)->None:
        table_name = self.get_table_name()
        parent_fields=self.compiled_form.hq+self.compiled_form.fq
        values=[getattr(self,field_name) for field_name in parent_fields]+[fileid,ofs]
        parent_fields+=["file","ofs"]
        if self.has_cache:
            changed=self.cache_has_old_value(db)
            if changed==self.CacheHasOldValue.PREV_SAME:
                return
            else:
                values.append(changed==self.CacheHasOldValue.PREV_DIFF)
            parent_fields+=["changed"]
        if self.use_epoch:
            if epochid is None:
                raise ValueError("No epoch id for a packet that needs it")
            parent_fields+=["epoch"]
            values.append(epochid)
        parent=db.insert_get_id(table_name,parent_fields,values)
        if self.compiled_form.bf is not None and len(self.compiled_form.bq)>0:
            columns=tuple([getattr(self,field_name) for field_name in self.compiled_form.bq])
            block_field_names=["parent",]+self.compiled_form.bq
            for values in zip(*columns):
                db.insert(table_name+"_block",block_field_names,(parent,)+values)

    def get_table_name(self):
        table_name = getattr(self, 'table_name', self.__class__.__name__[4:].lower())
        return table_name


def read_packet(inf:BinaryIO)->Packet:
    """
    Read a packet. This is a factory function, which reads
    the next byte from the file, figures out what kind of packet
    it is from that, and calls the correct constructor for that.

    :param inf: binary stream open for reading
    :return: object describing the next packet
    :raises: The constructors must raise an exception if the packet
             cannot be parsed. The input stream will still be open and
             valid, but advanced the minimum number of bytes possible.
             The caller can try again, and won't get caught in an infinte
             loop because this reader always advances the stream at least
             one byte.

    To register a packet type, add an entry to the read_packet.classes
    dictionary. The key is the first byte of the packet, and the value
    is a callable that takes a byte array and binary input stream and
    builds a packet from that. The callable must advance the input
    stream to the first byte after the current packet. The callable must
    return an object representing the class, or raise an exception if
    it can't.
    """
    while True:
        try:
            header=inf.read(1)
            if len(header)<1:
                return
            packet=read_packet.classes[header[0]](header,inf)
            if packet!=None:
                yield packet
            else:
                print(f"Null packet at {basename(inf.name)} 0x{inf.tell():08x}")
        except EOFError:
            return
        except Exception as e:
            print(f"{basename(inf.name)} 0x{inf.tell():08x}")
            import traceback
            traceback.print_exc()
            warnings.warn("Skipping bad packet")
read_packet.classes={}


def read_null_packet(header: bytes, inf: BinaryIO):
    """
    No known packet format starts with a null byte. If we see a null byte,
    read and discard all of them until we get a non-null byte.
    :param header:
    :param inf:
    :return:
    """
    result = header
    while result[-1] != 0x0:
        result += inf.read(1)
    return None
read_packet.classes[0]=read_null_packet



def ensure_timeseries_tables(db:Database,drop=False):
    """
    Ensure the presence of all tables needed for time series packet
    storage. Currently this includes:

    * epoch - all packets with the same epoch are effective at exactly
              the time described in this epoch
    * files - name and metadata of each file that is processed. Each packet
              record has a file id (primary key of this table) and offset
              indicating exactly where it came from.

    :param conn:
    :return:
    """
    db.make_table(table_name="files",
                  fields=(Field(name="basename",python_type=str,nullable=False),
                          Field(name="process_start_time",python_type=datetime,nullable=False,default="NOW()"),
                          Field(name="process_finish_time",python_type=datetime)),
                  unique_tuples=[("basename",)],drop=drop)
    db.make_table(table_name="epoch",
                  fields=(Field(name="utc",python_type=datetime,nullable=False),
                          Field(name="week",python_type=int,nullable=False),
                          Field(name="iTOW",python_type=Decimal,dec_precision=10,dec_scale=3,nullable=False)),
                  unique_tuples=[("week","iTOW")],drop=drop)


def register_file_start(db:Database,fn:str):
    id=db.select_id("files",("basename",),(basename(fn),))
    if id is not None:
        sql="update files set process_start_time=NOW(),process_finish_time=NULL where id=%s"
        db.execute(sql,(id,))
    else:
        id=db.insert_get_id("files",("basename",),(basename(fn),))
    return id



def register_file_finish(db:Database,fid:int):
    sql="UPDATE files SET process_finish_time=NOW() WHERE id=%s;"
    db.execute(sql,(fid,))


def register_epoch(db:Database,*,utc:datetime,week:int,iTOW:Decimal)->int:
    register_epoch.now=datetime.now()
    if hasattr(register_epoch,'then'):
        #print(register_epoch.now-register_epoch.then)
        pass
    else:
        register_epoch.timehist=[]
        register_epoch.first=register_epoch.now
    register_epoch.timehist.append(register_epoch.now)
    register_epoch.then=register_epoch.now
    # There is a race condition between here and the insert_get_id() below. If another
    # process tries to insert the same epoch, then the insert_get_id() below will fail.
    try:
        with db.transaction():
            id=db.select_id('epoch',("week","iTOW"),(week,iTOW))
            pre_exist=True
            if id is None:
                id=db.insert_get_id('epoch',("week","iTOW","utc"),(week,iTOW,utc))
                pre_exist=False
    except UniqueViolation:
        # We lost the race, so just get the id that the other process created
        id=db.select_id('epoch',("week","iTOW"),(week,iTOW))
        pre_exist=True
    return id,pre_exist


def make_comment(metadata: Mapping):
    if 'comment' in metadata:
        comment = metadata["comment"]
        if 'unit' in metadata:
            comment += f" [{metadata['unit']}]"
    else:
        if 'unit' in metadata:
            comment = f" [{metadata['unit']}]"


def ensure_table(db:Database,pktcls:dataclass,drop:bool,table_name:str=None)->None:
    """
    If necessary, create a table in the current database representing this packet.
    If a table of the given name already exists, don't do anything. Note that this
    means that if the table exists but has a different structure than specified by
    this class, the table won't be regenerated or modified. If you change the
    structure of a packet, either manually modify the table to match or delete the
    table and start over.

    :param db:
    :param pktcls:
    :param drop: If true, drop any existing table with the same name
    :return: None, but side effect is that table is guaranteed to exist, or exception
             is thrown if we can't satisfy that guarantee.
    """
    table_fields=[]
    block_fields=None
    table_comment=pktcls.__doc__
    if table_name is None:
        table_name=pktcls.__name__[4:].lower()
    unique_tuples=[]
    indexes=[]
    if getattr(pktcls,'use_epoch',False):
        table_fields.append(Field(name="epoch",python_type=int,comment="Foreign key to epoch table, holding exact UTC "
                                                                       "time. Across all tables, all rows with the "
                                                                       "same epoch id represent data describing the "
                                                                       "exact same instant in time."))
        indexes.append("epoch")
    for field in fields(pktcls):
        if not field.metadata.get("record",True):
            continue
        if str(field.type)[0:4]=='list':
            if block_fields is None:
                block_fields=[Field(name="parent",python_type=int,nullable=False).update_metadata(field.metadata)]
            block_fields.append(Field(name=field.name,python_type=field.type.__args__[0]).update_metadata(field.metadata))
        else:
            if field.metadata.get("unique",False):
                unique_tuples.append((field.name,))
            if field.metadata.get("index",False):
                indexes.append(field.name)
            table_fields.append(Field(name=field.name,python_type=field.type).update_metadata(field.metadata))
    if pktcls.has_cache:
        table_fields.append(Field(name="changed",python_type=bool,
                                  comment=f"If False, this is the first time this {pktcls.cache_index} has been seen. "
                                           "If True, this object has been seen before and one of "
                                          "the significant fields has changed."))
    table_fields.append(Field(name="file",python_type=int,nullable=False,comment="Foreign key to file table, holding "
                                                                                 "information about the file that this "
                                                                                 "packet is extracted from."))
    table_fields.append(Field(name="ofs",python_type=int,nullable=False,comment="Zero-based offset from beginning of "
                                                                                "file of byte 0 of this packet. If the "
                                                                                "packet is compressed (EG .ubx.bz2), "
                                                                                "this is the offset in the "
                                                                                "decompressed stream."))
    db.make_table(table_name=table_name,fields=table_fields,table_comment=table_comment,unique_tuples=unique_tuples,indexes=indexes,drop=drop)
    if block_fields is not None:
        db.make_table(table_name=table_name+"_block", fields=block_fields, indexes=["parent"],drop=drop)
