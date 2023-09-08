from decimal import Decimal
from datetime import datetime
from os.path import basename
from typing import BinaryIO, Mapping

from database import Database, Field


def read_packet(inf:BinaryIO)->'Packet':
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
            yield packet
        except EOFError:
            return
        except Exception as e:
            import warnings
            warnings.warn(str(e))
read_packet.classes={}


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
        sql="update files set process_start_time=NOW() where id=%s"
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
    id=db.select_id('epoch',("week","iTOW"),(week,iTOW))
    pre_exist=True
    if id is None:
        id=db.insert_get_id('epoch',("week","iTOW","utc"),(week,iTOW,utc))
        pre_exist=False
    return id,pre_exist


def make_comment(metadata: Mapping):
    if 'comment' in metadata:
        comment = metadata["comment"]
        if 'unit' in metadata:
            comment += f" [{metadata['unit']}]"
    else:
        if 'unit' in metadata:
            comment = f" [{metadata['unit']}]"





