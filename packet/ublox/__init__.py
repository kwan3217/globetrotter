"""
Packet descriptions for UBLOX binary packets.

To use:

Import this first, then one protocol package, like this:

```
import protocol_33_21
```

Then
"""
from collections import namedtuple, defaultdict
from dataclasses import dataclass, field, fields
from datetime import datetime
from enum import Enum
from functools import partial
from struct import unpack
from typing import BinaryIO, Callable, Any
from decimal import Decimal
import re

from database import Database, Field
from packet.bin import get_bits
from packet import read_packet, make_comment


def fletcher8(buf:bytes):
    """
    Calculate the 8-bit Fletcher checksum according to the algorithm in
    section 3.4
    :param buf: Combined header and payload
    :return: two-byte buffer with ck_a as element 0 and ck_b as element 1.
             This can be directly compared with the checksum as-read.
    """
    ck_a=0
    ck_b=0
    for byte in buf:
        ck_a=(ck_a+byte) & 0xFF
        ck_b=(ck_b+ck_a) & 0xFF
    return bytes((ck_a,ck_b))


def read_ublox_packet(header:bytes,inf:BinaryIO):
    """
    Read a ublox packet. This is also a factory function, which reads
    the rest of the header, figures out which packet this is, then
    calls the __init__ for the correct dataclass

    :param header:
    :param inf:
    :return:
    """
    header1=inf.read(1)
    if len(header1)<1:
        raise EOFError
    header=header+header1
    #Check the second byte and punch out if it's wrong. This way we use up less
    #of the input stream and have more potential bytes to get back in sync.
    if header[1]!=0x62:
        raise ValueError(f"Bad ublox packet signature {header[0]:02x}{header[1]:02x}")
    header4=inf.read(4)
    if len(header4)<4:
        raise EOFError
    header+=header4
    cls = header[2]
    id = header[3]
    length = unpack('<H', header[4:6])[0]
    if length==0:
        payload=bytes()
    else:
        payload=inf.read(length)
        if len(payload)<length:
            raise EOFError
    calc_ck=fletcher8(header+payload)
    read_ck=inf.read(2)
    if len(read_ck)<2:
        raise EOFError
    #if read_ck!=calc_ck:
    #    raise ValueError(f"Checksum doesn't match: Calculated {calc_ck[0]:02x}{calc_ck[1]:02x}, read {read_ck[0]:02x}{read_ck[1]:02x}")
    if cls in read_ublox_packet.classes and id in read_ublox_packet.classes[cls]:
        return read_ublox_packet.classes[cls][id](cls,id,payload)
    else:
        return UBloxPacket(cls,id,payload)
read_ublox_packet.classes={}
read_packet.classes[0xb5]=read_ublox_packet


class UBloxPacket:
    """
    Subclasses should be dataclasses. Each field in the packet is represented by a
    field in the dataclass. The type of the field is the type of the *scaled* value.
    If the type is a list, then this is considered to be part of the repeating
    section of a packet.

    """
    def parse_payload(self,payload:bytes)->None:
        """
        Parse a ublox packet

        :param packet: bytes array containting payload of packet, not including header or checksum
        :return: None, but sets fields of self as appropriate
        """
        def scale_field(value,b1,b0,scale):
            if b0 is not None:
                value=get_bits(value,b1=b1,b0=b0)
            if scale is not None:
                value=scale(value)
            return value
        if self.compiled_form.b > 0:
            unscaled_header = unpack(self.compiled_form.ht, payload[0:self.compiled_form.b])
            for field_name,i_unpack,b1,b0,scale in zip(self.compiled_form.hn,self.compiled_form.hp,self.compiled_form.h1,self.compiled_form.h0,self.compiled_form.hs):
                setattr(self, field_name, scale_field(unscaled_header[i_unpack],b1,b0,scale))
        if self.compiled_form.m > 0:
            # The repeating blocks are represented in memory by a list of fields, each long enough to hold
            # one element for each repeat. Following the database convention, we will call the collection
            # of numbers which all mean the same thing for different repeats, a "column" or "field", and
            # the collection of numbers which all mean different things in the same repeat, a "row".
            d = len(payload)
            assert (d - self.compiled_form.b - self.compiled_form.c) % self.compiled_form.m == 0, f"Non-integer number of rows in {self.__class__.__name__}, b={self.compiled_form.b}, c={self.compiled_form.c}, m={self.compiled_form.m}"
            n_rows = (d - self.compiled_form.b - self.compiled_form.c) // self.compiled_form.m
            n_cols = len(self.compiled_form.bs)
            # in memory -- the blocks are a tuple of columns, each a list long enough to hold one member
            # for each row. This means each cell has a double index, the first one being the column index,
            # second being row. We do it this way so that we can concatenate it with the header and
            # footer tuple, and hand the combo right off to the _make() method of namedtuple.
            cols = tuple([[None for x in range(n_rows)] for y in range(n_cols)])
            for i_row in range(n_rows):
                row0=self.compiled_form.b + i_row * self.compiled_form.m
                row1=row0+self.compiled_form.m
                row_payload=payload[row0:row1]
                unscaled_row = unpack(self.compiled_form.bt, row_payload)
                for i_col, (i_unpack, b1, b0, scale) in enumerate(zip(self.compiled_form.bp,self.compiled_form.b1, self.compiled_form.b0,self.compiled_form.bs)):
                    cols[i_col][i_row]=scale_field(unscaled_row[i_unpack], b1, b0, scale)
            for i_col,field_name in enumerate(self.compiled_form.bn):
                setattr(self,field_name,cols[i_col])
        if self.compiled_form.c > 0:
            i0=self.compiled_form.b + n_rows * self.compiled_form.m
            i1=i0+self.compiled_form.c
            unscaled_footer = unpack(self.compiled_form.ft, payload[i0:i1])
            for field_name,i_unpack,b1,b0,scale in zip(self.compiled_form.fn,self.compiled_form.fp,self.compiled_form.f1,self.compiled_form.f0,self.compiled_form.fs):
                setattr(self, field_name, scale_field(unscaled_footer[i_unpack],b1,b0,scale))
        self.fixup()
    def fixup(self)->None:
        """
        Once a packet has been read, run this to calculate some fields from other fields
        """
        if self.required_version is not None and self.required_version!=self.version:
            raise ValueError(f"Bad version for packet {self.__class__.__name__} version 0x{self.version:02x}, expected 0x{self.required_version:02x}")

    def write(self,db,*,fileid:int,ofs:int,epochid:int=None)->None:
        table_name = self.__class__.__name__[4:].lower()
        parent_fields=self.compiled_form.hq+self.compiled_form.fq
        values=[getattr(self,field_name) for field_name in parent_fields]+[fileid,ofs]
        parent_fields+=["file","ofs"]
        if self.use_epoch:
            if epochid is None:
                raise ValueError("No epoch id for a packet that needs it")
            parent_fields+=["epoch"]
            values+=[epochid]
        parent=db.insert_get_id(table_name,parent_fields,values)
        if self.compiled_form.bf is not None and len(self.compiled_form.bq)>0:
            columns=tuple([getattr(self,field_name) for field_name in self.compiled_form.bq])
            block_field_names=["parent",]+self.compiled_form.bq
            for values in zip(*columns):
                db.insert(table_name+"_block",block_field_names,(parent,)+values)
    def __init__(self,cls:int,id:int,payload:bytes):
        self.cls = cls
        self.id = id
        self.payload = payload
        if hasattr(self,'compiled_form'):
            self.parse_payload(payload)


def bin_field(raw_type:str, **kwargs):
    """
    Annotate a field with the necessary data to extract it from a binary packet. Raw type is required, any
    other named parameter will be included in the resulting metadata dictionary. Any value may be provided,
    but parameters below have special meaning to other parts of the code.

    :param raw_type: type of raw data in UBX form (UBX manual 3.3.5) field type in the binary packet data
              U1 - unsigned 8-bit int (B)
              I1 - signed 8-bit int (b)
              X1 - 8-bit bitfield or padding, treat as unsigned to make bit-manipulation easier (B)
              U2 - unsigned 16-bit int (<H)
              I2 - signed 16-bit int (<h)
              X2 - 16-bit bitfield (<H)
              U4 - unsigned 32-bit int (<I)
              I4 - signed 32-bit int (<i)
              X4 - 32-bit bitfield (<I)
              R4 - IEEE-754 32-bit floating point (<f)
              R8 - IEEE-754 64-bit floating point (<d)
    :param scale: either a number or a callable. If a number, the raw value in the binary data
               is multiplied by this value to get the scaled value. If a callable, it must
               take a single parameter and will be passed the raw binary data.
               The return type should match the declared type of the field. Default will
               result in the scaled value being the same type and value as the raw value.
    :param unit: Unit of the final scaled value. Generally we will scale values such that the
               units can be an SI base or derived unit with no prefixes -- IE if a field is
               integer milliseconds, use a scale of 1e-3 and declare the units to be "s" for
               seconds. Default is no unit.
    :param fmt: Format string for displaying the field
    :param b1: declares the value as a bitfield. All consecutive fields with the same type
               are considered to be the same bitfield. This is the lower bit (LSB is bit 0)
    :param b0: If a bitfield, this is the upper bit
    :param comment: Used to add the appropriate comment to the table field
    :param
    :return: A dictionary appropriate for passing to field(metadata=)
    """
    kwargs['type']=raw_type
    return kwargs


def register_ublox(cls:int,id:int,pktcls:dataclass)->None:
    #register the class after it is compiled
    if cls not in read_ublox_packet.classes:
        read_ublox_packet.classes[cls]={}
    read_ublox_packet.classes[cls][id]=pktcls


def compile_ublox(pktcls:dataclass)->None:
    """
    Compile a field_dict from the form that most closely matches the
    book to something more usable at runtime

    :param pktclass: Dataclass with names annotated with field(...,metadata=md()).
    :return: named tuple
      * b: number of bytes before repeating block
      * m: number of bytes in repeating block
      * c: number of bytes after repeating block
      for the following, ? is header, block, or footer
      * *_fields: iterable of header field names in order (possibly empty)
      * *_type: string suitable for handing to struct.unpack, for names before repeating block
      * *_unpack: iterable of index of struct.unpack result to use for this field
      * *_scale: iterable of lambdas which scale the field for names before repeating block
      * *_units: iterable units for names before repeating block
      * *_b0: iterable of bitfield position 0, if this is a bitfield.
      * *_b1: iterable of bitfield position 1, if this is a bitfield.

    At parse time, the number of repeats of the repeating block is determined as follows:

    d: full packet size
    n: number of repeats
    d=b+m*n+c
    d-b-c=m*n
    (d-b-c)/m=n
    """

    def make_scale(scale):
        if scale is None:
            return lambda x: x
        elif callable(scale):
            return scale
        else:
            return partial(lambda s, x: s * x, scale)

    def fmt_width(fmt):
        match = re.match("( *)[^1-9]*(\d+).*", fmt)
        return len(match.group(1)) + int(match.group(2))

    def fmt_set_width(fmt, width):
        match = re.match("(?P<spaces> *)(?P<prefix>[^1-9]*)(?P<sigwidth>\d+)(?P<suffix>.*)", fmt)
        old_width = int(match.group("sigwidth"))
        if width < old_width:
            return match.group("spaces") + match.group("prefix") + str(width) + match.group("suffix")
        else:
            return " " * (width - old_width) + match.group("prefix") + str(old_width) + match.group("suffix")

    size_dict={"U1":("B",1,"%3d"),
               "U2":("H",2,"%5d"),
               "U4":("I",4,"%9d"),
               "I1":("b",1,"%4d"),
               "I2":("h",2,"%6d"),
               "I4":("i",4,"%10d"),
               "X1":("B",1,"%02x"),
               "X2":("H",2,"%04x"),
               "X4":("I",4,"%08x"),
               "R4":("f",4,"%14.7e"),
               "R8":("d",8,"%21.14e")}
    last_x=None
    lengths=[0,0,0]
    names=[[],[],[]]
    types=["","",""]
    scales=[[],[],[]]
    unpacks=[[],[],[]]
    units=[[],[],[]]
    fmts=[[],[],[]]
    widths=[[],[],[]]
    b0s=[[],[],[]]
    b1s=[[],[],[]]
    record_names=[[],[],[]]
    part=0
    i_struct=0
    last_b1=None
    for field in fields(pktcls):
        if field.metadata["type"] is None:
            if 'record' not in field.metadata or field.metadata['record']:
                record_names[part].append(field.name)
            continue
        if str(field.type)[0:4]=='list':
            if part==0:
                part=1
                last_x=None
                i_struct=0
        else:
            if part==1:
                part=2
                last_x=None
                i_struct = 0
        names[part].append(field.name)
        if 'record' not in field.metadata or field.metadata['record']:
            record_names[part].append(field.name)
        ublox_type=field.metadata['type']
        if 'b0' in field.metadata:
            # Handle bitfields
            if (ublox_type==last_x) and (last_b1 is not None) and (field.metadata['b0']>last_b1):
                i_struct-=1
            else:
                types[part] += size_dict[ublox_type][0]
                lengths[part] += size_dict[ublox_type][1]
            last_x=ublox_type
            b0s[part].append(field.metadata['b0'])
            if 'b1' in field.metadata:
                last_b1=field.metadata['b1']
            else:
                last_b1=field.metadata['b0']
            b1s[part].append(last_b1)
        else:
            if ublox_type[0:2]=="CH":
                #handle strings CHxx. Returned value is a byte array of exactly this many bytes
                types[part]+=ublox_type[2:]+"s"
                lengths[part]+=int(ublox_type[2:])
            elif ublox_type[1]=="[":
                #Handle byte arrays U[xx]
                types[part]+=ublox_type[2:-1]+"s"
                lengths[part]+=int(ublox_type[2:-1])
            else:
                #handle numbers
                types[part] += size_dict[ublox_type][0]
                lengths[part] += size_dict[ublox_type][1]
            b0s[part].append(None)
            b1s[part].append(None)
            last_x=None
        unpacks[part].append(i_struct)
        print(i_struct,lengths[part],field.name)
        i_struct+=1
        if 'scale' in field.metadata:
            scales[part].append(make_scale(field.metadata['scale']))
        else:
            scales[part].append(None)
        if 'unit' in field.metadata:
            units[part].append(field.metadata['unit'])
        else:
            units[part].append(None)
        #if 'fmt' in field.metadata:
        #    fmt=field.metadata['fmt']
        #else:
        #    fmt=size_dict[ublox_type][2]
        #if part==1:
        #    colhead_width=len(field.name)+(0 if units[part][-1] is None else 3+len(units[part][-1]))
        #    if fmt_width(fmt)<colhead_width:
        #        fmt=fmt_set_width(fmt,colhead_width)
        #fmts[part].append(fmt)
        #widths[part].append(fmt_width(fmt))
    b,m,c=lengths
    header_fields,block_fields,footer_fields=names
    header_types,block_types,footer_types=["<"+x for x in types]
    header_scale,block_scale,footer_scale=scales
    header_units,block_units,footer_units=units
    header_format,block_format,footer_format=fmts
    header_widths,block_widths,footer_widths=widths
    header_b0,block_b0,footer_b0=b0s
    header_b1,block_b1,footer_b1=b1s
    header_unpack,block_unpack,footer_unpack=unpacks
    header_records,block_records,footer_records=record_names
    pktcls.compiled_form=namedtuple("packet_desc","b m c hn ht hs hu hf hw h0 h1 hp hq bn bt bs bu bf bw b0 b1 bp bq fn ft fs fu ff fw f0 f1 fp fq")._make((b,m,c,
            header_fields,header_types,header_scale,header_units,header_format,header_widths,header_b0,header_b1,header_unpack,header_records,
            block_fields,block_types,block_scale,block_units,block_format,block_widths,block_b0,block_b1,block_unpack,block_records,
            footer_fields, footer_types, footer_scale, footer_units, footer_format,footer_widths,footer_b0,footer_b1,footer_unpack,footer_records))


def ensure_table(db:Database,pktcls:dataclass,drop:bool)->None:
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
    table_name=pktcls.__name__[4:].lower()
    unique_tuples=[]
    indexes=[]
    if pktcls.use_epoch:
        table_fields.append(Field(name="epoch",python_type=int,comment="Foreign key to epoch table, holding exact UTC "
                                                                       "time. Across all tables, all rows with the "
                                                                       "same epoch id represent data describing the "
                                                                       "exact same instant in time."))
        unique_tuples.append(("epoch",))
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


def ublox_packet(cls:int,id:int,*,use_epoch:bool=True,required_version:int=None):
    def inner(pktcls):
        __class__=pktcls
        def __init__(self, cls: int, id: int, payload: int):
            super().__init__(cls, id, payload)
        pktcls.__init__=__init__
        pktcls=dataclass(pktcls)
        pktcls.use_epoch=use_epoch
        pktcls.required_version=required_version
        compile_ublox(pktcls)
        register_ublox(cls,id,pktcls)
        return pktcls
    return inner


def ensure_tables(db:Database,drop:bool=False):
    for cls,ids in read_ublox_packet.classes.items():
        for id,packet in ids.items():
            ensure_table(db,packet,drop=drop)


