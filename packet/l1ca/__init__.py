# GPS L1C/A Nav Message description. Each field consists of:
# key is name of field
# value is tuple
#  * True if value is twos-complement signed, False if unsigned
#  * Scaling factor or function, or None if the scaling is identity.
#  * Physical units of scaled value, or None if there are none.
#  * Recommended format string, suitable for the % operator.
#
from collections import namedtuple
from dataclasses import field, dataclass, fields
from datetime import datetime
from enum import Enum
from typing import Iterable, Callable, ClassVar

from psycopg import connect


@dataclass
class GPSSat:
    svn:int
    class Block(Enum):
        I=0
        II=1
        IIA=2
        IIR=3
        IIR_M=4
        IIF=5
        III=6
    block:Block
    serial:int
    plane:str
    slot:int
    class Clock(Enum):
        Cs=0
        Rb=1
    clock:Clock
    USA:int
    launch_date:datetime
    launch_vehicle:str
    retire_date:datetime


# GPS constellation as of 2023-09-06. Sources:
#  * https://www.navcen.uscg.gov/gps-constellation?order=field_gps_prn&sort=asc
#  * https://en.wikipedia.org/wiki/List_of_GPS_satellites
#  * https://planet4589.org/space/gcat/data/derived/launchlog.html
gps_constellation={
     1:GPSSat(svn=63,block=GPSSat.Block.IIF  ,serial= 2,plane='D',slot=2,clock=GPSSat.Clock.Rb,USA=232,launch_date=datetime(2011, 7,16, 6,41),launch_vehicle='Delta IV M+(4,2) #355',retire_date=datetime(2023, 8,10)),
     2:GPSSat(svn=61,block=GPSSat.Block.IIR  ,serial=13,plane='D',slot=1,clock=GPSSat.Clock.Rb,USA=180,launch_date=datetime(2004,11, 6, 5,39),launch_vehicle='Delta II 7925-9.5 #308',retire_date=None),
     3:GPSSat(svn=69,block=GPSSat.Block.IIF  ,serial= 8,plane='E',slot=1,clock=GPSSat.Clock.Rb,USA=258,launch_date=datetime(2014,10,29,17,21),launch_vehicle='Atlas V 401 AV-050',retire_date=None),
     4:GPSSat(svn=74,block=GPSSat.Block.III  ,serial= 1,plane='F',slot=4,clock=GPSSat.Clock.Rb,USA=289,launch_date=datetime(2018,12,23,13,51),launch_vehicle='Falcon 9 Block 5 B1054',retire_date=None),
     5:GPSSat(svn=50,block=GPSSat.Block.IIR_M,serial= 8,plane='E',slot=3,clock=GPSSat.Clock.Rb,USA=206,launch_date=datetime(2009, 8,17,10,35, 0),launch_vehicle='Delta II 7925-9.5 #343',retire_date=None),
     6:GPSSat(svn=67,block=GPSSat.Block.IIF  ,serial= 6,plane='D',slot=4,clock=GPSSat.Clock.Rb,USA=251,launch_date=datetime(2014, 5,17, 0, 3, 0),launch_vehicle='Delta IV M+(4,2) #366',retire_date=None),
     7:GPSSat(svn=48,block=GPSSat.Block.IIR_M,serial= 6,plane='A',slot=4,clock=GPSSat.Clock.Rb,USA=201,launch_date=datetime(2008, 3,15, 6,10),launch_vehicle='Delta II 7925-9.5 #332',retire_date=None),
     8:GPSSat(svn=72,block=GPSSat.Block.IIF  ,serial=10,plane='C',slot=3,clock=GPSSat.Clock.Cs,USA=262,launch_date=datetime(2015, 7,15,15,36),launch_vehicle='Atlas V 401 AV-055',retire_date=None),
     9:GPSSat(svn=68,block=GPSSat.Block.IIF  ,serial= 7,plane='F',slot=3,clock=GPSSat.Clock.Rb,USA=256,launch_date=datetime(2014, 8, 2, 3,23),launch_vehicle='Atlas V 401 AV-048',retire_date=None),
    10:GPSSat(svn=73,block=GPSSat.Block.IIF  ,serial= 7,plane='E',slot=2,clock=GPSSat.Clock.Rb,USA=265,launch_date=datetime(2015,10,31,16,13),launch_vehicle='Atlas V 401 AV-060',retire_date=None),
    11:GPSSat(svn=78,block=GPSSat.Block.III  ,serial= 5,plane='D',slot=5,clock=GPSSat.Clock.Rb,USA=319,launch_date=datetime(2021, 6,17,16, 9),launch_vehicle='Falcon 9 Block 5 B1062-2',retire_date=None),
    12:GPSSat(svn=58,block=GPSSat.Block.IIR_M,serial= 3,plane='B',slot=4,clock=GPSSat.Clock.Rb,USA=192,launch_date=datetime(2006,11,17,19,12),launch_vehicle='Delta II 7925-9.5 #321',retire_date=None),
    13:GPSSat(svn=43,block=GPSSat.Block.IIR  ,serial= 2,plane='F',slot=6,clock=GPSSat.Clock.Rb,USA=132,launch_date=datetime(1997, 7,23, 3,43, 1),launch_vehicle='Delta II 7925-9.5 #245',retire_date=None),
    14:GPSSat(svn=77,block=GPSSat.Block.III  ,serial= 4,plane='B',slot=6,clock=GPSSat.Clock.Rb,USA=309,launch_date=datetime(2020,11, 5,23,24,23),launch_vehicle='Falcon 9 Block 5 B1062-1',retire_date=None),
    15:GPSSat(svn=55,block=GPSSat.Block.IIR_M,serial= 4,plane='F',slot=2,clock=GPSSat.Clock.Rb,USA=196,launch_date=datetime(2007,10,17,12,23, 0),launch_vehicle='Delta II 7925-9.5 #328',retire_date=None),
    16:GPSSat(svn=56,block=GPSSat.Block.IIR  ,serial= 8,plane='B',slot=1,clock=GPSSat.Clock.Rb,USA=166,launch_date=datetime(2003, 1,29,18, 6),launch_vehicle='Delta II 7925-9.5 #295',retire_date=None),
    17:GPSSat(svn=53,block=GPSSat.Block.IIR_M,serial= 1,plane='C',slot=4,clock=GPSSat.Clock.Rb,USA=183,launch_date=datetime(2005, 9,26, 3,37),launch_vehicle='Delta II 7925-9.5 #313',retire_date=None),
    18:GPSSat(svn=75,block=GPSSat.Block.III  ,serial= 2,plane='D',slot=6,clock=GPSSat.Clock.Rb,USA=293,launch_date=datetime(2019, 8,22,13, 6, 0),launch_vehicle='Delta IV M+(4,2) #384',retire_date=None),
    19:GPSSat(svn=59,block=GPSSat.Block.IIR  ,serial=11,plane='C',slot=5,clock=GPSSat.Clock.Rb,USA=177,launch_date=datetime(2004, 3,20,17,53),launch_vehicle='Delta II 7925-9.5 #303',retire_date=None),
    20:GPSSat(svn=51,block=GPSSat.Block.IIR  ,serial= 4,plane='E',slot=4,clock=GPSSat.Clock.Rb,USA=150,launch_date=datetime(2000, 5,11, 1,48),launch_vehicle='Delta II 7925-9.5 #278',retire_date=None),
    21:GPSSat(svn=45,block=GPSSat.Block.IIR  ,serial= 9,plane='D',slot=3,clock=GPSSat.Clock.Rb,USA=168,launch_date=datetime(2003, 3,31,22, 9),launch_vehicle='Delta II 7925-9.5 #297',retire_date=None),
    23:GPSSat(svn=76,block=GPSSat.Block.III  ,serial= 3,plane='E',slot=5,clock=GPSSat.Clock.Rb,USA=304,launch_date=datetime(2020, 6,30,20,10,46),launch_vehicle='Falcon 9 Block 5 B1060-1',retire_date=None),
    24:GPSSat(svn=65,block=GPSSat.Block.IIF  ,serial= 3,plane='A',slot=1,clock=GPSSat.Clock.Cs,USA=239,launch_date=datetime(2012,10, 4, 2,32),launch_vehicle='Delta IV M+(4,2) #361',retire_date=None),
    25:GPSSat(svn=62,block=GPSSat.Block.IIF  ,serial= 1,plane='B',slot=2,clock=GPSSat.Clock.Rb,USA=213,launch_date=datetime(2010, 5,28, 3, 0),launch_vehicle='Delta IV M+(4,2) #370',retire_date=None),
    26:GPSSat(svn=71,block=GPSSat.Block.IIF  ,serial= 9,plane='B',slot=5,clock=GPSSat.Clock.Rb,USA=260,launch_date=datetime(2015, 3,25,18,36),launch_vehicle='Delta IV M+(4,2) #370',retire_date=None),
    27:GPSSat(svn=66,block=GPSSat.Block.IIF  ,serial= 4,plane='C',slot=2,clock=GPSSat.Clock.Rb,USA=242,launch_date=datetime(2013, 5,15,21,38),launch_vehicle='Atlas V 401 AV-039',retire_date=None),
    28:GPSSat(svn=79,block=GPSSat.Block.III  ,serial= 6,plane='A',slot=6,clock=GPSSat.Clock.Rb,USA=343,launch_date=datetime(2023, 1,18,12,24),launch_vehicle='Falcon 9 Block 5 B1077-2',retire_date=None),
    29:GPSSat(svn=57,block=GPSSat.Block.IIR_M,serial= 5,plane='C',slot=1,clock=GPSSat.Clock.Rb,USA=199,launch_date=datetime(2007,12,20,20, 4),launch_vehicle='Delta II 7925-9.5 #331',retire_date=None),
    30:GPSSat(svn=64,block=GPSSat.Block.IIF  ,serial= 5,plane='A',slot=3,clock=GPSSat.Clock.Rb,USA=248,launch_date=datetime(2014, 2,21, 1,59),launch_vehicle='Delta IV M+(4,2) #365',retire_date=None),
    31:GPSSat(svn=52,block=GPSSat.Block.IIR_M,serial= 2,plane='A',slot=2,clock=GPSSat.Clock.Rb,USA=190,launch_date=datetime(2006, 9,25,18,50),launch_vehicle='Delta II 7925-9.5 #318',retire_date=None),
    32:GPSSat(svn=70,block=GPSSat.Block.IIF  ,serial=12,plane='F',slot=1,clock=GPSSat.Clock.Rb,USA=266,launch_date=datetime(2016, 2, 5,13,38),launch_vehicle='Atlas V 401 AV-057',retire_date=None),
}



def get_bits(dwrd:Iterable[int], b0:int, b1:int):
    """
    Get bits from a payload

    :param dwrd:
    :param b0: most significant bit. MSB of word 0 is bit 1
    :param b1: least significant bit. LSB of word 9 is 300
    :return: unsigned int raw bitfield
    """
    dwrd_i = (b0 - 1) // 30
    rel_0 = b0 - (dwrd_i) * 30
    rel_1 = b1 - (dwrd_i) * 30
    width = rel_1 - rel_0 + 1
    shift = 30 - rel_1
    mask = (1 << width) - 1
    return (dwrd[dwrd_i] >> shift) & mask


def get_multi_bits(dwrd:Iterable[int], parts:Iterable[tuple[int,int]], signed:bool):
    """
    Get bits from several positions in a payload

    :param dwrd:
    :param parts:
    :param signed:
    :return:
    """
    result = 0
    width = 0
    for (b0, b1) in parts:
        result = result << (b1 - b0 + 1)
        result = result | get_bits(dwrd, b0, b1)
        width += (b1 - b0 + 1)
    if signed:
        cutoff = 1 << (width - 1)
        if result >= cutoff:
            result -= 2 * cutoff
    return result



def l1ca_md(parts:Iterable[tuple[int,int]], **kwargs):
    """
    Annotate a field with the necessary data to extract it from a binary packet. parts is required, any
    other named parameter will be included in the resulting metadata dictionary. Any value may be provided,
    but parameters below have special meaning to other parts of the code.

    :param parts: list of tuples, each indicating the start and end bit of a part of the value. Most fields are
                  contiguous, so there is only one entry in the list. If a field is discontiguous, each part will be
                  ordered in the list such that the more significant parts are earlier. Each tuple is the start and end
                  bits of the field part, numbered according to the ICD convention of bit 1 being the most significant
                  bit of dwrd[0], bit 31 being most significant of dwrd[1], etc. No part will ever cross a word
                  boundary (IE not something like 25,35) because the parity bits always delimit each word. Starts and
                  ends are inclusive (unlike Python in general, where start is included but end is not). So, the
                  preamble is bits 1-8, and should always be the preamble value 0x8B. If a word is documented as being
                  so many bits from the beginning of the message, write that, and it will be in the range 1-300. If the
                  word is documented as being so M bits in word N, write it as (N-1)*30+M (and explicitly write 8-1,
                  not 7, for N=8)
    :param signed: True if value is twos-complement signed, False (default) if unsigned
    :param scale:  either a number or a callable. If a number, the raw value in the binary data is multiplied by this
                   value to get the scaled value. If a callable, it must take a single parameter and will be passed the
                   raw binary data. The return type should match the declared type of the field. Default will result in
                   the scaled value being the same type and value as the raw value.
    :param unit: Unit of the final scaled value. Generally we will scale values such that the units can be an SI base or
                 derived unit with no prefixes -- IE if a field is integer milliseconds, use a scale of 1e-3 and declare
                 the units to be "s" for seconds. Default is no unit.
    :param comment: Used to add the appropriate comment to the table field
    :return: A dictionary appropriate for passing to field(metadata=)
    """
    kwargs['parts']=parts
    return kwargs


def compile_msg(cls):
    cls.compiled_form=True
    return cls


@dataclass
class L1CAMessage:
    # svid - not encoded in message, therefore must come from another source like rxm_sfrbx record
    prn:int
    sat:GPSSat
    # TLM word (word 1)
    preamble  :int=field(metadata=l1ca_md([( 1, 8)]))
    tlm       :int=field(metadata=l1ca_md([( 9,22)]))
    integ_stat:int=field(metadata=l1ca_md([(24,24)]))
    # HOW word (word 2)
    tow_count :int=field(metadata=l1ca_md([(31,47)]))
    alert     :bool=field(metadata=l1ca_md([(48,48)],scale=bool))
    antispoof :bool=field(metadata=l1ca_md([(49,49)],scale=bool))
    subframe  :int=field(metadata=l1ca_md([(50,52)]))
    # Factory map: A dictionary used in the factory method read_msg(). The
    # key of each is an int, and the value is a callable which takes a payload
    # and returns an object of class L1CAMessage. Register each callable like this:
    # class Subframe1(L1CAMessage):
    #    ...
    #    def __init__(self,payload:Iterable[int])->'L1CAMessage':
    #        super().parse_payload(payload)
    # L1CAMessage.subframe[1]=Subframe1
    factory_map:ClassVar[dict[int,Callable[[Iterable[int]], 'L1CAMessage']]]={}
    factory_map45:ClassVar[dict[int,Callable[[Iterable[int]], 'L1CAMessage']]]={}
    @staticmethod
    def read_msg(svid:int,payload: Iterable[int]) -> 'L1CAMessage':
        subframe = get_bits(payload, 50, 52)
        if subframe==4 or subframe==5:
            page_id=get_bits(payload,63,63+6-1)
            if page_id in L1CAMessage.factory_map45:
                return L1CAMessage.factory_map45[page_id](svid, subframe, payload)
            else:
                return L1CAMessage.factory_map[subframe](svid, subframe, payload)
        elif subframe in L1CAMessage.factory_map:
            return L1CAMessage.factory_map[subframe](svid,subframe,payload)
        else:
            return L1CAMessage(svid,subframe,payload)
    def parse_payload(self,payload:Iterable[int]):
        for field in fields(self):
            if 'parts' in field.metadata:
                value = get_multi_bits(payload, field.metadata['parts'], field.metadata.get('signed',False))
                scale=field.metadata.get('scale',None)
                if callable(scale):
                    value = scale(value)
                elif scale is not None:
                    value = scale * value
                setattr(self,field.name,value)
    def __init__(self,svid:int,subframe:int,payload:Iterable[int]):
        """

        :param payload:
        """
        self.prn=svid
        self.sat=gps_constellation[self.prn]
        self.subframe=subframe
        self.payload = payload
        self.parse_payload(payload)


@dataclass
class Subframe1(L1CAMessage):
    wn       :int=field(metadata=l1ca_md([(61,70)],unit="week"))
    msg_on_l2:int=field(metadata=l1ca_md([(71,72)]))
    def ura_nom(N:int):
        """
        Calculate the nominal URA. We will store this instead of the encoded URA
        :return:
        """
        if N == 1:
            return 2.8
        if N == 3:
            return 5.7
        if N == 5:
            return 11.3
        if N == 15:
            return float('inf')
        if N <= 6:
            return 2 ** (1 + N / 2)
        return 2 ** (N - 2)
    ura:int=field(metadata=l1ca_md([(73,76)],scale=ura_nom))
    sv_health:int=field(metadata=l1ca_md([(77,82)]))
    iodc:int=field(metadata=l1ca_md([(83,84),(211,218)]))
    t_gd:float=field(metadata=l1ca_md([(197,197+8-1)],signed=True,scale=2**-31,unit="s"))
    t_oc:float=field(metadata=l1ca_md([(219,219+16-1)],scale=2**4,unit="s"))
    a_f2:float=field(metadata=l1ca_md([(241,241+8-1)],signed=True,scale=2**-55,unit="s/s**2"))
    a_f1:float=field(metadata=l1ca_md([(241+8,241+8+16-1)],signed=True,scale=2**-43,unit="s/s"))
    a_f0:float=field(metadata=l1ca_md([(271,271+22-1)],signed=True,scale=2**-31,unit="s"))
    def __init__(self,svid:int,subframe:int,payload:Iterable[int])->'Subframe1':
        super().__init__(svid,subframe,payload)
L1CAMessage.factory_map[1]=Subframe1

@dataclass
class Subframe2(L1CAMessage):
    iode:int=field(metadata=l1ca_md([(61,68)]))
    c_rs:float=field(metadata=l1ca_md([(69,69+16-1)],signed=True,scale=2**-5,unit="m"))
    delta_n:float=field(metadata=l1ca_md([(91,106)],signed=True,scale=2**-43,unit="semicircle/s"))
    M_0:float=field(metadata=l1ca_md([(107,107+8-1),(121,121+24-1)],signed=True,scale=2**-31,unit="semicircle"))
    c_uc:float=field(metadata=l1ca_md([(151,166)],signed=True,scale=2**-29,unit="rad"))
    e:float=field(metadata=l1ca_md([(167,167+8-1),(181,181+24-1)],scale=2**-33))
    c_us:float=field(metadata=l1ca_md([(211,226)],signed=True,scale=2**-29,unit="rad"))
    A:float=field(metadata=l1ca_md([(227,227+8-1),(241,241+24-1)],scale=lambda sqrtA:(sqrtA*2**-19)**2,unit="m"))
    t_oe:int=field(metadata=l1ca_md([(271,286)],scale=2**4,unit="s"))
    fit:int=field(metadata=l1ca_md([(287,287)]))
    aodo:int=field(metadata=l1ca_md([(288,288+5-1)],scale=900,unit="s"))
    def __init__(self,svid:int,subframe:int,payload:Iterable[int])->'Subframe2':
        super().__init__(svid,subframe,payload)
L1CAMessage.factory_map[2]=Subframe2

@dataclass
class Subframe3(L1CAMessage):
    c_ic:int=field(metadata=l1ca_md([(61, 77-1)], signed=True,scale=2**-29,unit="rad"))
    Omega_0:float=field(metadata=l1ca_md([(77,77+8-1),(91,91+24-1)], signed=True, scale=2 ** -31, unit="semicircle"))
    c_is:float=field(metadata=l1ca_md([(121, 137-1)], signed=True, scale=2 ** -29, unit="rad"))
    i_0:float=field(metadata=l1ca_md([(137, 137 + 8 - 1), (151, 151 + 24 - 1)], signed=True, scale=2 ** -31,unit= "semicircle"))
    c_rc:float=field(metadata=l1ca_md([(181, 181+16-1)], signed=True, scale=2 ** -5, unit="m"))
    omega:float=field(metadata=l1ca_md([(181+16,181+16+8-1), (211, 211 + 24 - 1)], scale=2 ** -33))
    Omegad:float=field(metadata=l1ca_md([(241,241+24-1)], signed=True, scale=2 ** -29, unit="semicircle/s"))
    iode:float=field(metadata=l1ca_md([(271, 271 + 8 - 1)]))
    idot:float=field(metadata=l1ca_md([(279,279+14-1)], scale=2 ** -43, unit="s"))
    def __init__(self,svid:int,subframe:int,payload:Iterable[int])->'Subframe3':
        super().__init__(svid,subframe,payload)
L1CAMessage.factory_map[3]=Subframe3


@dataclass
class Subframe45(L1CAMessage):
    data_id:int=field(metadata=l1ca_md([(61,61+2-1)]))
    page_id:int=field(metadata=l1ca_md([(63,63+6-1)]))
    def __init__(self,svid:int,subframe:int,payload:Iterable[int])->'Subframe45':
        super().__init__(svid,subframe,payload)
L1CAMessage.factory_map[4]=Subframe45
L1CAMessage.factory_map[5]=Subframe45

@dataclass
class Almanac(Subframe45):
    e:float=field(metadata=l1ca_md([(69,69+16-1)],scale=2**-21))
    t_oa:int=field(metadata=l1ca_md([(91,91+8-1)],scale=2**12,unit="s"))
    i:float=field(metadata=l1ca_md([(99,99+16-1)],signed=True,scale=lambda delta_i:delta_i*2**-19+0.3,unit="semicircle"))
    Omegad:float=field(metadata=l1ca_md([(121,121+16-1)],signed=True,scale=2**-38,unit="semicircle/s"))
    health:int=field(metadata=l1ca_md([(121+16,121+16+8-1)]))
    A:float=field(metadata=l1ca_md([(151,151+24-1)],scale=lambda sqrtA:(sqrtA*2**-11)**2,unit="m"))
    Omega0:float=field(metadata=l1ca_md([(181,181+24-1)],signed=True,scale=2**-23,unit="semicircle"))
    omega:float=field(metadata=l1ca_md([(211,211+24-1)],signed=True,scale=2**-23,unit="semicircle"))
    M0:float=field(metadata=l1ca_md([(241,241+24-1)],signed=True,scale=2**-23,unit="semicircle"))
    a_f0:float=field(metadata=l1ca_md([(271,271+8-1),(290,290+3-1)],signed=True,scale=2**-20,unit="s"))
    a_f1:float=field(metadata=l1ca_md([(279,279+11-1)],signed=True,scale=2**-38,unit="s/s"))
    def __init__(self,svid:int,subframe:int,payload:Iterable[int])->'Almanac':
        super().__init__(svid,subframe,payload)
# This one is treated as default
for i in range(1,33):
    L1CAMessage.factory_map45[i]=Almanac #Subframe 5, pages 1-24, subframe 4, pages 2,3,4,5,7,8,9,10


class HealthEnum(Enum):
    All_Signals_OK = 0b00000
    All_Signals_Weak = 0b00001
    All_Signals_Dead = 0b00010
    All_Signals_Have_No_Data_Modulation = 0b00011
    L1P_Signal_Weak = 0b00100
    L1P_Signal_Dead = 0b00101
    L1P_Signal_Has_No_Data_Modulation = 0b00110
    L2P_Signal_Weak = 0b00111
    L2P_Signal_Dead = 0b01000
    L2P_Signal_Has_No_Data_Modulation = 0b01001
    L1C_Signal_Weak = 0b01010
    L1C_Signal_Dead = 0b01011
    L1C_Signal_Has_No_Data_Modulation = 0b01100
    L2C_Signal_Weak = 0b01101
    L2C_Signal_Dead = 0b01110
    L2C_Signal_Has_No_Data_Modulation = 0b01111
    L12P_Signal_Weak = 0b10000
    L12P_Signal_Dead = 0b10001
    L12P_Signal_Has_No_Data_Modulation = 0b10010
    L12C_Signal_Weak = 0b10011
    L12C_Signal_Dead = 0b10100
    L12C_Signal_Has_No_Data_Modulation = 0b10101
    L1_Signal_Weak = 0b10110
    L1_Signal_Dead = 0b10111
    L1_Signal_Has_No_Data_Modulation = 0b11000
    L2_Signal_Weak = 0b11001
    L2_Signal_Dead = 0b11010
    L2_Signal_Has_No_Data_Modulation = 0b11011
    SV_Is_Out = 0b11100
    SV_Will_Be_Out = 0b11101
    One_Or_More_Deformed_URA_Accurate = 0b11110
    Multiple_Anomalies = 0b11111


@dataclass
class Health:
    lnavBad: bool
    bits: HealthEnum
    def __init__(self,raw):
        self.lnavBad=raw>32
        self.bits=HealthEnum(raw%32)


@dataclass
class SVHealth(Subframe45):
    """
    All fields are marked reserved and are undocumented
    """
    t_oa:int=field(metadata=l1ca_md([(69,69+8-1)],scale=2**12,unit="s"))
    wn_a:int=field(metadata=l1ca_md([(69+8,69+8+8-1)],unit="week"))

    @staticmethod
    def slot_pos(slot_num:int)->tuple[int,int]:
        b0=(slot_num//4+3)*30+1+(slot_num%4)*6
        b1=b0+6-1
        return (b0,b1)
    SV01Health:Health=field(metadata=l1ca_md([slot_pos( 1)],scale=Health))
    SV02Health:Health=field(metadata=l1ca_md([slot_pos( 2)],scale=Health))
    SV03Health:Health=field(metadata=l1ca_md([slot_pos( 3)],scale=Health))
    SV04Health:Health=field(metadata=l1ca_md([slot_pos( 4)],scale=Health))
    SV05Health:Health=field(metadata=l1ca_md([slot_pos( 5)],scale=Health))
    SV06Health:Health=field(metadata=l1ca_md([slot_pos( 6)],scale=Health))
    SV07Health:Health=field(metadata=l1ca_md([slot_pos( 7)],scale=Health))
    SV08Health:Health=field(metadata=l1ca_md([slot_pos( 8)],scale=Health))
    SV09Health:Health=field(metadata=l1ca_md([slot_pos( 9)],scale=Health))
    SV10Health:Health=field(metadata=l1ca_md([slot_pos(10)],scale=Health))
    SV11Health:Health=field(metadata=l1ca_md([slot_pos(11)],scale=Health))
    SV12Health:Health=field(metadata=l1ca_md([slot_pos(12)],scale=Health))
    SV13Health:Health=field(metadata=l1ca_md([slot_pos(13)],scale=Health))
    SV14Health:Health=field(metadata=l1ca_md([slot_pos(14)],scale=Health))
    SV15Health:Health=field(metadata=l1ca_md([slot_pos(15)],scale=Health))
    SV16Health:Health=field(metadata=l1ca_md([slot_pos(16)],scale=Health))
    SV17Health:Health=field(metadata=l1ca_md([slot_pos(17)],scale=Health))
    SV18Health:Health=field(metadata=l1ca_md([slot_pos(18)],scale=Health))
    SV19Health:Health=field(metadata=l1ca_md([slot_pos(19)],scale=Health))
    SV20Health:Health=field(metadata=l1ca_md([slot_pos(20)],scale=Health))
    SV21Health:Health=field(metadata=l1ca_md([slot_pos(21)],scale=Health))
    SV22Health:Health=field(metadata=l1ca_md([slot_pos(22)],scale=Health))
    SV23Health:Health=field(metadata=l1ca_md([slot_pos(23)],scale=Health))
    SV24Health:Health=field(metadata=l1ca_md([slot_pos(24)],scale=Health))

    def __init__(self,svid:int,subframe:int,payload:Iterable[int])->'SVHealth':
        super().__init__(svid,subframe,payload)
L1CAMessage.factory_map45[51]=SVHealth #Subframe 5, page 25


@dataclass
class SpecialMessage(Subframe45):
    special_msg:str
    def __init__(self,svid:int,subframe:int,payload:Iterable[int])->'SVHealth':
        super().__init__(svid,subframe,payload)
        buf=[get_bits(payload,69,69+8-1),get_bits(payload,69+8,69+8+8-1)]
        for i in range(4,10):
            for j in range(3):
                b0=(i-1)*30+j*8+1
                buf=buf+[get_bits(payload,b0,b0+8-1)]
        buf+=[get_bits(payload,271,271+8-1),get_bits(payload,271+8,271+8+8-1)]
        self.special_msg=str(bytes(buf),encoding='cp437')
        #print(self.special_msg)
L1CAMessage.factory_map45[55]=SpecialMessage #Subframe 4, page 17




@dataclass
class Subframe45Reserved(Subframe45):
    """
    All fields are marked reserved and are undocumented
    """
    def __init__(self,svid:int,subframe:int,payload:Iterable[int])->'Subframe45Reserved':
        super().__init__(svid,subframe,payload)
L1CAMessage.factory_map45[57]=Subframe45Reserved #Subframe 4, pages 1,6,11,16,21
L1CAMessage.factory_map45[62]=Subframe45Reserved #Subframe 4, page 12,24
L1CAMessage.factory_map45[58]=Subframe45Reserved #Subframe 4, page 19
L1CAMessage.factory_map45[59]=Subframe45Reserved #Subframe 4, page 20
L1CAMessage.factory_map45[60]=Subframe45Reserved #Subframe 4, page 22
L1CAMessage.factory_map45[61]=Subframe45Reserved #Subframe 4, page 23


def main():
    dbname="Atlantic23_05"
    import_files=False
    do_plot=True
    drop=False
    profile=False
    with connect(f"dbname={dbname} user=jeppesen password=Locking1blitz",autocommit=True) as conn:
        head_cur=conn.cursor()
        block_cur=conn.cursor()
        head_cur.execute("select id,svid from rxm_sfrbx where sigid='GPS_L1CA' order by id asc;")
        for id,svid in head_cur:
            block_cur.execute("select dwrd from rxm_sfrbx_block where parent=%s order by id asc;",(id,))
            payload=[dwrd for dwrd, in block_cur]
            msg=L1CAMessage.read_msg(svid,payload)
            print(msg)
            if type(msg)==SpecialMessage:
                print(msg)


if __name__=="__main__":
    main()
