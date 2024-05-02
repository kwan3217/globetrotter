"""
Import gpsd pseudo-NMEA, which are created from the input binary datastream (in this case uBlox).
"""
import bz2
import gzip
from glob import glob
from os.path import basename

from packet.ublox.protocol_33_21 import packet_names, UBX_MON_VER
from packet.ublox.ublox_packet import read_packet


def smart_open(fn,mode:str=None):
    if ".bz2" in fn:
        return bz2.open(fn,mode)
    elif ".gz" in fn:
        return gzip.open(fn,mode)
    else:
        return open(fn,mode)


def main():
    dbname="Atlantic23_05"
    import_files=True
    do_plot=True
    drop=True
    profile=False
    seen_clsids={}
    infns=(sorted(glob('/mnt/big/Atlantic23.05/Fluttershy/FluttershyBase/2023/05/07/*11-1*.ubx.bz2')))
    for infn in infns:
        print(infn)
        with smart_open(infn,"rb") as inf:
            this_ofs=0
            for packet in read_packet(inf):
                if hasattr(packet,'compiled_form'):
                    clsid=(packet.cls,packet.id)
                    if clsid not in seen_clsids:
                        print(f"First time seeing {type(packet)} cls=0x{packet.cls:02x}, id=0x{packet.id:02x}")
                        seen_clsids[clsid]=[True,0]
                    seen_clsids[clsid][1]+=1
                    if type(packet)==UBX_MON_VER:
                        print(packet)
                elif type(packet)==packet.ublox_packet.UBloxPacket:
                    clsid=(packet.cls,packet.id)
                    if clsid not in seen_clsids:
                        print(f"Unhandled packet cls=0x{packet.cls:02x}, id=0x{packet.id:02x}, {basename(infn)}:0x{this_ofs:08x}")
                        seen_clsids[clsid]=[False,0]
                    seen_clsids[clsid][1]+=1
                this_ofs=inf.tell()
    k=sorted(seen_clsids.keys())
    for cls,id in k:
        if cls in packet_names:
            clsname=packet_names[cls][0]
        else:
            clsname=f"0x{cls:02x}"
        if cls in packet_names and id in packet_names[cls][1]:
            idname=packet_names[cls][1][id]
        else:
            idname=f"0x{id:02x}"
        n=seen_clsids[(cls,id)]
        print(f"{clsname}-{idname} (0x{cls:02x},0x{id:02x}): {n}")


if __name__=="__main__":
    main()