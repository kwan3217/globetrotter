"""
Import gpsd pseudo-NMEA, which are created from the input binary datastream (in this case uBlox).
"""
import bz2
import gzip
from glob import glob
from os.path import basename

import numpy as np
from matplotlib import pyplot as plt
from database import Database
from database.postgres import PostgresDatabase
from packet.ublox.protocol_33_21 import packet_names, UBX_NAV_PVT, UBX_NAV_TIMEGPS, UBX_NAV_EOE, UBX_ESF_MEAS
from packet.ublox import ensure_tables, read_packet
from packet import register_file_start, register_file_finish, ensure_timeseries_tables, register_epoch


def smart_open(fn,mode:str=None):
    if ".bz2" in fn:
        return bz2.open(fn,mode)
    elif ".gz" in fn:
        return gzip.open(fn,mode)
    else:
        return open(fn,mode)


def handle_packet(db:Database, fileid:int, ofs:int, packet:'Packet')->None:
    """

    :param conn:
    :param fileid:
    :param ofs:
    :param packet:
    :return:
    """
    packet.fileid=fileid
    packet.ofs=ofs
    if packet.use_epoch:
        if not hasattr(handle_packet,'epoch_packets'):
            handle_packet.epoch_packets=[]
        handle_packet.epoch_packets.append(packet)
    else:
        if type(packet)!=UBX_ESF_MEAS:
            packet.write(db,fileid=fileid,ofs=ofs)
    if type(packet)==UBX_NAV_PVT:
        handle_packet.utc=packet.utc
        handle_packet.iTOW=packet.iTOW
    elif type(packet)==UBX_NAV_TIMEGPS:
        handle_packet.week=packet.week
    elif type(packet)==UBX_NAV_EOE:
        if ((hasattr(handle_packet,'utc' ) and handle_packet.utc  is not None) and
            (hasattr(handle_packet,'iTOW') and handle_packet.iTOW is not None) and
            (hasattr(handle_packet,'week') and handle_packet.week is not None)):
            if handle_packet.iTOW != packet.iTOW:
                raise ValueError(f"Unexpected packet iTOW: Expected {handle_packet.iTOW}, saw {packet.iTOW}, "
                                 f"packet type {packet.__class__.name()}")
            epochid,pre_exist=register_epoch(db,utc=handle_packet.utc, iTOW=handle_packet.iTOW, week=handle_packet.week)
            if not pre_exist:
                for epoch_packet in handle_packet.epoch_packets:
                    if hasattr(epoch_packet,'iTOW') and handle_packet.iTOW!=epoch_packet.iTOW:
                        raise ValueError("Packet has iTOW that doesn't match epoch: Expected {handle_packet.iTOW}, "
                                         f"saw {epoch_packet.iTOW}, packet type {packet.__class__.name()}")
                    epoch_packet.write(db,epochid=epochid,fileid=epoch_packet.fileid,ofs=epoch_packet.ofs)
        else:
            print("Incomplete epoch")
        handle_packet.epoch_packets=[]
        handle_packet.utc =None
        handle_packet.iTOW=None
        handle_packet.week=None


def plot_height(db):
    sql = 'select utc,hMSL,height,height-hMSL as undulation from nav_pvt where gnssfixok order by utc;'
    db.execute(sql)
    utcs, hMSLs, heights, undulations = [], [], [], []
    for utc, hMSL, height, undulation in db._cur:
        utcs.append(utc)
        hMSLs.append(hMSL)
        heights.append(height)
        undulations.append(undulation)
    print(utcs[0])
    plt.figure("height")
    plt.clf()
    plt.plot(utcs, hMSLs, label='MSL')
    plt.plot(utcs, heights, label='ellipsoid')
    plt.plot(utcs, undulations, label='undulation')
    plt.plot([utcs[0], utcs[-1]], [0, 0], 'b-', label='Reference surface')
    plt.plot([utcs[0], utcs[-1]], [21.18, 21.18], 'k--', label='Estimated deck 6 rail above waterline')
    plt.xlabel("UTC")
    plt.ylabel("height/m")
    plt.legend()
    plt.pause(1)


def main():
    dbname="Atlantic23_05"
    import_files=True
    do_plot=True
    drop=False
    profile=False
    with PostgresDatabase(user="jeppesen", password="Locking1blitz", database=dbname) as db:
        if import_files:
            with db.transaction():
                ensure_timeseries_tables(db,drop=drop)
                ensure_tables(db,drop=drop)
            seen_clsids={}
            infns=sorted(glob('/mnt/big/Atlantic23.05/Fluttershy/FluttershyBase/2023/05/13/*.ubx.bz2'))[0:1]
            for infn in infns:
                with db.transaction():
                    fileid=register_file_start(db,basename(infn))
                with db.transaction():
                    this_ofs=0
                    with smart_open(infn,"rb") as inf:
                        for packet in read_packet(inf):
                            next_ofs=inf.tell()
                            if type(packet)==str:
                                # json but failed to decode
                                print(packet)
                            elif type(packet)==dict:
                                # json that successfully decoded
                                #print(packet)
                                pass
                            elif hasattr(packet,'compiled_form'):
                                clsid=(packet.cls,packet.id)
                                if clsid not in seen_clsids:
                                    print(f"First time seeing {type(packet)} cls=0x{packet.cls:02x}, id=0x{packet.id:02x}")
                                    seen_clsids[clsid]=[True,0]
                                seen_clsids[clsid][1]+=1
                                if type(packet)==UBX_NAV_PVT:
                                    print(packet.utc)
                                handle_packet(db,fileid,this_ofs,packet)
                                this_ofs=next_ofs
                            elif type(packet)==packet.ublox_packet.UBloxPacket:
                                clsid=(packet.cls,packet.id)
                                if clsid not in seen_clsids:
                                    print(f"Unhandled packet cls=0x{packet.cls:02x}, id=0x{packet.id:02x}")
                                    seen_clsids[clsid]=[False,0]
                                seen_clsids[clsid][1]+=1
                with db.transaction():
                    register_file_finish(db, fileid)
                if do_plot:
                    plot_height(db)
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
            print(register_epoch.now-register_epoch.first)
            if profile:
                d = {k: (v[0], v[1]) for k, v in dict(sorted(db.profile.items(), key=lambda item: -item[1][1])).items()}
                print(d)
                for k,v in db.profile.items():
                    if "SELECT" in k.upper() or "INSERT" in k.upper():
                        plt.figure()
                        plt.title(k[0:50])
                        xfloat=np.array([x.total_seconds() for x in v[2]])
                        plt.plot(xfloat)
                        plt.xlabel(f"Total time: {np.sum(xfloat)}")
                plt.show()
        if do_plot:
            plot_height(db)
            plt.show()

if __name__=="__main__":
    main()