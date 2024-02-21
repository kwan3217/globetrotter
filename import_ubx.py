"""
Import gpsd pseudo-NMEA, which are created from the input binary datastream (in this case uBlox).
"""
import bz2
import gzip
import multiprocessing
import sys
import warnings
from functools import partial
from glob import glob
from os.path import basename

import numpy as np
from matplotlib import pyplot as plt
from database import Database
from database.postgres import PostgresDatabase
from packet.ublox.protocol_33_21 import packet_names, UBX_NAV_PVT, UBX_NAV_TIMEGPS, UBX_NAV_EOE, UBX_ESF_MEAS
from packet.ublox import ensure_tables, read_packet
from packet import register_file_start, register_file_finish, ensure_timeseries_tables, register_epoch
import packet.json # Don't use anything, just register it
import packet.nmea # likewise

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
                                 f"packet type {packet.__class__.__name__}")
            epochid,pre_exist=register_epoch(db,utc=handle_packet.utc, iTOW=handle_packet.iTOW, week=handle_packet.week)
            if not pre_exist:
                write_epoch=True
                for epoch_packet in handle_packet.epoch_packets:
                    if hasattr(epoch_packet,'iTOW') and handle_packet.iTOW!=epoch_packet.iTOW:
                        warnings.warn(f"Packet has iTOW that doesn't match epoch: Expected {handle_packet.iTOW}, "
                                      f"saw {epoch_packet.iTOW}, packet type {packet.__class__.__name__}")
                        write_epoch=False
                if write_epoch:
                    for epoch_packet in handle_packet.epoch_packets:
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


def plot_speed(db):
    sql = 'select utc,hMSL,veln,vele,veld from nav_pvt where gnssfixok order by utc;'
    db.execute(sql)
    utcs, hMSLs,velns, veles, velds = [], [], [], [], []
    for utc, hMSL, veln, vele, veld in db._cur:
        utcs.append(utc)
        hMSLs.append(hMSL)
        velns.append(veln)
        veles.append(vele)
        velds.append(veld)
    print(utcs[0])
    velns=np.array(velns)
    veles=np.array(veles)
    velds=np.array(velds)
    vels=np.sqrt(velns**2+veles**2+velds**2)
    plt.figure("height,speed")
    plt.clf()
    plt.subplot(121)
    plt.plot(utcs, hMSLs)
    plt.xlabel("UTC")
    plt.ylabel("hMSL/m")
    plt.subplot(122)
    plt.plot(utcs, vels)
    plt.xlabel("UTC")
    plt.ylabel("vel/(m/s)")
    plt.pause(1)


def import_ubx(infns:str|list[str],*,dbname:str="globetrotter",schema:str,
               host:str="192.168.217.102",port:int=5432,user:str="globetrotter",password:str="globetrotter",
               import_files:bool=True,do_plot:bool=True,drop:bool=True,profile:bool=False,do_ensure:bool):
    if type(infns)==str:
        infns=[infns]
    n_pvt = 0
    with PostgresDatabase(host=host,port=port,user=user, password=password, database=dbname) as db:
        with db.transaction():
            if drop:
                db.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            if do_ensure:
                db.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            db.execute(f"SET SEARCH_PATH={schema};")
        if import_files:
            if do_ensure:
                with db.transaction():
                    ensure_timeseries_tables(db,drop=drop)
                    ensure_tables(db,drop=drop)
            seen_clsids={}
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
                                    print('.',end='')
                                    n_pvt+=1
                                    if n_pvt%100==0:
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
                    plot_speed(db)
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
            try:
                print(register_epoch.now-register_epoch.first)
            except AttributeError:
                print("Couldn't print register_epoch.now (no packets processed?)")
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


def main():
    case=sys.argv[1]
    #case="Bahamas22_08"
    if case=="Colorado23_09":
        infns=sorted(
            #Colorado 23.09 debug case
            #glob('/mnt/big/Colorado23.09/FluttershyBase/2023/09/27/*00-40-01.ubx', recursive=True)
            #Colorado 23.09 all
            glob('/mnt/big/Colorado23.09/FluttershyBase/2023/09/2[789]/*.ubx.bz2', recursive=True) +
            glob('/mnt/big/Colorado23.09/FluttershyBase/2023/09/2[789]/*.ubx', recursive=True)
        )
    elif case=="Atlantic23_05":
        infns = sorted(glob('/mnt/big/Atlantic23.05/Fluttershy/FluttershyBase/2023/05/**/*.ubx.bz2',recursive=True)+
                       glob('/mnt/big/Atlantic23.05/Fluttershy/FluttershyBase/2023/05/**/*.ubx',recursive=True))
    elif case=="Bahamas22_08":
        infns = sorted(
            glob('/mnt/big/Bahamas22.08/FluttershyBase/**/*.ubx.bz2',recursive=True)+
            glob('/mnt/big/Bahamas22.08/FluttershyBase/**/*.ubx',recursive=True)
        )
    pool=multiprocessing.Pool()
    import_ubx(infns=[infns[0]],schema=case,drop=True,do_plot=False,do_ensure=True)
    pool.map(partial(import_ubx,schema=case,drop=False,do_plot=False,do_ensure=False),infns[1:])
    #import_ubx(infns=infns,schema=case,drop=False)

if __name__=="__main__":
    main()