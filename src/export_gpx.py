"""
Import gpsd pseudo-NMEA, which are created from the input binary datastream (in this case uBlox).
"""
import sys
from datetime import timedelta

from matplotlib import pyplot as plt
from database.postgres import PostgresDatabase

def export_track(db,oufn,diff:timedelta=timedelta(seconds=60),max_lines=None):
    sql = ('select epoch.utc, nav_hpposllh.lon, nav_hpposllh.lat, nav_hpposllh.hmsl '
           'from nav_hpposllh inner join epoch on nav_hpposllh.epoch=epoch.id '
           'inner join nav_pvt on nav_hpposllh.epoch=nav_pvt.epoch '
           'where nav_pvt.gnssfixok and nav_pvt.gspeed>0 order by epoch.utc;')
    db.execute(sql)
    if max_lines is not None:
        i_file=0
        parts=oufn.split('.')
        oufn=f"{'.'.join(parts[:-1])}_{i_file:02d}.{parts[-1]}"
    ouf=open(oufn,"wt")
    header=f"""<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
<gpx xmlns="http://www.topografix.com/GPX/1/1" creator="export_gpx.py">
	<trk><name>%s</name>
		<trkseg>"""
    footer=f"""        </trkseg>
	</trk></gpx>"""
    print(header%(oufn),file=ouf)
    last_utc = None
    latss=[[]]
    lonss=[[]]
    i_lines=0
    for utc, lon, lat, hmsl in db._cur:
        if last_utc is not None and (utc - last_utc) > diff:
            latss.append([])
            lonss.append([])
            print("		</trkseg>",file=ouf)
            print("		<trkseg>",file=ouf)
        last_utc = utc
        utc=str(utc)
        utc=utc[0:10]+"T"+utc[11:26]+"Z"
        latss[-1].append(lat)
        lonss[-1].append(lon)
        print(f'	<trkpt lat="{lat}" lon="{lon}"><ele>{hmsl}</ele><time>{utc}</time></trkpt>',file=ouf)
        i_lines+=1
        if max_lines is not None and i_lines>max_lines:
            i_lines=0
            print(footer,file=ouf)
            ouf.close()
            i_file+=1
            oufn = f"{'.'.join(parts[:-1])}_{i_file:02d}.{parts[-1]}"
            ouf = open(oufn,"wt")
            print(header%(oufn),file=ouf)
    print(footer,file=ouf)
    ouf.close()
    plt.figure("lon/lat")
    plt.clf()
    for lons,lats in zip(lonss,latss):
        plt.plot(lons,lats)
    plt.pause(1)


def export_gpx(*,dbname:str="globetrotter",schema:str,
               host:str="192.168.217.102",port:int=5432,user:str="globetrotter",password:str="globetrotter",
               do_plot:bool=True,max_lines:int=None):
    with PostgresDatabase(host=host,port=port,user=user, password=password, database=dbname) as db:
        with db.transaction():
            db.execute(f"SET SEARCH_PATH={schema};")
        if do_plot:
            export_track(db,schema+".gpx",max_lines=max_lines)
            plt.show()


def main():
    case=sys.argv[1]
    export_gpx(schema=case,max_lines=200_000)


if __name__=="__main__":
    main()