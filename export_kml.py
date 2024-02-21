"""
Import gpsd pseudo-NMEA, which are created from the input binary datastream (in this case uBlox).
"""
from datetime import timedelta

from matplotlib import pyplot as plt
from database.postgres import PostgresDatabase

def export_track(db,oufn,diff:timedelta=timedelta(seconds=60)):
    sql = ('select epoch.utc, nav_pvt.lon, nav_pvt.lat, nav_pvt.hmsl '
           'from nav_pvt inner join epoch on nav_pvt.epoch=epoch.id '
           'where nav_pvt.gnssfixok order by epoch.utc;')
    db.execute(sql)
    utcss, lonss, latss, hmslss = [], [], [], []
    last_utc=None
    for utc, lon, lat, hmsl in db._cur:
        if last_utc is None or (utc-last_utc)>diff:
            utcss.append([])
            lonss.append([])
            latss.append([])
            hmslss.append([])
        last_utc=utc
        utcss[-1].append(utc)
        lonss[-1].append(lon)
        latss[-1].append(lat)
        hmslss[-1].append(hmsl)
    print(utcss[0][0])
    with open(oufn,"wt") as ouf:
        print(f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www.w3.org/2005/Atom">
<Document>
	<name>Dallas trip.kml</name>
	<Style id="multiTrack_n381">
		<IconStyle><Icon><href>http://earth.google.com/images/kml-icons/track-directional/track-0.png</href></Icon></IconStyle>
		<LineStyle><color>99ffac59</color><width>6</width></LineStyle>
	</Style>
	<Style id="multiTrack_h371">
		<IconStyle><scale>1.2</scale><Icon><href>http://earth.google.com/images/kml-icons/track-directional/track-0.png</href></Icon></IconStyle>
		<LineStyle><color>99ffac59</color><width>8</width></LineStyle>
	</Style>
	<StyleMap id="multiTrack381">
		<Pair><key>normal</key><styleUrl>#multiTrack_n381</styleUrl></Pair>
		<Pair><key>highlight</key><styleUrl>#multiTrack_h371</styleUrl></Pair>
	</StyleMap>
	<Folder>
		<name>{oufn}</name>
		<visibility>0</visibility>
""",file=ouf)
        for utcs,lons,lats,hmsls in zip(utcss,lonss,latss,hmslss):
            print(f"""		<Placemark>
			<name>{str(utcs[0])}</name>
			<visibility>0</visibility>
			<styleUrl>#multiTrack381</styleUrl>
			<gx:Track>""",file=ouf)
            for utc in utcs:
                utc=str(utc)
                utc=utc[0:10]+"T"+utc[11:26]+"Z"
                print(f"				<when>{utc}</when>",file=ouf)
            for lon,lat,hmsl in zip(lons,lats,hmsls):
                print(f"				<gx:coord>{lon} {lat} {hmsl}</gx:coord>",file=ouf)
            print(f"""        </gx:Track>
    </Placemark>""",file=ouf)
        print(f"""	</Folder>
</Document>
</kml>""",file=ouf)
    plt.figure("lon/lat")
    plt.clf()
    for lons,lats in zip(lonss,latss):
        plt.plot(lons,lats)
    plt.pause(1)


def export_kml(*,dbname:str="globetrotter",schema:str,
               host:str="192.168.217.102",port:int=5432,user:str="globetrotter",password:str="globetrotter",
               do_plot:bool=True):
    with PostgresDatabase(host=host,port=port,user=user, password=password, database=dbname) as db:
        with db.transaction():
            db.execute(f"SET SEARCH_PATH={schema};")
        if do_plot:
            export_track(db,schema+".kml")
            plt.show()


def main():
    case="Bahamas22_08"
    export_kml(schema=case)


if __name__=="__main__":
    main()