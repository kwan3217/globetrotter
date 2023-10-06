def save_path(track:list[dict],oufn:str,shipname:str):
    """

    :param track:
    :param oufn:
    :return:
    """
    with open(oufn,"wt") as ouf:
        print(fr"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www.w3.org/2005/Atom">
<Document>
	<name>{shipname}</name>
	<Style id="s_ylw-pushpin">
		<IconStyle>
			<scale>1.1</scale>
			<Icon>
				<href>http://maps.google.com/mapfiles/kml/pushpin/ylw-pushpin.png</href>
			</Icon>
			<hotSpot x="20" y="2" xunits="pixels" yunits="pixels"/>
		</IconStyle>
	</Style>
	<StyleMap id="m_ylw-pushpin">
		<Pair>
			<key>normal</key>
			<styleUrl>#s_ylw-pushpin</styleUrl>
		</Pair>
		<Pair>
			<key>highlight</key>
			<styleUrl>#s_ylw-pushpin_hl</styleUrl>
		</Pair>
	</StyleMap>
	<Style id="s_ylw-pushpin_hl">
		<IconStyle>
			<scale>1.3</scale>
			<Icon>
				<href>http://maps.google.com/mapfiles/kml/pushpin/ylw-pushpin.png</href>
			</Icon>
			<hotSpot x="20" y="2" xunits="pixels" yunits="pixels"/>
		</IconStyle>
	</Style>
	<Placemark>
		<name>Untitled Path</name>
		<styleUrl>#m_ylw-pushpin</styleUrl>
		<LineString>
			<tessellate>1</tessellate>
			<coordinates>""",file=ouf)
        for msg in track:
            print(f"{msg['lon']},{msg['lat']},0",file=ouf)
        print(r"""			</coordinates>
		</LineString>
	</Placemark>
</Document>
</kml>
""",file=ouf)


colors=[
    "000000",
    "aa5500",
    "ff0000",
    "ffaa00",
    "ffff00",
    "00ff00",
    "0000ff",
    "aa00ff",
    "888888",
    "ffffff"
]


def save_track(dts:list[datetime],track:list[dict],oufn:str,shipname:str,i_day:int):
    """

    :param track:
    :param oufn:
    :return:
    """
    d={}
    for dt,msg in zip(dts,track):
        d[dt]=msg
    sorted_dts=sorted(d.keys())
    with open(oufn,"wt") as ouf:
        print(fr"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www.w3.org/2005/Atom">
<Document>
	<name>{basename(oufn)}</name>
	<Style id="multiTrack_n">
		<IconStyle>
			<Icon>
				<href>http://earth.google.com/images/kml-icons/track-directional/track-0.png</href>
			</Icon>
		</IconStyle>
		<LineStyle>
			<color>99{colors[i_day%10][::-1]}</color>
			<width>6</width>
		</LineStyle>
	</Style>
	<Style id="multiTrack_h">
		<IconStyle>
			<scale>1.2</scale>
			<Icon>
				<href>http://earth.google.com/images/kml-icons/track-directional/track-0.png</href>
			</Icon>
		</IconStyle>
		<LineStyle>
			<color>99{colors[i_day%10][::-1]}</color>
			<width>8</width>
		</LineStyle>
	</Style>
	<StyleMap id="multiTrack">
		<Pair>
			<key>normal</key>
			<styleUrl>#multiTrack_n</styleUrl>
		</Pair>
		<Pair>
			<key>highlight</key>
			<styleUrl>#multiTrack_h</styleUrl>
		</Pair>
	</StyleMap>
	<Placemark>
		<name>{shipname}</name>
		<styleUrl>#multiTrack</styleUrl>
		<gx:balloonVisibility>1</gx:balloonVisibility>
		<gx:Track>
    		<gx:altitudeMode>clampToSeaFloor</gx:altitudeMode>""",file=ouf)
        for dt in sorted_dts:
            print(f"			<when>{dt.isoformat()[0:19]}Z</when>",file=ouf)
        for dt in sorted_dts:
            msg=d[dt]
            print(f"			<gx:coord>{msg['lon']} {msg['lat']} 0</gx:coord>",file=ouf)
        print(r"""		</gx:Track>
	</Placemark>
</Document>
</kml>""",file=ouf)

