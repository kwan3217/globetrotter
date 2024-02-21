#from datasystem.python_lib.log import log
#log("import numpy as np")
import numpy as np
import matplotlib.image as mpimg
#log("import matplotlib.pyplot as plt")
import matplotlib.pyplot as plt

#from datasystem import ANC
log=print
ANC='./'

def dms2rad(d,m,s):
    return np.radians(d+m/60+s/3600)

def ll2xyz(*,lat_rad:float=None,lon_rad:float=None,
             lat_deg:float=None,lon_deg:float=None):
    if lat_rad is None:
        lat_rad=np.deg2rad(lat_deg)
    if lon_rad is None:
        lon_rad=np.deg2rad(lon_deg)
    clat=np.cos(lat_rad)
    clon=np.cos(lon_rad)
    slat=np.sin(lat_rad)
    slon=np.sin(lon_rad)
    return np.array((clat*clon,clat*slon,slat))

def xyz2ll(xyz):
    nxyz=xyz/np.linalg.norm(xyz)
    return (np.arcsin(nxyz[2]),np.arctan2(nxyz[1],nxyz[0]))

def rq2ll(lat0,lon0,r,q):
    lat=np.arcsin(np.sin(lat0)*np.cos(r)+np.cos(lat0)*np.sin(r)*np.cos(q))
    lon=np.arctan2(np.sin(q)*np.sin(r)*np.cos(lat0),np.cos(r)-np.sin(lat0)*np.sin(lat))+lon0
    lon[lon>np.pi]-=2*np.pi
    lon[lon<-np.pi]+=2*np.pi
    return lat,lon

def ll2rq(lat0,lon0,lat,lon):
    r=np.arccos(np.sin(lat0)*np.sin(lat)+np.cos(lat0)*np.cos(lat)*np.cos(lon-lon0))
    q=np.arctan2(np.sin(lon-lon0)*np.cos(lat),np.cos(lat0)*np.sin(lat)-np.sin(lat0)*np.cos(lat)*np.cos(lon-lon0))
    try:
        q[q<0]+=2*np.pi
        q[q>2*np.pi]-=2*np.pi
    except:
        if(q<0):
            q+=2*np.pi
        elif(q>2*np.pi):
            q+=2*np.pi
    return (r,q)

def rq2xy(r,q,xsize,ysize,scl=3/np.pi,rot=0):
    x=r*np.sin(q-rot)
    x*=scl*xsize/2
    x=xsize//2+x
    y=r*np.cos(q-rot)
    y*=scl*xsize/2 #Yes, use xsize, because this makes it the same scale even if the image is not a square
    y=ysize//2-y
    try:
        x[x<0]=0
        x[x>=xsize]=xsize-1
    except:
        if(x<0):
            x=0
        elif(x>=xsize):
            x=xsize-1
    try:
        y[y<0]=0
        y[y>=ysize]=ysize-1
    except:
        if(y<0):
            y=0
        elif(y>=ysize):
            y=ysize-1
    return (x,y)

def xy2rq(x,y,xsize,ysize,scl=3/np.pi,rot=0):
    r=np.sqrt((x-xsize/2)**2+(ysize/2-y)**2)/(scl*xsize/2)
    q=np.arctan2(x-xsize/2,(ysize/2-y))+rot
    q[q<0]+=2*np.pi
    q[q>2*np.pi]-=2*np.pi
    return (r,q)

atlantic_waypoints={"HOIST":(dms2rad(-57,- 0   ,-0),dms2rad(55, 2,0)),
               "JANJO":(dms2rad(-57,- 0   ,-0),dms2rad(54, 2,0)),
               "KODIK":(dms2rad(-57,-12   , 0),dms2rad(53,28,0)),
               "LOMSI":(dms2rad(-56,-47   ,-0),dms2rad(53, 6,0)),
               "MELDI":(dms2rad(-56,-21   ,-0),dms2rad(52,44,0)),
               "NEEKO":(dms2rad(-55,-50   ,-0),dms2rad(52,24,0)),
               "RIKAL":(dms2rad(-54,-32   ,-0),dms2rad(51,48,0)),
               "TUDEP":(dms2rad(-53,-14   ,-0),dms2rad(51,10,0)),
               "SOORY":(dms2rad(-60,-16.05,-0),dms2rad(38,30,0)),
               "PIKIL":(dms2rad(-14, -0   ,-0),dms2rad(56, 0,0)),
               "SOVED":(dms2rad(-14,- 0   ,-0),dms2rad(56, 0,0))}


def calc_mid(*,lat0_rad:float,lon0_rad:float,
               lat1_rad:float,lon1_rad:float):
    """

    :param lat0_rad:
    :param lon0_rad:
    :param lat1_rad:
    :param lon1_rad:
    :return: Tuple of:
      * midpoint latitude in radians
      * midpoint longitude in radians
      * rotation in radians
    """
    xyz0=ll2xyz(lat0_rad,lon0_rad)
    xyz1=ll2xyz(lat1_rad,lon1_rad)
    (latm_rad,lonm_rad)=xyz2ll((xyz0+xyz1)/2)
    (_,rot_rad)=ll2rq(latm_rad,lonm_rad,lat1_rad,lon1_rad)
    rot_rad-=np.pi/2
    if(rot_rad>np.pi/2):
        rot_rad+=np.pi
    return latm_rad,lonm_rad,rot_rad


def project_map(*,xsize:int=2000,ysize:int=1000,map_name:str="world.topo.bathy.200405.3x21600x10800.png",
                  latm_rad:float=None,lonm_rad:float=None,rot_rad:float=None):
    log("Loading Earth map")
    Map=np.flipud(mpimg.imread(map_name).astype(np.float32))
    #Set up the destination image
    log("Calculating projection")
    x=np.arange(0,xsize).reshape(1,-1)
    y=np.arange(0,ysize).reshape(-1,1)
    (r,q)=xy2rq(x,y,xsize=xsize,ysize=ysize,rot=rot_rad)
    (lat_rad,lon_rad)=rq2ll(latm_rad,lonm_rad,r,q)
    lat_deg=np.rad2deg(lat_rad)
    lon_deg=np.rad2deg(lon_rad)
    xpix=((lon_deg+180)*(Map.shape[1]-1)/360).astype(np.int16)
    ypix=((lat_deg+90)*(Map.shape[0]-1)/180).astype(np.int16)
    xpix[xpix>Map.shape[1]-1]=Map.shape[1]-1
    xpix[xpix<0]=0
    ypix[ypix>Map.shape[0]-1]=Map.shape[0]-1
    ypix[ypix<0]=0
    img=Map[ypix,xpix,:]
    return img


def globetrot(lat0=dms2rad(42, 21, 47),
              #Logan Airport from Wikipedia
              lon0=dms2rad(-71,- 0,-23),
              #Dubai Airport from Wikipedia
              lat1=dms2rad( 25, 15, 10),
              lon1=dms2rad( 55, 21, 52),
              xsize=2000,
              ysize=1000,
              tracks=None
              ):
    """

    :param lat0: Latitude of starting point in radians, default to Boston Logan airport
    :param lon0: Longitude of starting point in radians
    :param lat1: Latitude of ending point in radians, default to Dubai International airport
    :param lon1: Longitude of end point in radians
    :param xsize: Horizontal size of projected map in pixels
    :param ysize: Vertical size of projected map in pixels
    :param trackfns:
    :return:
    """
    """
    Conventions:
    
    x - Horizontal pixel coordinate in final image. Ranges from 0 (left) to xsize-1 (top)
    y - Vertical   pixel coordinate in final image. ranges from 0 (top) to ysize-1 (bottom)
    img - final image. A 2D image has indices [y,x] and a 3D color image has indices [y,x,color]
    r - distance from center of projection in radians
    q - bearing from center of projection in radians, ranges from [0,2*pi). 0=north, pi/2(90deg)=east, pi(180deg)=south, 3pi/2(270deg)=west
    lat - latitude in radians, ranges from [-pi/2,pi/2]
    lon - longitude in radians, ranges from [-pi,pi)
    rot - angle that the map is rotated, such that the destination is directly to the right of the center. OR, with rot=0, no rotation is performed, which
          results in the north pole being directly above the center. Positive rot rotate the map counterclockwise
          relative to rot=0
          
    Which means that each pixel has three coordinates:
       x,y - pixel coordinate from the top
       r,q - distance and bearing from center. Bearing takes rot into account.
       lat,lon -   
    """
    #rot=1
    #Now, use latm and lonm as the center of an azimuthal equidistant map
    #Normalize map such that brightest point is 1.0 . This normalizes across
    #all channels 
    #Map/=255
    #log("Loading track")
    #tracklat = []
    #tracklon = []
    #for trackfn in trackfns:
    #    if ".nmea" in trackfn[:-5]:
    #        track=parse_nmea.parse(trackfn)
    #    else:
    #        track=kml_pos.parse(trackfn)
    #        track=track[0][4]
    #    for time in track:
    #        tracklat.append(track[time][0])
    #        tracklon.append(track[time][1])
    #    tracklat.append(float('NaN'))
    #    tracklon.append(float('NaN'))
    latm,lonm,rot=calc_mid(lat0_rad=lat0,lon0_rad=lon0,
                           lat1_rad=lat1,lon1_rad=lon1)
    img=project_map()
    plt.imshow(img)
    (r0,q0)=ll2rq(latm,lonm,lat0,lon0)
    (r1,q1)=ll2rq(latm,lonm,lat1,lon1)
    (x,y)=rq2xy(np.array([r0,r1]),np.array([q0,q1]),xsize=xsize,ysize=ysize,rot=rot)
    plt.plot(x,y,'k-')
    for i in range(24):
        (r,q)=ll2rq(latm,lonm,np.radians(np.arange(-90,91)),np.radians(i*15))
        (x,y)=rq2xy(r,q,xsize=xsize,ysize=ysize,rot=rot)
        plt.plot(x,y,'b')
    for i in range(12):
        (r,q)=ll2rq(latm,lonm,np.radians((i-6)*15),np.radians(np.arange(0,361)))
        (x,y)=rq2xy(r,q,xsize=xsize,ysize=ysize,rot=rot)
        plt.plot(x,y,'b')
    (r,q)=ll2rq(latm,lonm,np.radians(np.array(tracklat)),np.radians(np.array(tracklon)))
    (x,y)=rq2xy(r,q,xsize=xsize,ysize=ysize,rot=rot)
    plt.plot(x,y,'r+-')
    plt.axis([0,xsize,ysize,0])
    plt.axis('off')
    plt.axis('equal')    
    #for name in waypoints:
    #    (r, q) = ll2rq(latm, lonm, waypoints[name][1], waypoints[name][0])
    #    (x, y) = rq2xy(r, q, xsize=xsize, ysize=ysize, rot=rot)
    #    plt.plot(x,y,'y+')
    #    plt.text(x,y,name)
    plt.show()
    pass

if __name__ == "__main__":
    #globetrot(
    #    # KDEN Airport from Wikipedia
    #    lat0=dms2rad( 39, 51, 42),
    #    lon0=dms2rad(-104,-40,-23),
    #    #PHNL Airport from Wikipedia
    #    lat1=dms2rad(  21, 19,  7),
    #    lon1=dms2rad(-157,-55,-21),
    #    trackfn="/home/jeppesen/Downloads/FlightAwareLastWeek/FlightAware_UAL181_EDDF_KDEN_20190906.kml"
    #)
    root = "/mnt/pinkiepie/home/chrisj/Dubai19.09"
    globetrot(
        # KDEN Airport from Wikipedia
        lat0=dms2rad( 39, 51, 42),
        lon0=dms2rad(-104,-40,-23),
        # OMDB Airport from Wikipedia
        lat1=dms2rad(  25, 15, 10),
        lon1=dms2rad(  55, 21, 52),
        trackfns=[
            f"{root}/FlightAware_UAL182_KDEN_EDDF_20190922.kml",
            f"{root}/FlightAware_DLH630_EDDF_OMDB_20190923.kml",
            f"{root}/FlightAware_DLH631_OMDB_EDDF_20190926.kml",
            f"{root}/FlightAware_UAL181_EDDF_KDEN_20190927.kml"
        ]
    )
