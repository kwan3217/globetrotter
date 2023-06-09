# GlobeTrotter
Code to ingest track data in various forms,
including GPS and AIS NMEA, and JPEG EXIF.
Once ingested, create KML and other visualizations
of the given data.

## read_ais.py
This code reads [AIS](https://en.wikipedia.org/wiki/Automatic_identification_system)
data in the form recorded by Shipometer 23.04
during expedition Atlantic 23.05, Miami to
Barcelona on the Disney Dream. It is specific
to the idiosyncracies of that dataset.

## track.py
Home to the `Track` class, indended to be a
common in-memory format for GPS, AIS, and KML
tracks. A track is a list of positions where
each position has a timetag, and perhaps other
information.

## exif2kml.py
Extract GPS data from EXIF tags of images,
and gather the data into a KML with placemarkers
pointing to each image in the appropriate place.