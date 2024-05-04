Data which I have massaged and which *is* small enough to fit in github

* MidCodes.csv - table of [Maritime Identification Digits](https://en.wikipedia.org/wiki/Maritime_identification_digits) (three-digit codes)
  which each map to one country and authoritatively identify
  what flag the ship is or should be flying. If a country
  uses up its available ship codes with a given MID, it can
  be assigned more codes, so a country can be represented by more than
  one row in this table. Consists of three columns - MID, 
  three-letter code (or three-letter code followed by a dash and a 
  subdivision code), country name. Some subdivisions have their own
  MIDs - these will always be identified by a three-letter code with a dash
  and a name split by a dash surrounded by spaces. Only subdivisions
  which register ships get a row -- IE Gibraltar does (GBR-GIB) but
  Florida (USA-FL) does not.
* FlagCodes.csv - Table of Unicode flag codes. Unicode can represent
  the flags of all soverign nations and some subdivisions -- for instance,
  Gibraltar does but Alaska does not. If a subdivision has a flag, it will 
  have its own two-letter code representing itself, otherwise the code is blank.
  In this file, a subdivision is indicated by a three-letter code with a hyphen
  dividing the three-letter code of the soverign parent from the code for the
  subdivsion. The file is the two-letter code, the three-letter code, and a comment
  (usually the name of the country)