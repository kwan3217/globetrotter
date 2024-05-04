"""
From the MidCodes.csv file, make the FlagCodes.csv file. Populate the three-letter country code
or three-letter country with three-letter subdivision code as column 1, the country name as column 2,
column, and leave column 0 open to manually enter the two-letter code that will generate the correct
Unicode flag.

Created: 5/2/24
"""
from packet.ais import unicode_flag, mid_flag

flag_codes={
    "ALB"    :"AL",  # 🇦🇱Albania
    "AND"    :"AD",  # 🇦🇩Andorra
    "AUT"    :"AT",  # 🇦🇹Austria
    "PRT-AZO":""  ,  # Portugal - Azores
    "BEL"    :"BE",  # 🇧🇪Belgium
    "BLR"    :"BY",  # 🇧🇾Belarus
    "BGR"    :"BG",  # 🇧🇬Bulgaria
    "VAT"    :"VA",  # 🇻🇦Vatican City State
    "CYP"    :"CY",  # 🇨🇾Cyprus
    "GER"    :"DE",  # 🇩🇪Germany
    "GEO"    :"GE",  # 🇬🇪Georgia
    "MDA"    :"MD",  # 🇲🇩Moldova
    "MLT"    :"MT",  # 🇲🇹Malta
    "ARM"    :"AM",  # 🇦🇲Armenia
    "DNK"    :"DK",  # 🇩🇰Denmark
    "ESP"    :"ES",  # 🇪🇸Spain
    "FRA"    :"FR",  # 🇫🇷France
    "FIN"    :"FI",  # 🇫🇮Finland
    "DNK-FAE":"FO",  # 🇫🇴Denmark - Faroe Islands
    "GBR"    :"GB",  # 🇬🇧United Kingdom
    "GBR-GIB":"GI",  # 🇬🇮United Kingdom - Gibraltar
    "GRC"    :"GR",  # 🇬🇷Greece
    "HRV"    :"HR",  # 🇭🇷Croatia
    "MAR"    :"MA",  # 🇲🇦Morocco
    "HUN"    :"HU",  # 🇭🇺Hungary
    "NLD"    :"NL",  # 🇳🇱Netherlands
    "ITA"    :"IT",  # 🇮🇹Italy
    "IRL"    :"IE",  # 🇮🇪Ireland
    "ISL"    :"IS",  # 🇮🇸Iceland
    "LIE"    :"LI",  # 🇱🇮Liechtenstein
    "LUX"    :"LU",  # 🇱🇺Luxembourg
    "MCO"    :"MC",  # 🇲🇨Monaco
    "PRT-MAD":""  ,  # Portugal - Madeira
    "NOR"    :"NO",  # 🇳🇴Norway
    "POL"    :"PL",  # 🇵🇱Poland
    "MNE"    :"ME",  # 🇲🇪Montenegro
    "PRT"    :"PT",  # 🇵🇹Portugal
    "ROU"    :"RO",  # 🇷🇴Romania
    "SWE"    :"SE",  # 🇸🇪Sweden
    "SVK"    :"SK",  # 🇸🇰Slovak Republic
    "SMR"    :"SM",  # 🇸🇲San Marino
    "CHE"    :"CH",  # 🇨🇭Switzerland
    "CZE"    :"CZ",  # 🇨🇿Czech Republic
    "TUR"    :"TR",  # 🇹🇷Republic of Türkiye
    "UKR"    :"UA",  # 🇺🇦Ukraine
    "RUS"    :"RU",  # 🇷🇺Russian Federation
    "MKD"    :"MK",  # 🇲🇰North Macedonia
    "LVA"    :"LV",  # 🇱🇻Latvia
    "EST"    :"EE",  # 🇪🇪Estonia
    "LTU"    :"LT",  # 🇱🇹Lithuania
    "SVN"    :"SI",  # 🇸🇮Slovenia
    "GBR-ANG":"AI",  # 🇦🇮United Kingdom - Anguilla
    "USA-AK" :""  ,  # United States of America - Alaska
    "ATG"    :"AG",  # 🇦🇬Antigua and Barbuda
    "NLD-BES":"BQ",  # 🇧🇶Netherlands - Bonaire, Sint Eustatius and Saba
    "NLD-CW" :"CW",  # 🇨🇼Netherlands - Curaçao
    "NLD-SX" :"SX",  # 🇸🇽Netherlands - Sint Maarten (Dutch part)
    "NLD-ARB":"AW",  # 🇦🇼Netherlands - Aruba
    "BHS"    :"BS",  # 🇧🇸Bahamas
    "GBR-BER":"BM",  # 🇧🇲United Kingdom - Bermuda
    "BLZ"    :"BZ",  # 🇧🇿Belize
    "BRB"    :"BB",  # 🇧🇧Barbados
    "CAN"    :"CA",  # 🇨🇦Canada
    "GBR-CAY":"KY",  # 🇰🇾United Kingdom - Cayman Islands
    "CRI"    :"CR",  # 🇨🇷Costa Rica
    "CUB"    :"CU",  # 🇨🇺Cuba
    "DMA"    :"DM",  # 🇩🇲Dominica
    "DOM"    :"DO",  # 🇩🇴Dominican Republic
    "FRA-GUA":"GP",  # 🇬🇵France - Guadeloupe
    "GRD"    :"GD",  # 🇬🇩Grenada
    "DNK-GRL":"GL",  # 🇬🇱Denmark - Greenland
    "GTM"    :"GT",  # 🇬🇹Guatemala
    "HND"    :"HN",  # 🇭🇳Honduras
    "HTI"    :"HT",  # 🇭🇹Haiti
    "USA"    :"US",  # 🇺🇸United States of America
    "JAM"    :"JM",  # 🇯🇲Jamaica
    "KNA"    :"KN",  # 🇰🇳Saint Kitts and Nevis
    "LCA"    :"LC",  # 🇱🇨Saint Lucia
    "MEX"    :"MX",  # 🇲🇽Mexico
    "FRA-MAR":"MQ",  # 🇲🇶France - Martinique
    "GBR-MTS":"MS",  # 🇲🇸United Kingdom - Montserrat
    "NIC"    :"NI",  # 🇳🇮Nicaragua
    "PAN"    :"PA",  # 🇵🇦Panama
    "USA-PR" :"PR",  # 🇵🇷United States of America - Puerto Rico
    "SLV"    :"SV",  # 🇸🇻El Salvador
    "FRA-SPM":"PM",  # 🇵🇲France - Saint Pierre and Miquelon
    "TTO"    :"TT",  # 🇹🇹Trinidad and Tobago
    "GBR-TCI":"TC",  # 🇹🇨United Kingdom - Turks and Caicos Islands
    "VCT"    :"VC",  # 🇻🇨Saint Vincent and the Grenadines
    "GBR-BVI":"VG",  # 🇻🇬United Kingdom - British Virgin Islands
    "USA-VI" :"VI",  # 🇻🇮United States of America - United States Virgin Islands
    "AFG"    :"AF",  # 🇦🇫Afghanistan
    "SAU"    :"SA",  # 🇸🇦Saudi Arabia
    "BGD"    :"BD",  # 🇧🇩Bangladesh
    "BHR"    :"BH",  # 🇧🇭Bahrain
    "BTN"    :"BT",  # 🇧🇹Bhutan
    "CHN"    :"CN",  # 🇨🇳People's Republic of China
    "TWN"    :"TW",  # 🇹🇼Taiwan
    "LKA"    :"LK",  # 🇱🇰Sri Lanka
    "IND"    :"IN",  # 🇮🇳India
    "IRN"    :"IR",  # 🇮🇷Iran
    "AZE"    :"AZ",  # 🇦🇿Azerbaijan
    "IRQ"    :"IQ",  # 🇮🇶Iraq
    "ISR"    :"IL",  # 🇮🇱Israel
    "JPN"    :"JP",  # 🇯🇵Japan
    "TKM"    :"TM",  # 🇹🇲Turkmenistan
    "KAZ"    :"KZ",  # 🇰🇿Kazakhstan
    "UZB"    :"UZ",  # 🇺🇿Uzbekistan
    "JOR"    :"JO",  # 🇯🇴Jordan
    "KOR"    :"KR",  # 🇰🇷South Korea
    "PSE"    :"PS",  # 🇵🇸Palestine
    "PRK"    :"KP",  # 🇰🇵North Korea
    "KWT"    :"KW",  # 🇰🇼Kuwait
    "LBN"    :"LB",  # 🇱🇧Lebanon
    "KGZ"    :"KG",  # 🇰🇬Kyrgyzstan
    "CHN-MCO":"MO",  # 🇲🇴People's Republic of China - Macao
    "MDV"    :"MV",  # 🇲🇻Maldives
    "MNG"    :"MN",  # 🇲🇳Mongolia
    "NPL"    :"NP",  # 🇳🇵Nepal
    "OMN"    :"OM",  # 🇴🇲Oman
    "PAK"    :"PK",  # 🇵🇰Pakistan
    "QAT"    :"QA",  # 🇶🇦Qatar
    "SYR"    :"SY",  # 🇸🇾Syria
    "ARE"    :"AE",  # 🇦🇪United Arab Emirates
    "TJK"    :"TJ",  # 🇹🇯Tajikistan
    "YEM"    :"YE",  # 🇾🇪Yemen
    "CHN-HKG":"HK",  # 🇭🇰People's Republic of China - Hong Kong
    "BIH"    :"BA",  # 🇧🇦Bosnia and Herzegovina
    "FRA-ADE":""  ,  # France - Adelie Land
    "AUS"    :"AU",  # 🇦🇺Australia
    "MMR"    :"MM",  # 🇲🇲Myanmar
    "BRN"    :"BN",  # 🇧🇳Brunei
    "FSM"    :"FM",  # 🇫🇲Micronesia
    "PLW"    :"PW",  # 🇵🇼Palau (Republic of)
    "NZL"    :"NZ",  # 🇳🇿New Zealand
    "KHM"    :"KM",  # 🇰🇲Cambodia (Kingdom of)
    "AUS-CXR":"CX",  # 🇨🇽Australia - Christmas Island (Indian Ocean)
    "NZL-COK":"CK",  # 🇨🇰New Zealand - Cook Islands
    "FJI"    :"FJ",  # 🇫🇯Fiji (Republic of)
    "AUS-CCK":"CC",  # 🇨🇨Australia - Cocos (Keeling) Islands
    "IDN"    :"ID",  # 🇮🇩Indonesia (Republic of)
    "KIR"    :"KI",  # 🇰🇮Kiribati (Republic of)
    "LAO"    :"LA",  # 🇱🇦Laos
    "MYS"    :"MY",  # 🇲🇾Malaysia
    "USA-MP" :"MP",  # 🇲🇵United States of America - Northern Mariana Islands (Commonwealth of the)
    "MHL"    :"MH",  # 🇲🇭Marshall Islands (Republic of the)
    "FRA-NCL":"NC",  # 🇳🇨France - New Caledonia
    "NZL-NIU":"NU",  # 🇳🇺New Zealand - Niue
    "NRU"    :"NR",  # 🇳🇷Nauru (Republic of)
    "FRA-POL":"PF",  # 🇵🇫France - French Polynesia
    "PHL"    :"PH",  # 🇵🇭Philippines (Republic of the)
    "TLS"    :"TL",  # 🇹🇱Timor-Leste (Democratic Republic of)
    "PNG"    :"PG",  # 🇵🇬Papua New Guinea
    "GBR-PIT":"PN",  # 🇵🇳United Kingdom - Pitcairn Island
    "SLB"    :"SB",  # 🇸🇧Solomon Islands
    "USA-AS" :"AS",  # 🇦🇸United States of America - American Samoa
    "WSM"    :"WS",  # 🇼🇸Samoa (Independent State of)
    "SGP"    :"SG",  # 🇸🇬Singapore
    "THA"    :"TH",  # 🇹🇭Thailand
    "TON"    :"TO",  # 🇹🇴Tonga
    "TUV"    :"TV",  # 🇹🇻Tuvalu
    "VNM"    :"VN",  # 🇻🇳Viet Nam
    "VUT"    :"VU",  # 🇻🇺Vanuatu
    "FRA-WLF":"WF",  # 🇼🇫France - Wallis and Futuna Islands
    "ZAF"    :"ZA",  # 🇿🇦South Africa
    "AGO"    :"AO",  # 🇦🇴Angola
    "DZA"    :"DZ",  # 🇩🇿Algeria
    "FRA-SPA":""  ,  # France - Saint Paul and Amsterdam Islands
    "GBR-ASC":"AC",  # 🇦🇨United Kingdom - Ascension Island
    "BDI"    :"BI",  # 🇧🇮Burundi (Republic of)
    "BEN"    :"BJ",  # 🇧🇯Benin (Republic of)
    "BWA"    :"BW",  # 🇧🇼Botswana (Republic of)
    "CAF"    :"CF",  # 🇨🇫Central African Republic
    "CMR"    :"CM",  # 🇨🇲Cameroon (Republic of)
    "COD"    :"CD",  # 🇨🇩Congo (Republic of the)
    "COM"    :"KM",  # 🇰🇲Comoros (Union of the)
    "CPV"    :"CV",  # 🇨🇻Cabo Verde (Republic of)
    "FRA-CRO":""  ,  # France - Crozet Archipelago
    "CIV"    :"CI",  # 🇨🇮Côte d'Ivoire (Republic of)
    "DJI"    :"DJ",  # 🇩🇯Djibouti (Republic of)
    "EGY"    :"EG",  # 🇪🇬Egypt (Arab Republic of)
    "ETH"    :"ET",  # 🇪🇹Ethiopia (Federal Democratic Republic of)
    "ERI"    :"ER",  # 🇪🇷Eritrea
    "GAB"    :"GA",  # 🇬🇦Gabonese Republic
    "GHA"    :"GH",  # 🇬🇭Ghana
    "GMB"    :"GM",  # 🇬🇲Gambia (Republic of the)
    "GNB"    :"GW",  # 🇬🇼Guinea-Bissau (Republic of)
    "GNQ"    :"GQ",  # 🇬🇶Equatorial Guinea (Republic of)
    "GIN"    :"GN",  # 🇬🇳Guinea (Republic of)
    "BFA"    :"BF",  # 🇧🇫Burkina Faso
    "KEN"    :"KE",  # 🇰🇪Kenya
    "FRA-KER":""  ,  # France - Kerguelen Islands
    "LBR"    :"LR",  # 🇱🇷Liberia
    "SSD"    :"SS",  # 🇸🇸South Sudan
    "LIB"    :"LY",  # 🇱🇾Libya
    "LSO"    :"LS",  # 🇱🇸Lesotho
    "MUS"    :"MU",  # 🇲🇺Mauritius
    "MDG"    :"MG",  # 🇲🇬Madagascar
    "MLI"    :"ML",  # 🇲🇱Mali
    "MOZ"    :"MO",  # 🇲🇴Mozambique
    "MRT"    :"MR",  # 🇲🇷Mauritania
    "MWI"    :"MW",  # 🇲🇼Malawi
    "NER"    :"NE",  # 🇳🇪Niger
    "NGA"    :"NG",  # 🇳🇬Nigeria
    "NAM"    :"NA",  # 🇳🇦Namibia
    "FRA-REU":"RE",  # 🇷🇪France - Reunion
    "RWA"    :"RW",  # 🇷🇼Rwanda
    "SDN"    :"SD",  # 🇸🇩Sudan
    "SEN"    :"SN",  # 🇸🇳Senegal
    "SYC"    :"SY",  # 🇸🇾Seychelles
    "GBR-STH":"SH",  # 🇸🇭United Kingdom - Saint Helena
    "SOM"    :"SO",  # 🇸🇴Somalia
    "SLE"    :"SL",  # 🇸🇱Sierra Leone
    "STP"    :"ST",  # 🇸🇹Sao Tome and Principe
    "SWZ"    :"SZ",  # 🇸🇿Eswatini
    "TCD"    :"TD",  # 🇹🇩Chad
    "TGO"    :"TG",  # 🇹🇬Togolese Republic
    "TUN"    :"TN",  # 🇹🇳Tunisia
    "TZA"    :"TZ",  # 🇹🇿Tanzania (United Republic of)
    "UGA"    :"UG",  # 🇺🇬Uganda (Republic of)
    "ZMB"    :"ZM",  # 🇿🇲Zambia (Republic of)
    "ZWE"    :"ZW",  # 🇿🇼Zimbabwe (Republic of)
    "ARG"    :"AR",  # 🇦🇷Argentina
    "BRA"    :"BR",  # 🇧🇷Brazil (Federative Republic of)
    "BOL"    :"BO",  # 🇧🇴Bolivia (Plurinational State of)
    "CHL"    :"CL",  # 🇨🇱Chile
    "COL"    :"CO",  # 🇨🇴Colombia
    "ECU"    :"EC",  # 🇪🇨Ecuador
    "GBR-FKL":"FK",  # 🇫🇰United Kingdom - Falkland Islands
    "FRA-GUF":"GF",  # 🇬🇫France - Guiana
    "GUY"    :"GY",  # 🇬🇾Guyana
    "PRY"    :"PY",  # 🇵🇾Paraguay
    "PER"    :"PE",  # 🇵🇪Peru
    "SUR"    :"SR",  # 🇸🇷Suriname
    "URY"    :"UY",  # 🇺🇾Uruguay
    "VEN"    :"VE",  # 🇻🇪Venezuela
}


def main():
    print(mid_flag(211))
    print(mid_flag(231))
    with open("data/tables/MidCodes.csv","rt") as inf, open("data/tables/FlagCodes.csv","wt") as ouf:
        print("Flag code,Three-letter code,Country name",file=ouf)
        inf.readline()
        seen_codes=set()
        for inline in inf:
            MID,TLC,*name_parts=inline.strip().split(",")
            name=",".join(name_parts)
            if TLC=="#N/A":
                outline = f",,{name}"
                print(f'    ""       :""  , # {name}')
                print(outline,file=ouf)
            elif TLC not in seen_codes:
                seen_codes.add(TLC)
                outline=f"{flag_codes[TLC] if TLC in flag_codes else ''},{TLC},{name}"
                TLC_expanded=f'"{TLC}"'
                fc_expanded=f'"{flag_codes[TLC] if TLC in flag_codes else ""}"'
                print(f'    {TLC_expanded:9s}:{fc_expanded:4s},  # {unicode_flag(flag_codes[TLC]) if TLC in flag_codes and flag_codes[TLC]!="" else ""}{name}')
                print(outline,file=ouf)


if __name__ == "__main__":
    main()
