from database.postgres import PostgresDatabase


def main():
    with PostgresDatabase(host="192.168.217.102",port=5432,
                          user="globetrotter", password="globetrotter", database="globetrotter",
                          schema="atlantic23_05_ais",drop_schema=False,ensure_schema=False) as db:
        dream = 311042900
        for row in db.execute("select lon,lat,course,heading,speed,utc_xmit,utc_recv "
                              "from ais_3 "
                              "where mmsi=%s and (utc_xmit is not null or utc_recv is not null)",
                              (dream,)):
            print(row)


if __name__=="__main__":
    main()


