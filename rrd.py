#!/usr/bin/python

import sqlite3
import sys
import os

from datetime import datetime


class RRDData(object):

    _TIME_INTERVALS = [
        "60_60",
        "24_3600"]

    _CREATE_TABLE = "CREATE TABLE IF NOT EXISTS {name}\
(ID INT PRIMARY KEY NOT NULL,\
TIME INT NOT NULL,\
VALUE REAL);"

    _QUERY_ALL_DATA = "SELECT * FROM {name} ORDER BY TIME;"
    _QUERY_OLDEST_DATA = "SELECT min(ID),TIME,VALUE FROM {name} \
WHERE VALUE IS NOT NULL;"

    _INSERT_DATA = "INSERT OR REPLACE INTO {name} (ID,TIME,VALUE) \
VALUES({id},{time},{value})"

    def __init__(self, name):
        self.name = name
        self.connection = self._get_db_connection()
        for interval in self._TIME_INTERVALS:
            table_name = name + "_" + interval
            self._create_rrd_table(table_name)

    def _get_db_connection(self):
        return sqlite3.connect(self.name)

    def _create_rrd_table(self, name):
        sql = self._CREATE_TABLE.format(name=name)
        self.connection.execute(sql)
        self.connection.commit()

    def _get_table_name(self, appendix):
        return self.name + "_" + appendix

    def _save(self, table_appendix, pid, timestamp, value):
        sql = self._INSERT_DATA.format(
            name=self._get_table_name(table_appendix), id=pid,
            time=timestamp, value=value)
        # print sql
        self.connection.execute(sql)
        self.connection.commit()

    def _query(self, table_appendix):
        sql = self._QUERY_ALL_DATA.format(
            name=self._get_table_name(table_appendix))
        # print sql
        cursor = self.connection.execute(sql)
        values = []
        for row in cursor:
            print "%s, %s, %s" % (row[0], row[1], row[2])
            if row[2]:
                values.append(row[2])
        print "\nMin:(%s) Max(%s) Avg(%s)" % (
            min(values), max(values), sum(values)/len(values))

    def _get_lowest_record(self):
        sql = self._QUERY_OLDEST_DATA.format(
            name=self._get_table_name("60_60"))
        # print sql
        cursor = self.connection.execute(sql)
        row = cursor.next()
        return row[0], row[1], row[2]

    def save(self, timestamp, data):
        minute = datetime.fromtimestamp(timestamp).minute
        self._save("60_60", minute, timestamp, data)
        idm, last_timestamp, value = self._get_lowest_record()
        hour = datetime.fromtimestamp(last_timestamp).hour
        self._save("24_3600", hour, last_timestamp, value)

    def query(self, interval="minutes"):
        if interval == "minutes":
            self._query("60_60")
        elif interval == "hours":
            self._query("24_3600")

    def close_connection(self):
        self.connection.close()


def print_help():
    sys.stderr.write("""Wrong rrd command format.\n
It should be:
./rrd.py key command timestamp value
./rrd.py key command [minutes|hours]

Examples:
./rrd.py temperature save 1460674067 23.6
./rrd.py temperature save 1460674067 NULL
./rrd.py temperature query minutes
./rrd.py temperature query hours
""")


def main():
    if len(sys.argv) not in [4, 5]:
        print_help()
        sys.exit(1)

    key_name = sys.argv[1]
    command = sys.argv[2]
    if not os.path.isfile(key_name) and command == "query":
        sys.stderr.write("RRD file %s does not exist !!!\n" % key_name)
        sys.exit(1)
    timestamp_or_interval = sys.argv[3]
    if len(sys.argv) == 5:
        if sys.argv[4] == "NULL":
            value = sys.argv[4]
        else:
            value = float(sys.argv[4])

    # save value ?
    if command == "save" and timestamp_or_interval and value and key_name:
        rrd_data = RRDData(name=key_name)
        rrd_data.save(int(timestamp_or_interval), value)
        rrd_data.close_connection()
    # query value ?
    elif command == "query" and timestamp_or_interval in ["hours", "minutes"]:
        print "Printing %s:\n" % timestamp_or_interval
        rrd_data = RRDData(name=key_name)
        rrd_data.query(timestamp_or_interval)
        rrd_data.close_connection()
    else:
        print_help()
        sys.exit(1)

    sys.exit(0)

if __name__ == "__main__":
    main()
