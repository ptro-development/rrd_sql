# Description:
RRD storage implemented in SQLite.


# Packages required to install:
```
yum install -y sqlite
```

# To store data:

```
./rrd.py temperature save 1460671800 40.6
./rrd.py temperature save 1460672000 35.6
./rrd.py temperature save 1460673050 13.6
./rrd.py temperature save 1460673160 NULL
```

# To display data:
```
./rrd.py temperature query minutes
./rrd.py temperature query hours
```
