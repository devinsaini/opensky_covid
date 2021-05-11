import requests

opensky_files=[
    'flightlist_20191101_20191130.csv.gz',
    'flightlist_20191201_20191231.csv.gz',
    'flightlist_20200101_20200131.csv.gz',
    'flightlist_20200201_20200229.csv.gz',
    'flightlist_20200301_20200331.csv.gz',
    'flightlist_20200401_20200430.csv.gz',
    'flightlist_20200501_20200531.csv.gz',
    'flightlist_20200601_20200630.csv.gz',
    'flightlist_20200701_20200731.csv.gz',
    'flightlist_20200801_20200831.csv.gz',
    'flightlist_20200901_20200930.csv.gz',
    'flightlist_20201001_20201031.csv.gz',
    'flightlist_20201101_20201130.csv.gz',
    'flightlist_20201201_20201231.csv.gz',
    'flightlist_20210101_20210131.csv.gz',
    'flightlist_20210201_20210228.csv.gz',
    'flightlist_20210301_20210331.csv.gz',
    'flightlist_20210401_20210430.csv.gz',
    'LICENSE.txt',
    'readme.md'
]

# opensky files
for fname in opensky_files:
    url = f"https://zenodo.org/record/4737390/files/{fname}?download=1"
    r = requests.get(url, allow_redirects=True)
    open(f'data/opensky/{fname}', 'wb').write(r.content)

# JHU Enigma COVID-19 data
r = requests.get("https://covid19-lake.s3.us-east-2.amazonaws.com/enigma-jhu-timeseries/json/part-00000-23918a9c-a8b0-410e-a9d3-5772361fe925-c000.json", allow_redirects=True)
open(f'data/jhu_enigma/part-00000-23918a9c-a8b0-410e-a9d3-5772361fe925-c000.json', 'wb').write(r.content)

# download country IBAN data
r = requests.get("https://datahub.io/core/country-list/r/data.json", allow_redirects=True)
open(f'data/iban.json', 'wb').write(r.content)
