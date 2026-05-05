"""Weather station data and ensemble forecast fetching.

STATIONS contiene las 64 estaciones meteorologicas que Polymarket usa para resolver
mercados de temperatura. Cada entrada tiene coordenadas exactas del aeropuerto/estacion
de resolucion (NO del centro urbano), ICAO code, URL de Wunderground, timezone y unidad.

Fuente de resolucion oficial de Polymarket: Wunderground (https://www.wunderground.com).
Excepcion: Hong Kong usa weather.gov.hk. Moscow/Istanbul/Tel Aviv usan NOAA.

Forecast: Open-Meteo Ensemble API (GFS, 31 miembros), gratuito y sin API key.
"""

from __future__ import annotations

import logging
import statistics
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger("nachomarket.data.weather")

# ---------------------------------------------------------------------------
# STATIONS — 64 estaciones verificadas contra las reglas de Polymarket
# "verified": True = confirmado scrapeando la pagina del mercado en Polymarket
# "verified": False = inferido por patron, pendiente verificacion
# ---------------------------------------------------------------------------

STATIONS: Dict[str, dict] = {
    # === USA (20 ciudades, Fahrenheit) ===
    "nyc": {
        "city_name": "New York",
        "lat": 40.7772, "lon": -73.8726,
        "icao": "KLGA",
        "station_name": "LaGuardia Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA",
        "unit": "F", "timezone": "America/New_York",
        "verified": True,
    },
    "chicago": {
        "city_name": "Chicago",
        "lat": 41.9742, "lon": -87.9073,
        "icao": "KORD",
        "station_name": "Chicago O'Hare Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/il/chicago/KORD",
        "unit": "F", "timezone": "America/Chicago",
        "verified": True,
    },
    "miami": {
        "city_name": "Miami",
        "lat": 25.7959, "lon": -80.2870,
        "icao": "KMIA",
        "station_name": "Miami Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/fl/miami/KMIA",
        "unit": "F", "timezone": "America/New_York",
        "verified": True,
    },
    "dallas": {
        "city_name": "Dallas",
        "lat": 32.8471, "lon": -96.8518,
        "icao": "KDAL",
        "station_name": "Dallas Love Field",
        "wunderground": "https://www.wunderground.com/history/daily/us/tx/dallas/KDAL",
        "unit": "F", "timezone": "America/Chicago",
        "verified": True,
    },
    "seattle": {
        "city_name": "Seattle",
        "lat": 47.4502, "lon": -122.3088,
        "icao": "KSEA",
        "station_name": "Seattle-Tacoma Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/wa/seatac/KSEA",
        "unit": "F", "timezone": "America/Los_Angeles",
        "verified": True,
    },
    "atlanta": {
        "city_name": "Atlanta",
        "lat": 33.6407, "lon": -84.4277,
        "icao": "KATL",
        "station_name": "Hartsfield-Jackson Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/ga/atlanta/KATL",
        "unit": "F", "timezone": "America/New_York",
        "verified": True,
    },
    "los-angeles": {
        "city_name": "Los Angeles",
        "lat": 33.9425, "lon": -118.4081,
        "icao": "KLAX",
        "station_name": "Los Angeles Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/ca/los-angeles/KLAX",
        "unit": "F", "timezone": "America/Los_Angeles",
        "verified": True,
    },
    "denver": {
        "city_name": "Denver",
        "lat": 39.7017, "lon": -104.7515,
        "icao": "KBKF",
        "station_name": "Buckley Space Force Base",
        "wunderground": "https://www.wunderground.com/history/daily/us/co/aurora/KBKF",
        "unit": "F", "timezone": "America/Denver",
        "verified": True,
    },
    "houston": {
        "city_name": "Houston",
        "lat": 29.6454, "lon": -95.2789,
        "icao": "KHOU",
        "station_name": "William P. Hobby Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/tx/houston/KHOU",
        "unit": "F", "timezone": "America/Chicago",
        "verified": True,
    },
    "austin": {
        "city_name": "Austin",
        "lat": 30.1945, "lon": -97.6699,
        "icao": "KAUS",
        "station_name": "Austin-Bergstrom Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/tx/austin/KAUS",
        "unit": "F", "timezone": "America/Chicago",
        "verified": True,
    },
    "san-francisco": {
        "city_name": "San Francisco",
        "lat": 37.6189, "lon": -122.3750,
        "icao": "KSFO",
        "station_name": "San Francisco Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/ca/san-francisco/KSFO",
        "unit": "F", "timezone": "America/Los_Angeles",
        "verified": True,
    },
    "boston": {
        "city_name": "Boston",
        "lat": 42.3656, "lon": -71.0096,
        "icao": "KBOS",
        "station_name": "Logan Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/ma/boston/KBOS",
        "unit": "F", "timezone": "America/New_York",
        "verified": False,
    },
    "phoenix": {
        "city_name": "Phoenix",
        "lat": 33.4343, "lon": -112.0116,
        "icao": "KPHX",
        "station_name": "Phoenix Sky Harbor Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/az/phoenix/KPHX",
        "unit": "F", "timezone": "America/Phoenix",
        "verified": False,
    },
    "las-vegas": {
        "city_name": "Las Vegas",
        "lat": 36.0840, "lon": -115.1537,
        "icao": "KLAS",
        "station_name": "Harry Reid Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/nv/las-vegas/KLAS",
        "unit": "F", "timezone": "America/Los_Angeles",
        "verified": False,
    },
    "minneapolis": {
        "city_name": "Minneapolis",
        "lat": 44.8820, "lon": -93.2218,
        "icao": "KMSP",
        "station_name": "Minneapolis-St Paul Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/mn/minneapolis/KMSP",
        "unit": "F", "timezone": "America/Chicago",
        "verified": False,
    },
    "philadelphia": {
        "city_name": "Philadelphia",
        "lat": 39.8722, "lon": -75.2409,
        "icao": "KPHL",
        "station_name": "Philadelphia Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/pa/philadelphia/KPHL",
        "unit": "F", "timezone": "America/New_York",
        "verified": False,
    },
    "washington-dc": {
        "city_name": "Washington DC",
        "lat": 38.8521, "lon": -77.0377,
        "icao": "KDCA",
        "station_name": "Ronald Reagan Washington National Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/va/arlington/KDCA",
        "unit": "F", "timezone": "America/New_York",
        "verified": False,
    },
    "san-antonio": {
        "city_name": "San Antonio",
        "lat": 29.5337, "lon": -98.4698,
        "icao": "KSAT",
        "station_name": "San Antonio Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/tx/san-antonio/KSAT",
        "unit": "F", "timezone": "America/Chicago",
        "verified": False,
    },
    "new-orleans": {
        "city_name": "New Orleans",
        "lat": 29.9934, "lon": -90.2580,
        "icao": "KMSY",
        "station_name": "Louis Armstrong New Orleans Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/la/kenner/KMSY",
        "unit": "F", "timezone": "America/Chicago",
        "verified": False,
    },
    "oklahoma-city": {
        "city_name": "Oklahoma City",
        "lat": 35.3931, "lon": -97.6007,
        "icao": "KOKC",
        "station_name": "Will Rogers World Airport",
        "wunderground": "https://www.wunderground.com/history/daily/us/ok/oklahoma-city/KOKC",
        "unit": "F", "timezone": "America/Chicago",
        "verified": False,
    },
    # === Europa (14 ciudades, Celsius) ===
    "london": {
        "city_name": "London",
        "lat": 51.5048, "lon": 0.0495,
        "icao": "EGLC",
        "station_name": "London City Airport",
        "wunderground": "https://www.wunderground.com/history/daily/gb/london/EGLC",
        "unit": "C", "timezone": "Europe/London",
        "verified": True,
    },
    "paris": {
        "city_name": "Paris",
        "lat": 48.9694, "lon": 2.4414,
        "icao": "LFPB",
        "station_name": "Paris-Le Bourget Airport",
        "wunderground": "https://www.wunderground.com/history/daily/fr/bonneuil-en-france/LFPB",
        "unit": "C", "timezone": "Europe/Paris",
        "verified": True,
    },
    "madrid": {
        "city_name": "Madrid",
        "lat": 40.4719, "lon": -3.5626,
        "icao": "LEMD",
        "station_name": "Adolfo Suarez Madrid-Barajas Airport",
        "wunderground": "https://www.wunderground.com/history/daily/es/madrid/LEMD",
        "unit": "C", "timezone": "Europe/Madrid",
        "verified": True,
    },
    "berlin": {
        "city_name": "Berlin",
        "lat": 52.3667, "lon": 13.5033,
        "icao": "EDDB",
        "station_name": "Berlin Brandenburg Airport",
        "wunderground": "https://www.wunderground.com/history/daily/de/berlin/EDDB",
        "unit": "C", "timezone": "Europe/Berlin",
        "verified": False,
    },
    "amsterdam": {
        "city_name": "Amsterdam",
        "lat": 52.3086, "lon": 4.7639,
        "icao": "EHAM",
        "station_name": "Amsterdam Airport Schiphol",
        "wunderground": "https://www.wunderground.com/history/daily/nl/schiphol/EHAM",
        "unit": "C", "timezone": "Europe/Amsterdam",
        "verified": True,
    },
    "milan": {
        "city_name": "Milan",
        "lat": 45.6306, "lon": 8.7281,
        "icao": "LIMC",
        "station_name": "Malpensa Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/it/milan/LIMC",
        "unit": "C", "timezone": "Europe/Rome",
        "verified": True,
    },
    "munich": {
        "city_name": "Munich",
        "lat": 48.3537, "lon": 11.7750,
        "icao": "EDDM",
        "station_name": "Munich Airport",
        "wunderground": "https://www.wunderground.com/history/daily/de/munich/EDDM",
        "unit": "C", "timezone": "Europe/Berlin",
        "verified": True,
    },
    "warsaw": {
        "city_name": "Warsaw",
        "lat": 52.1657, "lon": 20.9671,
        "icao": "EPWA",
        "station_name": "Warsaw Chopin Airport",
        "wunderground": "https://www.wunderground.com/history/daily/pl/warsaw/EPWA",
        "unit": "C", "timezone": "Europe/Warsaw",
        "verified": True,
    },
    "helsinki": {
        "city_name": "Helsinki",
        "lat": 60.3172, "lon": 24.9633,
        "icao": "EFHK",
        "station_name": "Helsinki Vantaa Airport",
        "wunderground": "https://www.wunderground.com/history/daily/fi/vantaa/EFHK",
        "unit": "C", "timezone": "Europe/Helsinki",
        "verified": True,
    },
    "moscow": {
        "city_name": "Moscow",
        "lat": 55.5917, "lon": 37.2617,
        "icao": "UUWW",
        "station_name": "Vnukovo Intl Airport",
        "wunderground": "https://www.weather.gov/wrh/timeseries?site=UUWW",
        "unit": "C", "timezone": "Europe/Moscow",
        "verified": True,
    },
    "ankara": {
        "city_name": "Ankara",
        "lat": 40.1281, "lon": 32.9951,
        "icao": "LTAC",
        "station_name": "Esenboga Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/tr/cubuk/LTAC",
        "unit": "C", "timezone": "Europe/Istanbul",
        "verified": True,
    },
    "istanbul": {
        "city_name": "Istanbul",
        "lat": 41.2611, "lon": 28.7420,
        "icao": "LTFM",
        "station_name": "Istanbul Airport",
        "wunderground": "https://www.weather.gov/wrh/timeseries?site=LTFM",
        "unit": "C", "timezone": "Europe/Istanbul",
        "verified": True,
    },
    "tel-aviv": {
        "city_name": "Tel Aviv",
        "lat": 32.0114, "lon": 34.8867,
        "icao": "LLBG",
        "station_name": "Ben Gurion Intl Airport",
        "wunderground": "https://www.weather.gov/wrh/timeseries?site=LLBG",
        "unit": "C", "timezone": "Asia/Jerusalem",
        "verified": True,
    },
    # === Asia (21 ciudades, Celsius) ===
    "tokyo": {
        "city_name": "Tokyo",
        "lat": 35.7647, "lon": 140.3864,
        "icao": "RJTT",
        "station_name": "Tokyo Haneda Airport",
        "wunderground": "https://www.wunderground.com/history/daily/jp/tokyo/RJTT",
        "unit": "C", "timezone": "Asia/Tokyo",
        "verified": True,
    },
    "seoul": {
        "city_name": "Seoul",
        "lat": 37.4691, "lon": 126.4505,
        "icao": "RKSI",
        "station_name": "Incheon Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/kr/incheon/RKSI",
        "unit": "C", "timezone": "Asia/Seoul",
        "verified": True,
    },
    "hong-kong": {
        "city_name": "Hong Kong",
        "lat": 22.3020, "lon": 114.1743,
        "icao": "",
        "station_name": "Hong Kong Observatory",
        "wunderground": "",
        "unit": "C", "timezone": "Asia/Hong_Kong",
        "verified": True,
    },
    "singapore": {
        "city_name": "Singapore",
        "lat": 1.3502, "lon": 103.9940,
        "icao": "WSSS",
        "station_name": "Singapore Changi Airport",
        "wunderground": "https://www.wunderground.com/history/daily/sg/singapore/WSSS",
        "unit": "C", "timezone": "Asia/Singapore",
        "verified": True,
    },
    "beijing": {
        "city_name": "Beijing",
        "lat": 40.0801, "lon": 116.5846,
        "icao": "ZBAA",
        "station_name": "Beijing Capital Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/cn/beijing/ZBAA",
        "unit": "C", "timezone": "Asia/Shanghai",
        "verified": True,
    },
    "shanghai": {
        "city_name": "Shanghai",
        "lat": 31.1443, "lon": 121.8083,
        "icao": "ZSPD",
        "station_name": "Shanghai Pudong Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/cn/shanghai/ZSPD",
        "unit": "C", "timezone": "Asia/Shanghai",
        "verified": True,
    },
    "taipei": {
        "city_name": "Taipei",
        "lat": 25.0697, "lon": 121.5517,
        "icao": "RCSS",
        "station_name": "Taipei Songshan Airport",
        "wunderground": "https://www.wunderground.com/history/daily/tw/taipei/RCSS",
        "unit": "C", "timezone": "Asia/Taipei",
        "verified": True,
    },
    "shenzhen": {
        "city_name": "Shenzhen",
        "lat": 22.6393, "lon": 113.8107,
        "icao": "ZGSZ",
        "station_name": "Shenzhen Bao'an Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/cn/shenzhen/ZGSZ",
        "unit": "C", "timezone": "Asia/Shanghai",
        "verified": True,
    },
    "chengdu": {
        "city_name": "Chengdu",
        "lat": 30.5785, "lon": 103.9471,
        "icao": "ZUUU",
        "station_name": "Chengdu Shuangliu Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/cn/chengdu/ZUUU",
        "unit": "C", "timezone": "Asia/Shanghai",
        "verified": True,
    },
    "chongqing": {
        "city_name": "Chongqing",
        "lat": 29.7192, "lon": 106.6417,
        "icao": "ZUCK",
        "station_name": "Chongqing Jiangbei Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/cn/chongqing/ZUCK",
        "unit": "C", "timezone": "Asia/Shanghai",
        "verified": True,
    },
    "wuhan": {
        "city_name": "Wuhan",
        "lat": 30.7838, "lon": 114.2081,
        "icao": "ZHHH",
        "station_name": "Wuhan Tianhe Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/cn/wuhan/ZHHH",
        "unit": "C", "timezone": "Asia/Shanghai",
        "verified": True,
    },
    "qingdao": {
        "city_name": "Qingdao",
        "lat": 36.2661, "lon": 120.3842,
        "icao": "ZSQD",
        "station_name": "Qingdao Jiaodong Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/cn/qingdao/ZSQD",
        "unit": "C", "timezone": "Asia/Shanghai",
        "verified": True,
    },
    "guangzhou": {
        "city_name": "Guangzhou",
        "lat": 23.3924, "lon": 113.2990,
        "icao": "ZGGG",
        "station_name": "Guangzhou Baiyun Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/cn/guangzhou/ZGGG",
        "unit": "C", "timezone": "Asia/Shanghai",
        "verified": True,
    },
    "manila": {
        "city_name": "Manila",
        "lat": 14.5086, "lon": 121.0196,
        "icao": "RPLL",
        "station_name": "Ninoy Aquino Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/ph/manila/RPLL",
        "unit": "C", "timezone": "Asia/Manila",
        "verified": True,
    },
    "jakarta": {
        "city_name": "Jakarta",
        "lat": -6.2666, "lon": 106.8911,
        "icao": "WIHH",
        "station_name": "Halim Perdanakusuma Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/id/jakarta/WIHH",
        "unit": "C", "timezone": "Asia/Jakarta",
        "verified": True,
    },
    "kuala-lumpur": {
        "city_name": "Kuala Lumpur",
        "lat": 2.7456, "lon": 101.7099,
        "icao": "WMKK",
        "station_name": "Kuala Lumpur Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/my/sepang-district/WMKK",
        "unit": "C", "timezone": "Asia/Kuala_Lumpur",
        "verified": True,
    },
    "jeddah": {
        "city_name": "Jeddah",
        "lat": 21.6796, "lon": 39.1565,
        "icao": "OEJN",
        "station_name": "King Abdulaziz Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/sa/jeddah/OEJN",
        "unit": "C", "timezone": "Asia/Riyadh",
        "verified": True,
    },
    "karachi": {
        "city_name": "Karachi",
        "lat": 24.9065, "lon": 67.1608,
        "icao": "OPKC",
        "station_name": "Jinnah Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/pk/karachi/OPKC",
        "unit": "C", "timezone": "Asia/Karachi",
        "verified": True,
    },
    "delhi": {
        "city_name": "Delhi",
        "lat": 28.5686, "lon": 77.1126,
        "icao": "VIDP",
        "station_name": "Indira Gandhi Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/in/delhi/VIDP",
        "unit": "C", "timezone": "Asia/Kolkata",
        "verified": False,
    },
    "lucknow": {
        "city_name": "Lucknow",
        "lat": 26.7606, "lon": 80.8893,
        "icao": "VILK",
        "station_name": "Chaudhary Charan Singh Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/in/lucknow/VILK",
        "unit": "C", "timezone": "Asia/Kolkata",
        "verified": True,
    },
    "busan": {
        "city_name": "Busan",
        "lat": 35.1796, "lon": 128.9382,
        "icao": "RKPK",
        "station_name": "Gimhae Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/kr/busan/RKPK",
        "unit": "C", "timezone": "Asia/Seoul",
        "verified": True,
    },
    # === Latinoamerica (4 ciudades, Celsius) ===
    "buenos-aires": {
        "city_name": "Buenos Aires",
        "lat": -34.8222, "lon": -58.5358,
        "icao": "SAEZ",
        "station_name": "Ministro Pistarini Intl Airport (Ezeiza)",
        "wunderground": "https://www.wunderground.com/history/daily/ar/ezeiza/SAEZ",
        "unit": "C", "timezone": "America/Argentina/Buenos_Aires",
        "verified": True,
    },
    "mexico-city": {
        "city_name": "Mexico City",
        "lat": 19.4363, "lon": -99.0721,
        "icao": "MMMX",
        "station_name": "Benito Juarez Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/mx/mexico-city/MMMX",
        "unit": "C", "timezone": "America/Mexico_City",
        "verified": True,
    },
    "sao-paulo": {
        "city_name": "Sao Paulo",
        "lat": -23.4356, "lon": -46.4731,
        "icao": "SBGR",
        "station_name": "Sao Paulo-Guarulhos Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/br/guarulhos/SBGR",
        "unit": "C", "timezone": "America/Sao_Paulo",
        "verified": True,
    },
    "panama-city": {
        "city_name": "Panama City",
        "lat": 8.9733, "lon": -79.5556,
        "icao": "MPMG",
        "station_name": "Marcos A. Gelabert Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/pa/panama-city/MPMG",
        "unit": "C", "timezone": "America/Panama",
        "verified": True,
    },
    # === Otros (6 ciudades, Celsius) ===
    "toronto": {
        "city_name": "Toronto",
        "lat": 43.6772, "lon": -79.6306,
        "icao": "CYYZ",
        "station_name": "Toronto Pearson Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/ca/mississauga/CYYZ",
        "unit": "C", "timezone": "America/Toronto",
        "verified": True,
    },
    "lagos": {
        "city_name": "Lagos",
        "lat": 6.5774, "lon": 3.3212,
        "icao": "DNMM",
        "station_name": "Murtala Muhammad Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/ng/lagos/DNMM",
        "unit": "C", "timezone": "Africa/Lagos",
        "verified": True,
    },
    "cape-town": {
        "city_name": "Cape Town",
        "lat": -33.9648, "lon": 18.6017,
        "icao": "FACT",
        "station_name": "Cape Town Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/za/matroosfontein/FACT",
        "unit": "C", "timezone": "Africa/Johannesburg",
        "verified": True,
    },
    "wellington": {
        "city_name": "Wellington",
        "lat": -41.3272, "lon": 174.8052,
        "icao": "NZWN",
        "station_name": "Wellington Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/nz/wellington/NZWN",
        "unit": "C", "timezone": "Pacific/Auckland",
        "verified": True,
    },
    "sydney": {
        "city_name": "Sydney",
        "lat": -33.9461, "lon": 151.1772,
        "icao": "YSSY",
        "station_name": "Sydney Kingsford Smith Intl Airport",
        "wunderground": "https://www.wunderground.com/history/daily/au/sydney/YSSY",
        "unit": "C", "timezone": "Australia/Sydney",
        "verified": False,
    },
}

# ---------------------------------------------------------------------------
# Helpers de lookup
# ---------------------------------------------------------------------------

# Cache: (city_name).lower() -> station_key
_station_alias_cache: Dict[str, str] = {}
for _key, _data in STATIONS.items():
    _station_alias_cache[_key.lower()] = _key
    _station_alias_cache[_data["city_name"].lower()] = _key

MONTH_MAP: Dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def resolve_station(city_name: str) -> dict | None:
    """Resuelve una ciudad a su estacion meteorologica de Polymarket.

    Busca en STATIONS por key exacta, luego por city_name fuzzy match.
    Si no encuentra, intenta geocoding como fallback (NO confiable — devuelve
    centro urbano, no el aeropuerto de resolucion).
    """
    key = city_name.lower().replace(" ", "-")
    if key in STATIONS:
        return STATIONS[key]
    # Fuzzy match por key alias
    alias_key = city_name.lower()
    if alias_key in _station_alias_cache:
        return STATIONS[_station_alias_cache[alias_key]]
    # Geocoding fallback (ultimo recurso, NO confiable para trading)
    logger.error(
        "STATION_NOT_FOUND: '%s' no esta en STATIONS. "
        "Usando geocoding (NO confiable — centro urbano, no aeropuerto de resolucion). "
        "Verifica la pagina del mercado en Polymarket para la estacion correcta.",
        city_name,
    )
    geo = geocode_city(city_name)
    if geo:
        return {
            "city_name": geo["name"],
            "lat": geo["lat"],
            "lon": geo["lon"],
            "icao": "",
            "station_name": "UNKNOWN (geocoded - NOT the resolution station)",
            "wunderground": "",
            "unit": "C",
            "timezone": "UTC",
            "verified": False,
        }
    return None


def get_station(city_key: str) -> dict | None:
    """Busca directa por key de STATIONS (ej: 'nyc', 'london')."""
    return STATIONS.get(city_key)


def get_wunderground_url(city_key: str) -> str:
    """URL de Wunderground para verificacion post-resolucion."""
    station = STATIONS.get(city_key, {})
    return station.get("wunderground", "")


# ---------------------------------------------------------------------------
# EnsembleForecast dataclass
# ---------------------------------------------------------------------------


@dataclass
class EnsembleForecast:
    """Ensemble weather forecast with per-member temperature data."""

    city_key: str
    city_name: str
    target_date: date
    member_highs: List[float]
    member_lows: List[float]
    mean_high: float = 0.0
    std_high: float = 0.0
    mean_low: float = 0.0
    std_low: float = 0.0
    num_members: int = 0
    fetched_at: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if self.member_highs:
            self.mean_high = statistics.mean(self.member_highs)
            self.std_high = (
                statistics.stdev(self.member_highs)
                if len(self.member_highs) > 1
                else 0.0
            )
            self.num_members = len(self.member_highs)
        if self.member_lows:
            self.mean_low = statistics.mean(self.member_lows)
            self.std_low = (
                statistics.stdev(self.member_lows)
                if len(self.member_lows) > 1
                else 0.0
            )

    def probability_high_above(self, threshold_f: float) -> float:
        if not self.member_highs:
            return 0.5
        count = sum(1 for h in self.member_highs if h > threshold_f)
        return count / len(self.member_highs)

    def probability_high_below(self, threshold_f: float) -> float:
        return 1.0 - self.probability_high_above(threshold_f)

    def probability_low_above(self, threshold_f: float) -> float:
        if not self.member_lows:
            return 0.5
        count = sum(1 for l in self.member_lows if l > threshold_f)
        return count / len(self.member_lows)

    def probability_low_below(self, threshold_f: float) -> float:
        return 1.0 - self.probability_low_above(threshold_f)

    @property
    def ensemble_agreement(self) -> float:
        if not self.member_highs:
            return 0.5
        median = statistics.median(self.member_highs)
        above = sum(1 for h in self.member_highs if h > median)
        frac = above / len(self.member_highs)
        return max(frac, 1.0 - frac)


# ---------------------------------------------------------------------------
# Caches
# ---------------------------------------------------------------------------

_forecast_cache: Dict[str, tuple[float, "EnsembleForecast"]] = {}
_CACHE_TTL = 900  # 15 minutos

_geo_cache: Dict[str, dict] = {}


def geocode_city(city_name: str) -> dict | None:
    """Resuelve coordenadas via Open-Meteo Geocoding API (solo fallback)."""
    if city_name in _geo_cache:
        return _geo_cache[city_name]
    try:
        r = requests.get(
            "https://geocoding-api.open-meteo.com/v1/search",
            params={"name": city_name, "count": 1, "language": "en"},
            timeout=10.0,
        )
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        if results:
            city = {
                "name": results[0].get("name", city_name),
                "lat": float(results[0].get("latitude", 0)),
                "lon": float(results[0].get("longitude", 0)),
                "country": results[0].get("country", ""),
            }
            _geo_cache[city_name] = city
            return city
    except Exception as e:
        logger.warning("Geocoding failed for %s: %s", city_name, e)
    return None


# ---------------------------------------------------------------------------
# Forecast fetching
# ---------------------------------------------------------------------------


def _celsius_to_fahrenheit(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


def _local_day_utc_range(station: dict, target_date: date) -> tuple[date, date]:
    """Devuelve el rango de fechas UTC que cubre el dia local completo de la estacion.

    Open-Meteo agrega datos diarios por fecha UTC, pero la temperatura maxima
    diaria se mide en el dia calendario local de la estacion.
    Si el dia local cruza el limite UTC, necesitamos consultar 2 fechas UTC.

    Ejemplo: NYC (America/New_York, UTC-4 en EDT) para May 5:
    Local May 5 = 2026-05-05T04:00Z a 2026-05-06T04:00Z
    → retorna (2026-05-05, 2026-05-06)
    """
    tz_name = station.get("timezone", "UTC")
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        return target_date, target_date

    local_start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=tz)
    utc_start = local_start.astimezone(timezone.utc)
    utc_end_moment = utc_start + timedelta(days=1) - timedelta(seconds=1)
    return utc_start.date(), utc_end_moment.date()


def fetch_ensemble_forecast(
    city_name: str, target_date: Optional[date] = None
) -> Optional["EnsembleForecast"]:
    """Obtiene forecast del ensemble GFS (31 miembros) para la estacion de resolucion.

    Usa las coordenadas exactas de STATIONS (aeropuerto, no centro urbano).
    Convierte Celsius a Fahrenheit internamente si la estacion usa Celsius,
    para que el resto de la estrategia siempre trabaje en Fahrenheit.

    Args:
        city_name: Nombre de ciudad o key de STATIONS (ej: 'New York', 'nyc').
        target_date: Fecha objetivo. Default: hoy.

    Returns:
        EnsembleForecast con temps en Fahrenheit, o None si falla.
    """
    if target_date is None:
        target_date = date.today()

    station = resolve_station(city_name)
    if station is None:
        logger.warning("City not found: %s", city_name)
        return None

    cache_key = f"{station.get('icao', city_name.lower())}_{target_date.isoformat()}"
    now = time.time()
    if cache_key in _forecast_cache:
        cached_time, cached_forecast = _forecast_cache[cache_key]
        if now - cached_time < _CACHE_TTL:
            return cached_forecast

    if not station.get("verified", False):
        logger.warning(
            "UNVERIFIED station for %s (%s): coords and ICAO not confirmed "
            "against Polymarket rules. Verify at first active market.",
            city_name, station.get("station_name", "?"),
        )

    # Timezone-aware date range: ensure we cover the full local calendar day
    utc_start, utc_end = _local_day_utc_range(station, target_date)
    dates_to_query = [utc_start]
    if utc_end != utc_start:
        dates_to_query.append(utc_end)

    try:
        all_member_highs: dict[str, list[float]] = {}
        all_member_lows: dict[str, list[float]] = {}

        for query_date in dates_to_query:
            params = {
                "latitude": station["lat"],
                "longitude": station["lon"],
                "daily": "temperature_2m_max,temperature_2m_min",
                "temperature_unit": "fahrenheit",
                "start_date": query_date.isoformat(),
                "end_date": query_date.isoformat(),
                "models": "gfs_seamless",
            }

            data = None
            for attempt in range(3):
                try:
                    response = requests.get(
                        "https://ensemble-api.open-meteo.com/v1/ensemble",
                        params=params,
                        timeout=15.0,
                    )
                    response.raise_for_status()
                    data = response.json()
                    break
                except requests.RequestException:
                    if attempt < 2:
                        time.sleep(1.0 * (attempt + 1))
                    else:
                        raise

            if data is None:
                continue

            daily = data.get("daily", {})
            for key, values in daily.items():
                if not isinstance(values, list) or not values:
                    continue
                val = values[0]
                if val is None:
                    continue
                if key == "temperature_2m_max" or key.startswith("temperature_2m_max_member"):
                    all_member_highs.setdefault(key, []).append(float(val))
                elif key == "temperature_2m_min" or key.startswith("temperature_2m_min_member"):
                    all_member_lows.setdefault(key, []).append(float(val))

        # Combine: daily max = max across UTC dates, daily min = min
        member_highs: list[float] = [max(vals) for vals in all_member_highs.values() if vals]
        member_lows: list[float] = [min(vals) for vals in all_member_lows.values() if vals]

        if not member_highs:
            logger.warning("No ensemble data for %s on %s", station["city_name"], target_date)
            return None

        forecast = EnsembleForecast(
            city_key=station.get("icao", city_name.lower()),
            city_name=station["city_name"],
            target_date=target_date,
            member_highs=member_highs,
            member_lows=member_lows,
        )

        _forecast_cache[cache_key] = (now, forecast)
        logger.info(
            "Ensemble forecast for %s (%s) on %s: High %.1fF +/- %.1fF (%d members)",
            station["city_name"],
            station.get("icao", "?"),
            target_date,
            forecast.mean_high,
            forecast.std_high,
            forecast.num_members,
        )

        return forecast

    except Exception as e:
        logger.warning("Failed to fetch ensemble forecast for %s: %s", city_name, e)
        return None


# ---------------------------------------------------------------------------
# Observed temperature (NWS — US stations only, legacy)
# ---------------------------------------------------------------------------


def fetch_nws_observed_temperature(
    city_key: str, target_date: Optional[date] = None
) -> Optional[Dict[str, float]]:
    """Obtiene temperatura observada real desde NWS API.

    Solo funciona para estaciones US con ICAO valido en STATIONS.
    """
    station = STATIONS.get(city_key)
    if station is None:
        return None
    icao = station.get("icao", "")
    if not icao or not icao.startswith("K"):
        return None

    if target_date is None:
        target_date = date.today()

    try:
        url = f"https://api.weather.gov/stations/{icao}/observations"
        headers = {"User-Agent": "(nachomarket-weather-bot, contact@example.com)"}

        start = datetime.combine(target_date, datetime.min.time()).isoformat() + "Z"
        end = datetime.combine(
            target_date + timedelta(days=1), datetime.min.time()
        ).isoformat() + "Z"

        response = requests.get(
            url, params={"start": start, "end": end}, headers=headers, timeout=15.0
        )
        response.raise_for_status()
        data = response.json()

        features = data.get("features", [])
        if not features:
            return None

        temps: list[float] = []
        for obs in features:
            props = obs.get("properties", {})
            temp_c = props.get("temperature", {}).get("value")
            if temp_c is not None:
                temps.append(_celsius_to_fahrenheit(float(temp_c)))

        if not temps:
            return None

        return {"high": max(temps), "low": min(temps)}

    except Exception as e:
        logger.warning("Failed to fetch NWS observations for %s: %s", city_key, e)
        return None


# ---------------------------------------------------------------------------
# Observed temperature (global) via Open-Meteo Archive API (ERA5 reanalysis)
# ---------------------------------------------------------------------------


def fetch_observed_temperature(
    city_key: str, target_date: Optional[date] = None
) -> Optional[Dict[str, float]]:
    """Obtiene temperatura observada real desde Open-Meteo Archive (ERA5).

    Funciona globalmente, sin API key. Datos de reanalisis ERA5 (~5 dias de rezago).
    Para fechas muy recientes devuelve None.

    Args:
        city_key: Key de STATIONS (ej: 'nyc', 'london').
        target_date: Fecha objetivo. Default: hoy.

    Returns:
        Dict con 'high' y 'low' en Fahrenheit, o None si no disponible.
    """
    station = STATIONS.get(city_key)
    if station is None:
        return None

    if target_date is None:
        target_date = date.today()

    try:
        params = {
            "latitude": station["lat"],
            "longitude": station["lon"],
            "daily": "temperature_2m_max,temperature_2m_min",
            "temperature_unit": "fahrenheit",
            "start_date": target_date.isoformat(),
            "end_date": target_date.isoformat(),
        }

        response = requests.get(
            "https://archive-api.open-meteo.com/v1/archive",
            params=params,
            timeout=10.0,
        )
        response.raise_for_status()
        data = response.json()

        daily = data.get("daily", {})
        highs = daily.get("temperature_2m_max", [])
        lows = daily.get("temperature_2m_min", [])

        if not highs:
            return None

        return {"high": float(highs[0]), "low": float(lows[0] or highs[0])}

    except Exception as e:
        logger.debug(
            "Archive temp not available for %s on %s: %s",
            city_key, target_date, e,
        )
        return None


# ---------------------------------------------------------------------------
# NWS point forecast — US stations pre-trade cross-check
# ---------------------------------------------------------------------------


def fetch_nws_point_forecast(city_key: str) -> dict | None:
    """Obtiene forecast oficial del NWS para las coordenadas exactas del aeropuerto.

    Usa la API gratuita de weather.gov. El endpoint /points/{lat},{lon}
    redirige al gridpoint forecast oficial de esa ubicacion.

    Solo funciona para estaciones US (ICAO starts with 'K').

    Returns:
        {
            "today": {"high": float, "low": float},
            "tomorrow": {"high": float, "low": float},
            "station": "KLGA",
        } or None
    """
    station = STATIONS.get(city_key)
    if station is None:
        return None
    icao = station.get("icao", "")
    if not icao or not icao.startswith("K"):
        return None

    try:
        headers = {"User-Agent": "(nachomarket-weather-bot, contact@example.com)"}

        # Step 1: Resolve lat/lon to gridpoint
        points_url = (
            f"https://api.weather.gov/points/"
            f"{station['lat']:.4f},{station['lon']:.4f}"
        )
        r = requests.get(points_url, headers=headers, timeout=10.0)
        r.raise_for_status()
        points_data = r.json()
        forecast_url = points_data.get("properties", {}).get("forecast", "")
        if not forecast_url:
            return None

        # Step 2: Fetch daily forecast
        r = requests.get(forecast_url, headers=headers, timeout=10.0)
        r.raise_for_status()
        forecast_data = r.json()
        periods = forecast_data.get("properties", {}).get("periods", [])
        if not periods:
            return None

        # NWS returns 12h periods: "Today", "Tonight", "Tomorrow", "Tomorrow Night", ...
        # Extract max temp from "day" periods, min from "night" periods
        result: dict = {"today": {}, "tomorrow": {}, "station": icao}
        for period in periods:
            name = period.get("name", "").lower()
            temp = float(period.get("temperature", 0))
            is_daytime = period.get("isDaytime", False)

            if "today" in name:
                if is_daytime:
                    result["today"]["high"] = temp
                else:
                    result["today"]["low"] = temp
            elif "tomorrow" in name:
                if is_daytime:
                    result["tomorrow"]["high"] = temp
                else:
                    result["tomorrow"]["low"] = temp

        if "high" not in result["today"] and "high" not in result["tomorrow"]:
            return None

        return result

    except Exception as e:
        logger.debug("NWS forecast unavailable for %s: %s", city_key, e)
        return None
