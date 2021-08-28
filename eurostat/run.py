#!/usr/bin/env python3
import os
import sys
import math
import urllib.request
import click
import gzip
import shutil
import sqlite3
import collections
import csv
import re
import matplotlib.pyplot as plt
from statistics import mean
import traceback
import jinja2

HERE = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(HERE, 'data')
RESULTS_DIR = os.path.join(HERE, 'results')

FILE_CONFS = [
    {
        "url": "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/tps00001.tsv.gz",
        "fname": "population.tsv"
    },
    {
        "url": "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/tps00029.tsv.gz",
        "fname": "deaths.tsv"
    },
    {
        "url": "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/tps00010.tsv.gz",
        "fname": "ages.tsv"
    },
    {
        "url": "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/demo_pjan.tsv.gz",
        "fname": "population_age_sex.tsv"
    },
    {
        "url": "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/demo_magec.tsv.gz",
        "fname": "deaths_age_sex.tsv"
    }
]

COUNTRY_CODES = {
    "AL":"Albanie",   # Missing data
    "AM": "Arménie",   # Missing data
    "AT": "Autriche",
    "AZ": "Azerbaïdjan",
    "BG": "Bulgaria",
    "CH": "Suisse",
    "CZ": "Tchéquie",
    "DE": "Allemagne",
    "DK": "Danemark",
    "ES": "Espagne",
    "FR": "France",
    "FI": "Finlande",
    "GE": "Géorgie",   # Missing data
    "HR": "Croatie",
    "HU": "Hongrie",
    "IE": "Irlande",
    "IT": "Italie",
    "LT": "Lituanie",
    "LV": "Lettonie",
    "MD": "Moldavie",   # Missing data
    "MK": "Macédoine du Nord",
    "NL": "Pays-Bas",
    "NO": "Norvège",
    "PL": "Pologne",
    "PT": "Portugal",
    "RO": "Roumanie",
    "RS": "Serbie",
    "RU": "Russie",   # Missing data
    "SE": "Suède",
    "SI": "Slovénie",
    "SK": "Slovaquie",
    "TR": "Turquie",
    "UA": "Ukraine",
    "UK": "Royaume-Uni",  # no 2020 population !
    "XK": "Kosovo"   # Missing data
}

AGE_MAX = 90  # after 90, data are less significant, and numbers should be summed together

#YEARS = range(2010, 2020+1)


@click.group()
def main():
    pass


@main.command("all")
@click.option("--import", "do_import", type=bool, default=True)
def cmd_all(do_import):
    if do_import:
        download_data()
        import_data()


@main.command("download_data")
def cmd_download_data():
    download_data()

def download_data():
    _mkdir(DATA_DIR)
    for conf in FILE_CONFS:
        fpath = os.path.join(DATA_DIR, conf['fname'])
        if not os.path.exists(fpath):
            _download(conf["url"], f"{fpath}.gz")
            _ungzip(f"{fpath}.gz")


@main.command("import_data")
def cmd_import_data():
    init_db()
    import_data()

def db_connect():
    return sqlite3.connect(os.path.join(HERE, "data.sqlite"))

def init_db():
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS population_age_sex(geo text, year integer, sex text, age integer, value integer)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS deaths(geo text, year integer, value integer)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS deaths_age_sex(geo text, year integer, sex text, age integer, value integer)''')
        cur.execute('''DELETE FROM population_age_sex''')
        cur.execute('''DELETE FROM deaths''')
        cur.execute('''DELETE FROM deaths_age_sex''')

def import_data():
    with db_connect() as conn:
        _import_population(conn)
        _import_deaths(conn)
        _import_deaths_age_sex(conn)

def _import_population(conn):
    vals = collections.defaultdict(int)
    with open(os.path.join(DATA_DIR, "population_age_sex.tsv"), newline='') as csvf:
        for row in csv.DictReader(csvf, delimiter='\t'):
            ind, age, sex, geo = row["unit,age,sex,geo\\time"].split(",")
            age = _parse_age(age)
            if ind == "NR" and sex != 'T' and age:
                for year in range(1960, 2021+1):
                    vals[(geo, year, sex, age)] += _parse_int(row[f"{year} "])
    _db_bulk_insert(conn, "population_age_sex", [{
        "geo": geo,
        "year": year,
        "sex": sex,
        "age": age,
        "value": val
    } for (geo, year, sex, age), val in vals.items()])

# def _import_population(conn):
#     db_rows = []
#     with open(os.path.join(DATA_DIR, "population.tsv"), newline='') as csvf:
#         for row in csv.DictReader(csvf, delimiter='\t'):
#             ind, geo = row["indic_de,geo\\time"].split(",")
#             for year in YEARS:
#                 db_rows.append({
#                     "geo": geo,
#                     "year": year,
#                     "value": _parse_int(row[f"{year} "])
#                 })
#     _db_bulk_insert(conn, "population", db_rows)

def _import_deaths_age_sex(conn):
    vals = collections.defaultdict(int)
    with open(os.path.join(DATA_DIR, "deaths_age_sex.tsv"), newline='') as csvf:
        for row in csv.DictReader(csvf, delimiter='\t'):
            ind, sex, age, geo = row["unit,sex,age,geo\\time"].split(",")
            age = _parse_age(age)
            if ind == "NR" and sex != 'T' and age:
                for year in range(1960, 2019+1):
                    vals[(geo, year, sex, age)] += _parse_int(row[f"{year} "])
    _db_bulk_insert(conn, "deaths_age_sex", [{
        "geo": geo,
        "year": year,
        "sex": sex,
        "age": age,
        "value": val
    } for (geo, year, sex, age), val in vals.items()])

def _import_deaths(conn):
    db_rows = []
   # 2020 from deaths.tsv
    with open(os.path.join(DATA_DIR, "deaths.tsv"), newline='') as csvf:
        for row in csv.DictReader(csvf, delimiter='\t'):
            ind, geo = row["indic_de,geo\\time"].split(",")
            if ind == "DEATH_NR":
                db_rows.append({
                    "geo": geo,
                    "year": 2020,
                    "value": _parse_int(row[f"2020 "])
                })
    # before 2020 from deaths_age_sex.tsv
    with open(os.path.join(DATA_DIR, "deaths_age_sex.tsv"), newline='') as csvf:
        for row in csv.DictReader(csvf, delimiter='\t'):
            ind, sex, age, geo = row["unit,sex,age,geo\\time"].split(",")
            if ind == "NR" and sex == 'T' and age == 'TOTAL':
                for year in range(1960, 2019+1):
                    db_rows.append({
                        "geo": geo,
                        "year": year,
                        "value": _parse_int(row[f"{year} "])
                    })
    _db_bulk_insert(conn, "deaths", db_rows)

# def _import_ages(conn):
#     db_rows = []
#     with open(os.path.join(DATA_DIR, "ages.tsv"), newline='') as csvf:
#         for row in csv.DictReader(csvf, delimiter='\t'):
#             ind, geo = row["indic_de,geo\\time"].split(",")
#             if ind.startswith("PC_"):
#                 age = ind[3:]
#                 for year in YEARS:
#                     db_rows.append({
#                         "geo": geo,
#                         "age": age,
#                         "year": year,
#                         "value": _parse_int(row[f"{year} "])
#                     })
#     _db_bulk_insert(conn, "ages", db_rows)

def _parse_int(val):
    if val == ": ": return None
    val = re.sub("[^0-9]", "", val)
    if not val: return None
    return int(val)

def _parse_age(val):
    if val == 'Y_OPEN': return AGE_MAX
    if val == 'Y_LT1': return 0
    val = re.sub("[^0-9]", "", val)
    if not val: return None
    return min(int(val), AGE_MAX)


@main.command("plot_deaths")
@click.option("--start")
@click.option("--country")
def cmd_plot_deaths(*args, **kwargs):
    plot_deaths(*args, **kwargs)

def plot_deaths(start=None, country=None):
    country_filter = country
    if not start: start = 1980
    ages = range(0, AGE_MAX+1)
    years_2019 = range(start, 2019+1)
    years_2020 = range(start, 2020+1)
    with db_connect() as conn:
        nb_country_ok = 0
        for code, country in COUNTRY_CODES.items():
            if country_filter and code != country_filter:
                continue
            try:
                print(f"Plot deaths of {code} ({country})")
                rows = conn.execute((
                    "SELECT year, age, SUM(value) "
                    "FROM population_age_sex "
                    "WHERE geo = ? "
                    "AND year BETWEEN ? AND ? "
                    "GROUP BY year, age"
                ), [code, start, 2020])
                pops = {}
                for year, age, value in rows:
                    pops.setdefault(year, {})[age] = value or 0
                # if a year have too much holes: clean it (it will cancel estimation)
                for year in years_2020:
                    nb_nulls = sum(1 for a in ages if pops[year].get(a,0) == 0)
                    if nb_nulls >= 5:
                        for age in ages:
                            pops[year][age] = 0
                rows = conn.execute((
                    "SELECT year, age, SUM(value) "
                    "FROM deaths_age_sex "
                    "WHERE geo = ? "
                    "AND year BETWEEN ? AND ? "
                    "GROUP BY year, age"
                ), [code, start, 2019])
                deaths = {}
                for year, age, value in rows:
                    deaths.setdefault(year, {})[age] = value or 0
                rows = conn.execute((
                    "SELECT year, SUM(value) "
                    "FROM deaths "
                    "WHERE geo = ? "
                    "AND year BETWEEN ? AND ? "
                    "GROUP BY year"
                ), [code, start, 2020])
                total_deaths = {
                    year: value
                    for year, value in rows
                }
                # to be sure 2020 (coming from 'deaths') and other years (coming from 'deaths_age_sex')
                # are synchronised, let's use a corrector
                deaths_correction = {
                    year: _div(total_deaths[year], sum(deaths[year].get(age, 0) for age in ages))
                    for year in years_2019
                }
                death_rates = {
                    year: {
                        age: _div(deaths[year].get(age, 0), pops[year].get(age, 0))
                        for age in ages
                    }
                    for year in years_2019
                }
                real_deaths = [
                    total_deaths[year] or None
                    for year in years_2020
                ]
                simulated_deaths = [
                    sum(
                        int( death_rates[year].get(age,0) * pops[2020].get(age,0) )
                        for age in ages
                    ) * deaths_correction[year] or None
                    for year in years_2019
                ]
                if not total_deaths.get(2020):
                    print("  WARNING: unsufficient data (no 2020 real deaths)")
                    continue
                if len([d for d in real_deaths if d is not None]) < 10:
                    print("  WARNING: unsufficient data (less than 10 years for real deaths)")
                    continue
                if len([d for d in simulated_deaths if d is not None]) < 10:
                    print("  WARNING: unsufficient data (less than 10 years for simulated deaths)")
                    continue
                nb_xticks, nb_years = 5, len(years_2020)
                xticks_period = math.floor(nb_years/nb_xticks)
                _plot(f"[{country}] Mortalite",
                    years_2020,
                    {
                        "Mortalité réelle": real_deaths,
                        f"Mortalité standardisée à population constante (2020)": simulated_deaths+[total_deaths[2020]]
                    },
                    f'{code}_deaths.png',
                    axis=[None, None, 0, None],
                    xticks=[y if (2020-y) % xticks_period == 0 else None for y in years_2020]
                )
                nb_country_ok += 1
            except Exception:
                traceback.print_exc()
        print(f"Nb countries successfully computed: {nb_country_ok}/{len(COUNTRY_CODES)}")


            # for year in range(start, 2019+1):
            #     death_rates[year] = {}
            #     for age 
            #     age: sum(deaths[year][age] for year in range(start, 2019+1)) / sum()
            #     for age in range(0, 99+1)
            # }
            # deaths = {
            #     year: value
            #     for year, value in conn.execute(
            #         "SELECT year, value FROM deaths WHERE geo = ?",
            #         [code]
            #     )
            # }
            # part65 = {
            #     year: _div(value, 1000)
            #     for year, value in conn.execute(
            #         "SELECT year, SUM(value) FROM ages WHERE geo = ? and age in ('Y65_79', 'Y80_MAX') GROUP BY year",
            #         [code]
            #     )
            # }
            # _plot(f"[{country}] Nombre de décès",
            #     YEARS,
            #     [[deaths.get(year, 0) for year in YEARS]],
            #     f'{code}_deaths.png')
            # _plot(f"[{country}] Population",
            #     YEARS,
            #     [
            #         [pops.get(year, 0) for year in YEARS],
            #         [pops.get(year, 0) * part65.get(year, 0) for year in YEARS],
            #     ],
            #     f'{code}_population.png')
            # _plot(f"[{country}] Taux de mortalité normalisé (65 ans et +)",
            #     YEARS,
            #     [[_div(deaths.get(year, 0), pops.get(year, 0) * part65.get(year, 0)) for year in YEARS]],
            #     f'{code}_death_rates_65.png')
    
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(HERE)
    )
    template = env.get_template("README.md.tmpl")
    with open("README.md", "w") as outputf:
        outputf.write(
            template.render(countries={
                code: {
                    "name": name
                }
                for code, name in COUNTRY_CODES.items()
            })
        )


# utils

def _div(a, b):
    if not b: return 0
    return a / b

def _mkdir(path):
    try:
        os.makedirs(path)
    except FileExistsError:
        pass

def _download(url, ofpath):
    if not os.path.exists(ofpath):
        print(f'Download {os.path.basename(ofpath)}... ', end='')
        sys.stdout.flush()
        urllib.request.urlretrieve(url, ofpath)
        print(f'DONE')

def _ungzip(fpath):
    with gzip.open(fpath, 'rb') as f_in:
        with open(os.path.splitext(fpath)[0], 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(fpath)

def _db_bulk_insert(conn, table_name, values):
    if len(values) == 0:
        return
    conn.cursor().executemany(
        f"INSERT INTO {table_name} ({','.join(values[0].keys())}) VALUES ({','.join('?' for _ in range(len(values[0])))})",
        [list(v.values()) for v in values])

def _plot(title, xs, yss, ofname, axis=None, xticks=None):
    plt.clf()
    plt.title(title)
    for label, ys in yss.items():
        plt.plot(xs, ys, label=label)
    if axis:
        plt.axis(axis)
    if xticks:
        plt.xticks(xs,xticks)
    plt.legend()
    plt.savefig(os.path.join(RESULTS_DIR, ofname))


if __name__ == '__main__':
    main()