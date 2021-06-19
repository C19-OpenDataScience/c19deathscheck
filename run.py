#!/usr/bin/env  python3
import os
import sys
import re
import click
import sqlite3
import urllib.request
from datetime import datetime, timedelta
from collections import defaultdict
from glob import glob
import xlrd
import matplotlib.pyplot as plt
from statistics import mean

HERE = os.path.dirname(__file__)

def _get_date_ranges():
    return _get_peak_date_ranges()

def _get_peak_date_ranges():
    return [
        {"name":"Grippe (de 2017-01-01 à 2017-02-01)", "year":2017, "range":("2017-01-01", "2017-02-01")},
        {"name":"Covid19 (de 2020-03-20 à 2020-04-20)", "year":2020, "range":("2020-03-20", "2020-04-20")},
    ]

def _get_years_date_ranges():
    return [
        {
            "name": str(year),
            "year": year,
            "range": (f"{year}-01-01", _add_days(f"{year}-01-01", 365))
        }
        for year in range(2000, 2020+1)
    ]

DECES_FILES_SRC = [
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-190504/deces-2000.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-190558/deces-2001.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-190702/deces-2002.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-190755/deces-2003.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-190852/deces-2004.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-190939/deces-2005.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-191027/deces-2006.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-191117/deces-2007.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-191225/deces-2008.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-191359/deces-2009.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-191659/deces-2010.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-191745/deces-2011.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-191851/deces-2012.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-191938/deces-2013.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-192022/deces-2014.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-192119/deces-2015.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-192203/deces-2016.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-192304/deces-2017.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191205-191652/deces-2018.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20200113-173945/deces-2019.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20210112-143457/deces-2020.txt"
]

DATA_FILES_CONFS = [
    {
        "type": "pyramide-des-ages",
        "src": "https://www.insee.fr/fr/statistiques/pyramide/3312958/xls/pyramides-des-ages_bilan-demo_2019.xls",
        "sheet": "France métropolitaine",
        "rows": {
            "annee": 9,
            "hommes_debut": 11,
            "hommes_fin": 116,
            "femmes_debut": 120,
            "femmes_fin": 225
        },
        "cols":{
            "age": 2
        }
    }
] + [{
    "type": "deces",
    "src": src
} for src in DECES_FILES_SRC]


def _get_conf_fname(conf):
    return conf.get("name") or os.path.basename(conf["src"])


@click.group()
def main():
    pass


@main.command("all")
def all():
    _init_db()
    _download_data()
    _import_data()
    _compute_taux_mortalite_par_age()
    _compute_deces_par_date()
    _compute_population_par_age()
    _compute_deces_par_age()
    _compute_taux_mortalite_moyenne_par_age(list(range(2000,2020+1)))
    _compute_mortality_forecast()


@main.command("init_db")
def init_db_cmd():
    _init_db()


def _db_connect():
    return sqlite3.connect(os.path.join(HERE, "data.sqlite"))


def _init_db():
    with _db_connect() as conn:
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS deces(sex text, date_naissance text, date_deces text, age integer)''')
        cur.execute('''DELETE FROM deces''')
        cur.execute('''CREATE TABLE IF NOT EXISTS ages(annee integer, age integer, nb integer)''')
        cur.execute('''DELETE FROM ages''')


@main.command("download_data")
def download_data_cmd():
    _download_data()


def _download_data():
    data_path = os.path.join(HERE, "data")
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    for conf in DATA_FILES_CONFS:
        _download_data_file(conf)


def _download_data_file(conf):
    fname = _get_conf_fname(conf)
    fpath = os.path.join(HERE, "data", fname)
    if not os.path.exists(fpath):
        print(f'Download {fname}... ', end='')
        sys.stdout.flush()
        urllib.request.urlretrieve(conf["src"], fpath)
        print(f'DONE')


@main.command("import_data")
def import_data_cmd():
    _import_data()


def _import_data():
    with _db_connect() as conn:
        for conf in DATA_FILES_CONFS:
            print(f"import {_get_conf_fname(conf)}")
            if conf["type"] == "deces":
                _import_deces_file(conn, conf)
            if conf["type"] == "pyramide-des-ages":
                _import_pda_file(conn, conf)


def _import_deces_file(conn, conf):
    fname = _get_conf_fname(conf)
    path = os.path.join(HERE, "data", fname)
    with open(path, 'rb') as file:
        rows, errors = [], []
        num_line = 1
        nb_inserted = 0
        for line in file.readlines():
            try:
                date_naissance = _parse_date(line[81:89].decode("utf-8"), def_month="06", def_day="15")
                date_deces = _parse_date(line[154:162].decode("utf-8"))
                naissance_dt = _to_dt(date_naissance)
                deces_dt = _to_dt(date_deces)
                age = max(0, min(100, _dt_to_annees(deces_dt - naissance_dt)))
                parsed = {
                    "sex": _parse_sex(line[80:81].decode("utf-8")),
                    "date_naissance": date_naissance,
                    "date_deces": date_deces,
                    "age": age
                }
                rows.append(parsed)
                nb_inserted += 1
            except Exception as exc:
                if isinstance(exc, ParseError) or isinstance(exc, ValueError):
                    errors.append(exc)
                else:
                    raise()
            num_line += 1
        print(f"Nb errors for {fname}: {len(errors)} / {num_line-1} ({'{:.5f}'.format(100*len(errors)/(num_line-1))}%)")
        for e in errors[:10]: print(e)
    _db_bulk_insert(conn, "deces", rows)


def _import_pda_file(conn, conf):
    fname = _get_conf_fname(conf)
    path = os.path.join(HERE, "data", fname)
    book = xlrd.open_workbook(path)
    sheet = book.sheet_by_name(conf["sheet"])
    rows = []
    # loop on rows
    # (with xlrd rows and columns start with 0)
    row_annee = conf["rows"]["annee"]-1
    col_age = conf["cols"]["age"]-1
    def _parse_age(val):
        assert val != ''
        if type(val) is str:
            # traitement specifique pour "100 et +"
            res= int(re.sub("[^0-9]", "", val))
        else:
            res = int(val)
        return max(0, min(100, res))
    def _parse_int(val):
        if type(val) is int:
            return val
        if (type(val) is float) or (type(val) is str and val.isdigit()):
            return int(val)
        return 0
    pop_by_annee_age = defaultdict(int)
    for annee in range(2000, 2020+1):
        col = 0
        while True:
            annee_cell = _parse_int(sheet.cell(row_annee, col).value)
            if annee_cell == annee:
                for row in range(conf["rows"]["hommes_debut"]-1, conf["rows"]["hommes_fin"]):
                    age = _parse_age(sheet.cell(row, col_age).value)
                    nb_hommes = _parse_int(sheet.cell(row, col).value)
                    pop_by_annee_age[(annee, age)] += nb_hommes
                for row in range(conf["rows"]["femmes_debut"]-1, conf["rows"]["femmes_fin"]):
                    age = _parse_age(sheet.cell(row, col_age).value)
                    nb_femmes = _parse_int(sheet.cell(row, col).value)
                    pop_by_annee_age[(annee, age)] += nb_femmes
                break
            col += 1
            if col > 200:
                raise(f"Annee {annee} not found")
    rows = [{
        "annee": annee,
        "age": age,
        "nb": nb
    } for (annee, age), nb in pop_by_annee_age.items()]
    _db_bulk_insert(conn, "ages", rows)


@main.command("compute_taux_mortalite_par_age")
def compute_taux_mortalite_par_age():
    _compute_taux_mortalite_par_age()


def _compute_taux_mortalite_par_age():
    print(f"compute taux_mortalite_par_age")
    _assert_all_date_ranges_have_same_duration()
    plt.clf()
    plt.title("[France] Taux de mortalité par âge")
    with _db_connect() as conn:
        age_range = list(range(1, 101))
        for dr in _get_date_ranges():
            taux_mortalite_par_age = __compute_taux_mortalite_par_age(conn, dr["year"], dr["range"])
            plt.plot(age_range, [taux_mortalite_par_age.get(i, 0) for i in age_range], label=dr["name"])
    plt.legend()
    plt.savefig(os.path.join(HERE, 'results/taux_mortalite_par_age.png'))


def _select_pop_par_age(conn, annee):
    rows = conn.cursor().execute(
        '''SELECT age, SUM(nb) FROM ages WHERE annee = ? GROUP BY age''',
        [annee]
    )
    return {age:nb for age, nb in rows}


def _select_deces_par_age(conn, date_range):
    rows = conn.cursor().execute(
        '''SELECT age, count(*) FROM deces WHERE date_deces BETWEEN ? AND ? GROUP BY age''',
        [*date_range]
    )
    return {age: nb for age, nb in rows}


def __compute_taux_mortalite_par_age(conn, year, date_range):
    pop_par_age = _select_pop_par_age(conn, year)
    nb_deces_par_age = _select_deces_par_age(conn, date_range)
    _div = lambda a, b: a/b if b else 0
    return {age: _div(nb, pop_par_age.get(age)) for age, nb in nb_deces_par_age.items()}


@main.command("compute_deces_par_date")
def compute_deces_par_date():
    _compute_deces_par_date()


def _compute_deces_par_date():
    print(f"compute deces_par_date")
    _assert_all_date_ranges_have_same_duration()
    plt.clf()
    plt.title("[France] Décès par date")
    with _db_connect() as conn:
        for dr in _get_date_ranges():
            deces_par_date = _select_deces_par_date(conn, dr["range"])
            dates = _date_range_to_dates(dr["range"])
            plt.plot(range(len(dates)), [deces_par_date.get(d, 0) for d in dates], label=dr["name"])
    plt.legend()
    plt.savefig(os.path.join(HERE, 'results/deces_par_date.png'))


def _select_deces_par_date(conn, date_range):
    rows = conn.cursor().execute(
        '''SELECT date_deces, count(*) FROM deces WHERE date_deces BETWEEN ? AND ? GROUP BY date_deces''',
        [*date_range]
    )
    return {_to_dt(date_deces): nb for date_deces, nb in rows}


@main.command("compute_population_par_age")
def compute_population_par_age():
    _compute_population_par_age()


def _compute_population_par_age():
    print(f"compute population_par_age")
    _assert_all_date_ranges_have_same_duration()
    plt.clf()
    plt.title("[France] Population par âge")
    age_range = list(range(1, 101))
    with _db_connect() as conn:
        for dr in _get_date_ranges():
            pop_par_age = _select_pop_par_age(conn, dr["year"])
            plt.plot(age_range, [pop_par_age.get(i, 0) for i in age_range], label=dr["year"])
    plt.legend()
    plt.savefig(os.path.join(HERE, 'results/population_par_age.png'))


@main.command("compute_deces_par_age")
def compute_deces_par_age():
    _compute_deces_par_age()


def _compute_deces_par_age():
    print("compute deces_par_age")
    _assert_all_date_ranges_have_same_duration()
    plt.clf()
    plt.title("[France] Décès par âge")
    age_range = list(range(1, 101))
    with _db_connect() as conn:
        for dr in _get_date_ranges():
            nb_deces_par_age = _select_deces_par_age(conn, dr["range"])
            plt.plot(age_range, [nb_deces_par_age.get(i, 0) for i in age_range], label=dr["name"])
    plt.legend()
    plt.savefig(os.path.join(HERE, 'results/deces_par_age.png'))


@main.command("compute_taux_mortalite_moyenne_par_age")
def compute_taux_mortalite_moyenne_par_age():
    _compute_taux_mortalite_moyenne_par_age(list(range(2000,2020+1)))


def _compute_taux_mortalite_moyenne_par_age(annees):
    print("compute compute_taux_mortalite_moyenne_par_age")
    plt.clf()
    plt.title("[France] Taux de mortalité moyennée par âge")
    moyennes_mortalite = []
    with _db_connect() as conn:
        for annee in annees:
            pop_par_age = _select_pop_par_age(conn, annee)
            deces_par_age = _select_deces_par_age(conn, (f"{annee}-01-01", f"{annee}-12-31"))
            mortalite_par_age = {
                age: deces_par_age.get(age, 0) / pop if pop > 0 else 0
                for age, pop in pop_par_age.items()
            }
            moyennes_mortalite.append(sum(mortalite_par_age.values()) / len(mortalite_par_age))
    plt.bar(annees, moyennes_mortalite)
    plt.legend()
    plt.savefig(os.path.join(HERE, 'results/taux_mortalite_moyenne_par_age.png'))



@main.command("compute_mortalite_par_annee")
def compute_mortalite_par_annee():
    _compute_mortalite_par_annee()


def _compute_mortalite_par_annee():
    print("compute mortalite_par_annee")
    plt.clf()
    plt.title("[France] Mortalité")
    moyennes_mortalite = []
    with _db_connect() as conn:
        res = __compute_mortalite_par_annee(conn, 2000, 2020)
        plt.bar(res.keys(),res.values())
        plt.legend()
        plt.savefig(os.path.join(HERE, 'results/mortalite_par_annee.png'))


def __compute_mortalite_par_annee(conn, annee1, annee2):
    rows = conn.cursor().execute(
        '''select substr(date_deces, 1, 4) as year, count(*) as nb from deces where date_deces between ? and ? group by year order by year''',
        (str(annee1), str(annee2+1))
    )
    return {int(year): nb for year, nb in rows}



@main.command("compute_mortality_forecast")
def compute_mortality_forecast():
    _compute_mortality_forecast()


def _compute_mortality_forecast():
    print("compute mortality_forecast")
    plt.clf()
    plt.title("[France] Prévision de mortalité")
    DEBUT_PREV = 2010
    with _db_connect() as conn:
        mortalite_reelle_par_annee = __compute_mortalite_par_annee(conn, DEBUT_PREV, 2020)
        taux_mortalite_par_age_moyen = _compute_taux_mortalite_par_age_moyen(conn, DEBUT_PREV, 2019)
        prev_morts = {}
        pop_par_age = _select_pop_par_age(conn, DEBUT_PREV)
        def _estimate_mort_par_age():
            return {
                age: pop_par_age[age] * taux_mortalite_par_age_moyen[age]
                for age in range(0, 100+1)
            }
        mort_par_age = _estimate_mort_par_age()
        prev_morts[DEBUT_PREV] = sum(mort_par_age.values())
        for annee in range(DEBUT_PREV+1, 2050+1):
            pop_par_age[100] = max(0, pop_par_age[99] - mort_par_age[99]) + max(0, pop_par_age[100] - mort_par_age[100])
            for age in reversed(range(1, 99+1)):
                pop_par_age[age] = max(0, pop_par_age[age-1] - mort_par_age[age-1])
            mort_par_age = _estimate_mort_par_age()
            prev_morts[annee] = sum(mort_par_age.values())
    plt.bar(mortalite_reelle_par_annee.keys(), mortalite_reelle_par_annee.values(), label="Mortalité réelle")
    plt.plot(prev_morts.keys(), prev_morts.values(), 'r', label="Prévision de mortalité")
    plt.legend()
    plt.savefig(os.path.join(HERE, 'results/prevision_morts.png'))


def _compute_taux_mortalite_par_age_moyen(conn, annee1, annee2):
    taux_mortalite_par_age_par_annee = {
        annee: __compute_taux_mortalite_par_age(conn, annee, (f"{annee}-01-01", f"{annee}-12-31"))
        for annee in range(annee1, annee2+1)
    }
    return {
        age: mean(
            taux_mortalite_par_age_par_annee[annee][age]
            for annee in range(annee1, annee2+1)
        )
        for age in range(0, 100+1)
    }



# parsing

class ParseError(Exception):
    pass

class ParseSexError(ParseError):
    pass

def _parse_sex(val):
    if val == "1": return "M"
    if val == "2": return "F"
    raise ParseSexError(f"Bad sex value: {val}")

class DateParseError(ParseError):
    pass

def _parse_date(val, def_month=None, def_day=None):
    try:
        year = val[0:4]
        month = val[4:6]
        day = val[6:8]
        if year=="0000":
            raise DateParseError(f"Bad year value: {year}")
        if month=="00":
            if def_month:
                month = def_month
            else:
                raise DateParseError(f"Bad month value: {month}")
        if day=="00":
            if def_day:
                day = def_day
            else:
                raise DateParseError(f"Bad day value: {day}")
        return f"{year}-{month}-{day}"
    except Exception as exc:
        raise DateParseError(exc)


def _assert_all_date_ranges_have_same_duration():
    duration = None
    for dr in _get_date_ranges():
        start_date, end_date = dr["range"]
        dur = _to_dt(end_date) - _to_dt(start_date)
        if duration is None:
            duration = dur
        else:
            assert duration == dur


def _date_range_to_dates(date_range):
    res = []
    start, end = _to_dt(date_range[0]), _to_dt(date_range[1])
    day = start
    while day <= end:
        res.append(day)
        day += timedelta(days=1)
    return res


# utils

def _to_dt(date):
    return datetime.strptime(date, '%Y-%m-%d')

def _dt_to_annees(dt):
    return int(dt.days / 365.25)

def _add_days(date, days):
    dt = _to_dt(date)
    dt += datetime.delta(days=days)
    return dt.stftime('%Y-%m-%d')

def _db_bulk_insert(conn, table_name, values):
    if len(values) == 0:
        return
    conn.cursor().executemany(
        f"INSERT INTO {table_name} ({','.join(values[0].keys())}) VALUES ({','.join('?' for _ in range(len(values[0])))})",
        [list(v.values()) for v in values])

if __name__ == "__main__":
    main()