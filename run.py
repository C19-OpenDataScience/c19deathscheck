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

HERE = os.path.dirname(__file__)

def _get_date_ranges():
    return _get_peak_date_ranges()

def _get_peak_date_ranges():
    return [
        {"name":"Grippe (de 2017-01-01 à 2017-02-01)", "year":2017, "range":("2017-01-01", "2017-02-01")},
        {"name":"Covid19 (de 2020-03-20 à 2020-04-20)", "year":2020, "range":("2020-03-20", "2020-04-20")},
    ]

DECES_FILES_SRC = [
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20191209-192304/deces-2017.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20210112-143457/deces-2020.txt"
]

DATA_FILES_CONFS = [
    {
        "src": "https://www.insee.fr/fr/statistiques/fichier/1913143/pyramide-des-ages-2017.xls",
        "type": "pyramide-des-ages",
        "annee": 2017,
        "cols": {
            "age": 2,
            "nb": 5
        },
        "rows": (7, 107)
    },
    {
        "src": "https://www.insee.fr/fr/statistiques/fichier/1913143/pyramide-des-ages-2020.xls",
        "type": "pyramide-des-ages",
        "annee": 2020,
        "cols": {
            "age": 2,
            "nb": 5
        },
        "rows": (7, 107)
    },
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
    with open(path) as file:
        rows, errors = [], []
        num_line = 1
        nb_inserted = 0
        for line in file.readlines():
            try:
                date_naissance = _parse_date(line[81:89], def_month="06", def_day="15")
                date_deces = _parse_date(line[154:162])
                naissance_dt = _to_dt(date_naissance)
                deces_dt = _to_dt(date_deces)
                age = _dt_to_annees(deces_dt - naissance_dt)
                parsed = {
                    "sex": _parse_sex(line[80]),
                    "date_naissance": date_naissance,
                    "date_deces": date_deces,
                    "age": age
                }
                rows.append(parsed)
                nb_inserted += 1
            except ParseError as exc:
                if isinstance(exc, ParseError):
                    errors.append(exc)
            num_line += 1
        print(f"Nb errors for {fname}: {len(errors)} / {num_line-1} ({'{:.5f}'.format(100*len(errors)/(num_line-1))}%)")
        for e in errors[:10]: print(e)
    _db_bulk_insert(conn, "deces", rows)


def _import_pda_file(conn, conf):
    fname = _get_conf_fname(conf)
    path = os.path.join(HERE, "data", fname)
    book = xlrd.open_workbook(path)
    sheet = book.sheet_by_index(0)
    rows = []
    # loop on rows
    # (with xlrd rows and columns start with 0)
    first_row, last_row = conf["rows"]
    age_col = conf["cols"]["age"]
    nb_col = conf["cols"]["nb"]
    for i in range(first_row-1, last_row):
        # parse age
        age = sheet.cell(i, age_col-1).value
        assert age != ''
        if type(age) is str:
            # traitement specifique pour "100 et +"
            age = int(re.sub("[^0-9]", "", age))
        else:
            age = int(age)
        # parse nb
        nb = sheet.cell(i, nb_col-1).value
        assert nb != ''
        nb = int(nb)
        rows.append({
            "annee": conf["annee"],
            "age": age,
            "nb": nb
        })
    _db_bulk_insert(conn, "ages", rows)


@main.command("compute_taux_mortalite_par_age")
def compute_taux_mortalite_par_age():
    _compute_taux_mortalite_par_age()


def _compute_taux_mortalite_par_age():
    print(f"compute taux_mortalite_par_age")
    _assert_all_date_ranges_have_same_duration()
    plt.clf()
    plt.title("Taux de mortalité par âge")
    with _db_connect() as conn:
        age_range = list(range(1, 101))
        for dr in _get_date_ranges():
            pop_par_age = _select_pop_par_age(conn, dr["year"])
            nb_deces_par_age = _select_deces_par_age(conn, dr["range"])
            _div = lambda a, b: a/b if b else 0
            taux_mortalite_par_age = {age: _div(nb, pop_par_age.get(age)) for age, nb in nb_deces_par_age.items()}
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


@main.command("compute_deces_par_date")
def compute_deces_par_date():
    _compute_deces_par_date()


def _compute_deces_par_date():
    print(f"compute deces_par_date")
    _assert_all_date_ranges_have_same_duration()
    plt.clf()
    plt.title("Décès par date")
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
    plt.title("Population par âge")
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
    plt.title("Décès par âge")
    age_range = list(range(1, 101))
    with _db_connect() as conn:
        for dr in _get_date_ranges():
            nb_deces_par_age = _select_deces_par_age(conn, dr["range"])
            plt.plot(age_range, [nb_deces_par_age.get(i, 0) for i in age_range], label=dr["name"])
    plt.legend()
    plt.savefig(os.path.join(HERE, 'results/deces_par_age.png'))


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