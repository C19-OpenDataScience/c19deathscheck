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
from math import floor
from statistics import mean, stdev

HERE = os.path.dirname(__file__)

def _to_dt(date):
    return datetime.strptime(date, '%Y-%m-%d')

def _add_days(date, days):
    dt = _to_dt(date)
    dt += timedelta(days=days)
    return dt.strftime('%Y-%m-%d')

def _date_range(date, days):
    return (date, _add_days(date, days))

RANGES = {
    "pics_2017_2020": [
        {"name":"Grippe (de 2017-01-01 à 2017-02-01)", "year":2017, "range":("2017-01-01", "2017-02-01")},
        {"name":"Covid19 (de 2020-03-20 à 2020-04-20)", "year":2020, "range":("2020-03-20", "2020-04-20")},
    ],
    "2016_2020": [
        {"name":str(annee), "year":annee, "range":_date_range(f"{annee}-01-01", 365)}
        for annee in (2016, 2020)
    ],
    "2017_2020": [
        {"name":str(annee), "year":annee, "range":_date_range(f"{annee}-01-01", 365)}
        for annee in (2017, 2020)
    ],
    "2000_to_2020": [
        {
            "name": str(year),
            "year": year,
            "range": _date_range(f"{year}-01-01", 365)
        }
        for year in range(2000, 2020+1)
    ],
    "2000_to_2021_juin": [
        {
            "name": str(year),
            "year": year,
            "range": _date_range(f"{year-1}-06-01", 365)
        }
        for year in range(2001, 2021+1)
    ]
}


# assert all date ranges have same duration
for drkey, ranges in RANGES.items():
    duration = None
    for dr in ranges:
        start_date, end_date = dr["range"]
        dur = _to_dt(end_date) - _to_dt(start_date)
        if duration is None:
            duration = dur
        else:
            assert duration == dur, (duration, dur, drkey)


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
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20210112-143457/deces-2020.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20210409-131502/deces-2021-t1.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20210709-174839/deces-2021-t2.txt",
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
    }, {
        "type": "pyramide-des-ages-2",
        "src": "https://www.insee.fr/fr/statistiques/fichier/5007688/Pyramides-des-ages-2021.xlsx",
        "sheet": "2021 Métro",
        "annee": 2021,
        "rows": {
            "debut": 7,
            "fin": 112
        },
        "cols":{
            "age": 2,
            "nb": 5
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
@click.option("--import", "do_import", type=bool, default=True)
def cmd_all(do_import):
    if do_import:
        _download_data()
        _import_data()
    compute_taux_mortalite_par_age("pics_2017_2020")
    compute_taux_mortalite_par_age("2000_to_2020")
    compute_deces_par_date("pics_2017_2020")
    compute_population_par_age("pics_2017_2020")
    compute_deces_par_age("pics_2017_2020")
    compute_deces_par_age("2016_2020", simulate=True)
    compute_taux_mortalite_standardise_par_age("2000_to_2020")
    compute_mortality_forecast()
    compute_surmortality()


def _db_connect():
    return sqlite3.connect(os.path.join(HERE, "data.sqlite"))


def _init_db():
    with _db_connect() as conn:
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS deces(sex text, date_naissance text, date_deces text, lieu_deces text, age integer, is_metro bool)''')
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
    _init_db()
    _import_data()


def _import_data():
    with _db_connect() as conn:
        for conf in DATA_FILES_CONFS:
            print(f"import {_get_conf_fname(conf)}")
            if conf["type"] == "deces":
               _import_deces_file(conn, conf)
            if conf["type"] == "pyramide-des-ages":
               _import_pda_file(conn, conf)
            if conf["type"] == "pyramide-des-ages-2":
                _import_pda2_file(conn, conf)


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
                lieu_deces = line[162:167].decode("utf-8")
                naissance_dt = _to_dt(date_naissance)
                deces_dt = _to_dt(date_deces)
                age = max(0, min(100, _dt_to_annees(deces_dt - naissance_dt)))
                parsed = {
                    "sex": _parse_sex(line[80:81].decode("utf-8")),
                    "date_naissance": date_naissance,
                    "date_deces": date_deces,
                    "lieu_deces": lieu_deces,
                    "age": age,
                    "is_metro": _parse_int(lieu_deces, 99999) < 96000
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


def _import_pda2_file(conn, conf):
    fname = _get_conf_fname(conf)
    path = os.path.join(HERE, "data", fname)
    book = xlrd.open_workbook(path)
    sheet = book.sheet_by_name(conf["sheet"])
    rows = []
    # loop on rows
    # (with xlrd rows and columns start with 0)
    col_age = conf["cols"]["age"]-1
    col_nb = conf["cols"]["nb"]-1
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
    res = {}
    for row in range(conf["rows"]["debut"], conf["rows"]["fin"]):
        age = _parse_age(sheet.cell(row, col_age).value)
        nb = _parse_int(sheet.cell(row, col_nb).value)
        res[age] = nb
    rows = [{
        "annee": conf["annee"],
        "age": age,
        "nb": nb
    } for age, nb in res.items()]
    _db_bulk_insert(conn, "ages", rows)


@main.command("compute_taux_mortalite_par_age")
@click.argument("date_ranges", type=click.Choice(RANGES.keys()))
def cmd_compute_taux_mortalite_par_age(date_ranges):
    compute_taux_mortalite_par_age(date_ranges)

def compute_taux_mortalite_par_age(drkey):
    print(f"compute taux_mortalite_par_age {drkey}")
    _compute_taux_mortalite_par_age(RANGES[drkey], f'taux_mortalite_par_age_{drkey}.png')


def _compute_taux_mortalite_par_age(ranges, ofname):
    plt.clf()
    plt.title("[France] Taux de mortalité par âge")
    with _db_connect() as conn:
        age_range = list(range(1, 101))
        for dr in ranges:
            taux_mortalite_par_age = __compute_taux_mortalite_par_age(conn, dr["year"], dr["range"])
            plt.plot(age_range, [taux_mortalite_par_age.get(i, 0) for i in age_range], label=dr["name"])
    plt.legend()
    plt.savefig(os.path.join(HERE, f'results/{ofname}'))


def _select_pop_par_age(conn, annee):
    rows = conn.cursor().execute(
        '''SELECT age, SUM(nb) FROM ages WHERE annee = ? GROUP BY age''',
        [annee]
    )
    return {age:nb for age, nb in rows}


def _select_deces_par_age(conn, date_range):
    rows = conn.cursor().execute(
        '''SELECT age, count(*) FROM deces WHERE is_metro=true AND date_deces BETWEEN ? AND ? GROUP BY age''',
        [*date_range]
    )
    return {age: nb for age, nb in rows}


def __compute_taux_mortalite_par_age(conn, year, date_range):
    pop_par_age = _select_pop_par_age(conn, year)
    nb_deces_par_age = _select_deces_par_age(conn, date_range)
    _div = lambda a, b: a/b if b else 0
    return {age: _div(nb, pop_par_age.get(age)) for age, nb in nb_deces_par_age.items()}


@main.command("compute_deces_par_date")
@click.argument("date_ranges", type=click.Choice(RANGES.keys()))
def cmd_compute_deces_par_date(date_ranges):
    compute_deces_par_date(date_ranges)


def compute_deces_par_date(drkey, forecast_diff=False):
    print(f"compute deces_par_date {drkey}")
    plt.clf()
    plt.title("[France] Décès par date")
    with _db_connect() as conn:
        for dr in RANGES[drkey]:
            dates = _date_range_to_dates(dr["range"])
            deces_par_date = _select_deces_par_date(conn, dr["range"])
            plt.plot(range(len(dates)), [deces_par_date.get(d, 0) for d in dates], label=dr["name"])
    plt.legend()
    plt.savefig(os.path.join(HERE, f'results/deces_par_date_{drkey}.png'))


def _select_deces_par_date(conn, date_range):
    rows = conn.cursor().execute(
        '''SELECT date_deces, count(*) FROM deces WHERE is_metro=true AND date_deces BETWEEN ? AND ? GROUP BY date_deces''',
        [*date_range]
    )
    return {_to_dt(date_deces): nb for date_deces, nb in rows}


@main.command("compute_population_par_age")
@click.argument("date_ranges", type=click.Choice(RANGES.keys()))
def cmd_compute_population_par_age(drkey):
    compute_population_par_age(drkey)


def compute_population_par_age(drkey):
    print(f"compute population_par_age {drkey}")
    plt.clf()
    plt.title("[France] Population par âge")
    age_range = list(range(1, 101))
    with _db_connect() as conn:
        for dr in RANGES[drkey]:
            pop_par_age = _select_pop_par_age(conn, dr["year"])
            plt.plot(age_range, [pop_par_age.get(i, 0) for i in age_range], label=dr["year"])
    plt.legend()
    plt.savefig(os.path.join(HERE, f'results/population_par_age_{drkey}.png'))


@main.command("compute_deces_par_age")
@click.argument("date_ranges", type=click.Choice(RANGES.keys()))
@click.option("--simulate", is_flag=True)
@click.option("--cum-diff", is_flag=True)
def cmd_compute_deces_par_age(date_ranges, simulate, cum_diff):
    compute_deces_par_age(date_ranges, simulate=simulate, cum_diff=cum_diff)


def compute_deces_par_age(drkey, simulate=False, cum_diff=False):
    print(f"compute deces_par_age {drkey}")
    plt.clf()
    plt.title("[France] Décès par âge")
    age_range = list(range(1, 101))
    nb_deces_par_age = {}
    with _db_connect() as conn:
        for dr in RANGES[drkey]:
            name = dr["name"]
            nb_deces_par_age[name] = _select_deces_par_age(conn, dr["range"])
            plt.plot(age_range, [nb_deces_par_age[name].get(i, 0) for i in age_range], label=name)
        range1, range2 = RANGES[drkey][0], RANGES[drkey][1]
        name1, name2 = range1["name"], range2["name"]
        if simulate:
            nb_deces_par_age["simulation"] = _simulate_deces_par_age(conn, range1["year"], range1["range"], range2["year"])
            plt.plot(age_range, [nb_deces_par_age["simulation"].get(i, 0) for i in age_range], label=f"simulation: {range2['year']} population with {name1} mortality by age")
        if cum_diff:
            cum_diffs = _cum_diff_dicts(nb_deces_par_age[name1], nb_deces_par_age[name2])
            plt.plot(age_range, [cum_diffs.get(i, 0) for i in age_range], label=f"cum_diff: {name2} - {name1}")
            if simulate:
                sim_cum_diffs = _cum_diff_dicts(nb_deces_par_age["simulation"], nb_deces_par_age[name2])
                plt.plot(age_range, [sim_cum_diffs.get(i, 0) for i in age_range], label=f"cum_diff: {name2} - simulation")
    plt.legend()
    plt.savefig(os.path.join(HERE, f'results/deces_par_age_{drkey}.png'))


def _cum_diff_dicts(dict_1, dict_2):
    diffs = {
        k: dict_2[k] - dict_1.get(k,0)
        for k in dict_2.keys()
    }
    res = {}
    for k in sorted(diffs.keys()):
        res[k] = diffs[k] + res.get(k-1,0)
    return res


def _simulate_deces_par_age(conn, year1, range1, year2):
    taux_mort = __compute_taux_mortalite_par_age(conn, year1, range1)
    pop_par_age = _select_pop_par_age(conn, year2)
    return {
        age: taux_mort[age] * pop_par_age[age]
        for age in range(0, 101)
    }


@main.command("compute_taux_mortalite_standardise_par_age")
@click.argument("date_ranges", type=click.Choice(RANGES.keys()))
@click.option("--age-min", type=int, default=0)
def cmd_compute_taux_mortalite_standardise_par_age(date_ranges, age_min):
    compute_taux_mortalite_standardise_par_age(date_ranges, age_min=age_min)


def compute_taux_mortalite_standardise_par_age(drkey, age_min=0):
    print(f"compute taux_mortalite_standardise_par_age {drkey}")
    plt.clf()
    plt.title("[France] Taux de mortalité moyennée par âge")
    moyennes_mortalite = []
    with _db_connect() as conn:
        for dr in RANGES[drkey]:
            annee = dr["year"]
            pop_par_age = _select_pop_par_age(conn, annee)
            deces_par_age = _select_deces_par_age(conn, (f"{annee}-01-01", f"{annee}-12-31"))
            mortalite_par_age = {
                age: deces_par_age.get(age, 0) / pop if pop > 0 else 0
                for age, pop in pop_par_age.items()
                if age >= age_min
            }
            moyennes_mortalite.append(sum(mortalite_par_age.values()) / len(mortalite_par_age))
    plt.bar([dr["year"] for dr in RANGES[drkey]], moyennes_mortalite)
    plt.legend()
    plt.savefig(os.path.join(HERE, f'results/taux_mortalite_standardise_par_age_{drkey}.png'))


@main.command("compute_mortalite_par_annee")
@click.argument("date_range", type=click.Choice(RANGES.keys()))
def cmd_compute_mortalite_par_annee(date_ranges):
    compute_mortalite_par_annee(date_ranges)


def compute_mortalite_par_annee(drkey):
    print(f"compute mortalite_par_annee {drkey}")
    plt.clf()
    plt.title("[France] Mortalité")
    moyennes_mortalite = []
    with _db_connect() as conn:
        res = __compute_mortalite_par_annee(conn, [dr["year"] for dr in RANGES[drkey]])
        plt.bar(res.keys(),res.values())
        plt.legend()
        plt.savefig(os.path.join(HERE, f'results/mortalite_par_annee_{drkey}.png'))


def __compute_mortalite_par_annee(conn, annees):
    cur = conn.cursor()
    res = {}
    for annee in annees:
        row = cur.execute(
            '''SELECT count(*) FROM deces WHERE is_metro=true AND date_deces between ? and ?''',
            (str(annee), str(annee+1))
        ).fetchone()
        res[annee] = row[0]
    return res


@main.command("compute_mortality_forecast")
def cmd_compute_mortality_forecast():
    compute_mortality_forecast()


def compute_mortality_forecast():
    print("compute mortality_forecast")
    plt.clf()
    plt.title("[France] Prévision de mortalité")
    DEBUT_PREV = 2010
    with _db_connect() as conn:
        mortalite_reelle_par_annee = __compute_mortalite_par_annee(conn, range(DEBUT_PREV, 2020+1))
        taux_mortalite_par_age_moyen = _compute_taux_mortalite_par_age_moyen(conn, DEBUT_PREV, 2019)
        prev_morts = {}
        pop_par_age = _select_pop_par_age(conn, DEBUT_PREV)
        def _estimate_mort_par_age():
            return {
                age: floor(pop_par_age[age] * taux_mortalite_par_age_moyen[age])
                for age in range(0, 100+1)
            }
        mort_par_age = _estimate_mort_par_age()
        prev_morts[DEBUT_PREV] = sum(mort_par_age.values())
        for annee in range(DEBUT_PREV+1, 2050+1):
            pop_par_age[100] = max(0, pop_par_age[99] - mort_par_age[99]) + max(0, pop_par_age[100] - mort_par_age[100])
            for age in reversed(range(1, 99+1)):
                pop_par_age[age] = max(0, pop_par_age[age-1] - mort_par_age[age-1])
            mort_par_age = _estimate_mort_par_age()
            sum_morts = sum(mort_par_age.values())
            prev_morts[annee] = sum_morts
            # print(annee, mortalite_reelle_par_annee.get(annee, 0), sum_morts)
    plt.bar(mortalite_reelle_par_annee.keys(), mortalite_reelle_par_annee.values(), label="Mortalité réelle")
    plt.plot(prev_morts.keys(), prev_morts.values(), 'r', label="Prévision de mortalité")
    plt.legend()
    plt.savefig(os.path.join(HERE, 'results/prevision_morts.png'))
    # print("Ecart type", stdev([(prev_morts[annee]-mortalite_reelle_par_annee[annee]) for annee in range(DEBUT_PREV, 2020+1)]))


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


@main.command("compute_surmortality")
@click.option("--debut", default=2010)
def cmd_compute_surmortality(debut):
    compute_surmortality(debut=debut)

def compute_surmortality(debut=2010):
    print("compute surmortality")
    plt.clf()
    plt.title("[France] Surmortalité")
    DEBUT = 2010
    FIN_TAUX_MORTALITE = 2019
    FIN = 2020
    with _db_connect() as conn:
        mortalite_reelle_par_annee = __compute_mortalite_par_annee(conn, range(debut, FIN+1))
        taux_mortalite_par_age_moyen = _compute_taux_mortalite_par_age_moyen(conn, debut, FIN_TAUX_MORTALITE)
        pop_par_ages = {
            annee: _select_pop_par_age(conn, annee)
            for annee in range(debut, FIN+1)
        }
        mortalite_estimee_par_annee = {
            annee: sum(
                taux_mortalite_par_age_moyen[age] * pop_par_ages[annee][age]
                for age in range(0, 100+1)
            )
            for annee in range(debut, FIN+1)
        }
        surmortalite_par_annee = {
            annee: mortalite_reelle_par_annee[annee] - mortalite_estimee_par_annee[annee]
            for annee in range(debut, FIN+1)
        }
        surmortalite_stdev = stdev(surmortalite_par_annee.values())
    plt.bar(range(debut, FIN+1), surmortalite_par_annee.values(), label=f"Surmortalité (avec taux mortalité moyen depuis {debut})")
    plt.hlines(surmortalite_stdev, debut, FIN, colors='r')
    plt.legend()
    plt.savefig(os.path.join(HERE, f'results/surmortalite_{debut}.png'))


# parsing

class ParseError(Exception):
    pass

class ParseSexError(ParseError):
    pass

def _parse_sex(val):
    if val == "1": return "M"
    if val == "2": return "F"
    raise ParseSexError(f"Bad sex value: {val}")

def _parse_int(val, def_val=0):
    try:
        return int(val)
    except:
        return def_val

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


def _date_range_to_dates(date_range):
    res = []
    start, end = _to_dt(date_range[0]), _to_dt(date_range[1])
    day = start
    while day <= end:
        res.append(day)
        day += timedelta(days=1)
    return res


# utils

def _dt_to_annees(dt):
    return int(dt.days / 365.25)

def _db_bulk_insert(conn, table_name, values):
    if len(values) == 0:
        return
    conn.cursor().executemany(
        f"INSERT INTO {table_name} ({','.join(values[0].keys())}) VALUES ({','.join('?' for _ in range(len(values[0])))})",
        [list(v.values()) for v in values])

if __name__ == "__main__":
    main()