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
import csv
import numpy as np
from scipy import optimize

HERE = os.path.dirname(__file__)
DATA_PATH = os.path.join(HERE, "../data")
DB_PATH = os.path.join(HERE, "data.sqlite")

def _to_dt(date):
    return datetime.strptime(date, '%Y-%m-%d')

def _add_days(date, days):
    dt = _to_dt(date)
    dt += timedelta(days=days)
    return dt.strftime('%Y-%m-%d')

def _date_range(date, days):
    return (date, _add_days(date, days))


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
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20211012-093424/deces-2021-t3.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20211118-093353/deces-2021-m10.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20211215-093836/deces-2021-m11.txt",
    "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20220106-161749/deces-2021-m12.txt",
]

PDA_CONFS = [
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
]

METEO_SRC_FILE = "https://public.opendatasoft.com/explore/dataset/donnees-synop-essentielles-omm/download/?format=csv&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B"

START_DATE = FIRST_DATE = "2010-01-01"
END_DATE = LAST_DATE = "2021-12-31"


def _get_conf_fname(conf):
    return conf.get("name") or os.path.basename(conf["src"])


@click.group()
def main():
    pass


@main.command("all")
@click.option("--import", "do_import", type=bool, default=True)
def cmd_all(do_import):
    if do_import:
        download_data()
        import_data()


def db_connect():
    return sqlite3.connect(DB_PATH)


def init_db(name=None):
    with db_connect() as conn:
        cur = conn.cursor()
        if name in (None, "deces"):
            cur.execute('''CREATE TABLE IF NOT EXISTS deces(sex text, date_naissance text, date_deces text, lieu_deces text, dep text, age integer, is_metro bool)''')
            cur.execute('''DELETE FROM deces''')
        if name in (None, "pda"):
            cur.execute('''CREATE TABLE IF NOT EXISTS ages(annee integer, age integer, nb integer)''')
            cur.execute('''DELETE FROM ages''')
        if name in (None, "meteo"):
            cur.execute('''CREATE TABLE IF NOT EXISTS meteo(date text, dep text, temperature float)''')
            cur.execute('''DELETE FROM meteo''')


@main.command("download_data")
def cmd_download_data_cmd():
    download_data()


def download_data():
    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH)
    for url in DECES_FILES_SRC:
        _download_data_file(url, os.path.join(DATA_PATH, url.split('/')[-1]))
    for conf in PDA_CONFS:
        _download_data_file(conf["src"], os.path.join(DATA_PATH, _get_conf_fname(conf)))
    _download_data_file(METEO_SRC_FILE, os.path.join(DATA_PATH, "meteo.csv"))


def _download_data_file(url, ofpath):
    if not os.path.exists(ofpath):
        print(f'Download {os.path.basename(ofpath)}... ', end='')
        sys.stdout.flush()
        urllib.request.urlretrieve(url, ofpath)
        print(f'DONE')


@main.command("import_data")
@click.option("--name")
def cmd_import_data(name):
    init_db(name=name)
    import_data(name=name)


def import_data(name=None):
    with db_connect() as conn:
        if name in (None, "deces"):
            for src in DECES_FILES_SRC:
                fname = os.path.basename(src)
                print(f"import {fname}")
                _import_deces_file(conn, fname)
        if name in (None, "pda"):
            for conf in PDA_CONFS:
                if conf["type"] == "pyramide-des-ages":
                    _import_pda_file(conn, conf)
                if conf["type"] == "pyramide-des-ages-2":
                    _import_pda2_file(conn, conf)
        if name in (None, "meteo"):
            print(f"import meteo")
            _import_meteo_file(conn)


def _import_deces_file(conn, fname):
    fpath = os.path.join(DATA_PATH, fname)
    with open(fpath, 'rb') as file:
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
                    "dep": lieu_deces[:2],
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
    path = os.path.join(DATA_PATH, fname)
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
    path = os.path.join(DATA_PATH, fname)
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


def _import_meteo_file(conn):
    fpath = os.path.join(DATA_PATH, "meteo.csv")

    def parse_float(val):
        try:
            return float(val)
        except ValueError:
            return None

    values = []
    with open(fpath, newline='') as csvf:
        for row in csv.DictReader(csvf, delimiter=';'):
            date = row["Date"][:10]
            if date < FIRST_DATE or date > LAST_DATE: continue
            temp = parse_float(row["Température (°C)"])
            if temp is None: continue
            # values.append({
            #     "date": date,
            #     "dep": row["department (code)"][:2],
            #     "temperature": temp,
            # })
            key = (date, row["department (code)"])
            if key not in values: values[key] = []
            values[key].append(temp)
    values_meaned = (
        {
            "date": date,
            "dep": dep,
            "temperature": mean(vals),
        }
        for (date, dep), vals in values.items()
    )
    for vals in _get_by_paquet(values, 1000):
        _db_bulk_insert(conn, "meteo", values_meaned)


@main.command("plot_mortalite_par_temperature")
@click.option("--ages")
def cmd_plot_mortalite_par_temperature(ages):
    with db_connect() as conn:
        plot_mortalite_par_temperature(conn, ages=ages.split('-') if ages else None)


def plot_mortalite_par_temperature(conn, ages=None):
    dates = _get_all_dates(START_DATE, END_DATE)
    years = sorted(set(d[:4] for d in dates))
    ages = range(0, 100+1)
    deaths_by_date_dep_age = {
        (date, dep, age): deaths
        for date, dep, age, deaths in conn.execute(
            "SELECT date_deces, dep, age, count(*) FROM deces WHERE is_metro=true AND date_deces between ? and ? GROUP BY date_deces, dep, age",
            [START_DATE, END_DATE]
        )
    }
    deps = sorted(set(dep for _, dep, _ in deaths_by_date_dep_age.keys()))
    summer_dates_by_year = {
        year: [
            date
            for date in dates
            if date > f"{year}-06" and date < f"{year}-08"
        ]
        for year in years
    }
    summer_deaths_by_year_age_dep_age = {
        (year, dep, age): mean(
            deaths_by_date_dep_age.get((date, dep, age),0)
            for date in summer_dates_by_year[year]
        )
        for dep in deps
        for year in years
        for age in ages
    }
    print("TMP summer_deaths_by_year_age_dep_age", list(summer_deaths_by_year_age_dep_age.values())[:0])
    temps_by_date_dep = {
        (date, dep): temp
        for date, dep, temp in conn.execute(
            f"select date, dep, AVG(temperature) from meteo group by date, dep"
        )
    }
    death_refpairs_by_age_temp = {}
    for date in dates:
        for dep in deps:
            if (date, dep) in temps_by_date_dep:
                temp = int(temps_by_date_dep[(date, dep)])
                for age in ages:
                    deaths = deaths_by_date_dep_age.get((date, dep, age), 0)
                    summer_deaths = summer_deaths_by_year_age_dep_age.get((date, dep, age), 0)
                    death_refpairs_by_age_temp.setdefault((age, temp), []).append((deaths, summer_deaths))
    death_factors_by_age_temp = {
        (age, temp): float(sum(v for v, _ in refpairs)) / max(sum(v for _, v in refpairs), 1)
        for (age, temp), refpairs in death_refpairs_by_age_temp.items()
    }
    temps = sorted(set(t for _, t in death_factors_by_age_temp.keys()))

    plt.clf()
    title = ["[France] Facteur de mortalité par age et température"]
    #if ages: title.append(f"(Ages: {'-'.join(ages)})")
    plt.title(" ".join(title))
    for age in range(40, 90+1, 10):
        plt.plot(temps, [death_factors_by_age_temp.get((age, t)) for t in temps], label=age)
    plt.legend()
    figname = ["facteur_mortalite_par_age_temperature"]
    #if ages: figname.append(f"ages_{'_'.join(ages)}")
    plt.savefig(os.path.join(HERE, f'results/{"_".join(figname)}.png'))




    # deps = [
    #     dep
    #     for dep in deps
    #     if min(summer_deaths_by_year_dep[(year, dep)] for year in years) >= 10
    # ]
    # death_factors_by_date_dep = {
    #     (date, dep): deaths_by_date_dep.get((date, dep),0) / summer_deaths_by_year_dep[(date[:4], dep)]
    #     for date in dates
    #     for dep in deps
    # }
    # temps_death_factors = [
    #     (temps_by_date_dep[(date, dep)], deathf)
    #     for (date, dep), deathf in death_factors_by_date_dep.items()
    #     if (date, dep) in temps_by_date_dep
    # ]

    # plt.clf()
    # title = ["[France] Mortalité estivale par année et département"]
    # #if ages: title.append(f"(Ages: {'-'.join(ages)})")
    # plt.title(" ".join(title))
    # for dep in deps:
    #     plt.plot(years, [summer_deaths_by_year_dep.get((year, dep),0) for year in years])
    # plt.legend()
    # figname = ["mortalite_estivale"]
    # #if ages: figname.append(f"ages_{'_'.join(ages)}")
    # plt.savefig(os.path.join(HERE, f'results/{"_".join(figname)}.png'))

    # plt.clf()
    # title = ["[France] Mortalité par date et département"]
    # #if ages: title.append(f"(Ages: {'-'.join(ages)})")
    # plt.title(" ".join(title))
    # for dep in deps:
    #     plt.plot(dates, [deaths_by_date_dep.get((date, dep),0) for date in dates])
    # plt.legend()
    # figname = ["mortalite"]
    # #if ages: figname.append(f"ages_{'_'.join(ages)}")
    # plt.savefig(os.path.join(HERE, f'results/{"_".join(figname)}.png'))

    # plt.clf()
    # title = ["[France] Facteur de mortalité par date et département"]
    # #if ages: title.append(f"(Ages: {'-'.join(ages)})")
    # plt.title(" ".join(title))
    # for dep in deps:
    #     plt.plot(dates, [death_factors_by_date_dep.get((date, dep),0) for date in dates])
    # plt.legend()
    # figname = ["facteur_de_mortalite"]
    # #if ages: figname.append(f"ages_{'_'.join(ages)}")
    # plt.savefig(os.path.join(HERE, f'results/{"_".join(figname)}.png'))

    # plt.clf()
    # title = ["[France] Mortalité par température"]
    # #if ages: title.append(f"(Ages: {'-'.join(ages)})")
    # plt.title(" ".join(title))
    # plt.scatter([t for t, _ in temps_death_factors], [m for _, m in temps_death_factors], s=5)
    # plt.legend()
    # figname = ["mortalite_par_temperature"]
    # #if ages: figname.append(f"ages_{'_'.join(ages)}")
    # plt.savefig(os.path.join(HERE, f'results/{"_".join(figname)}.png'))


def plot_mortalite_par_temperature_old(ages=None):
    first_date = FIRST_DATE
    last_date = LAST_DATE
    with db_connect() as conn:
        standard2_mortality_by_date = comp_standard2_mortality_by_date(conn, first_date, last_date, ages=ages)
        temps_by_date = comp_temps_by_date(conn)
        mortality_by_temp = comp_mortalite_par_temperature(conn, first_date, last_date, temps_by_date, standard2_mortality_by_date)

    plt.clf()
    fig, axs = plt.subplots(2)
    title = ["[France] Mortalité et température"]
    if ages: title.append(f"(Ages: {'-'.join(ages)})")
    fig.suptitle(" ".join(title))
    axs[0].plot(temps_by_date.keys(), temps_by_date.values(), label="Temperature")
    axs[1].plot(standard2_mortality_by_date.keys(), standard2_mortality_by_date.values(), label="Mortalite")
    plt.legend()
    figname = ["mortalite_et_temperature"]
    if ages: figname.append(f"ages_{'_'.join(ages)}")
    plt.savefig(os.path.join(HERE, f'results/{"_".join(figname)}.png'))

    plt.clf()
    title = ["[France] Mortalité par température"]
    if ages: title.append(f"(Ages: {'-'.join(ages)})")
    plt.title(" ".join(title))
    plt.scatter([t for _, t in mortality_by_temp], [m for m, _ in mortality_by_temp], s=5)
    plt.legend()
    figname = ["mortalite_par_temperature"]
    if ages: figname.append(f"ages_{'_'.join(ages)}")
    plt.savefig(os.path.join(HERE, f'results/{"_".join(figname)}.png'))


def comp_standard2_mortality_by_date(conn, first_date, last_date, ages=None):
    first_year = int(first_date[:4])
    last_year = int(last_date[:4])
    standard_mortality_by_date = _compute_standard_mortality_by_date(conn, first_year, last_year, ages=ages)
    ref_mortality_by_year = {
        year: mean(
            standard_mortality_by_date[date]
            for date in standard_mortality_by_date.keys()
            if date >= f"{year}-06" and date < f"{year}-08"
        )
        for year in range(first_year, last_year+1)
    }
    return {
        date: standard_mortality_by_date[date] / ref_mortality_by_year[int(date[:4])]
        for date in standard_mortality_by_date.keys()
    }


def comp_temps_by_date(conn, agg='avg'):
    return {
        date: temp
        for date, temp in conn.execute(
            f"select date, {agg}(temperature) from meteo group by date"
        )
    }

def comp_temps_by_date_dep(conn, agg='avg'):
    return {
        (date, dep): temp
        for date, dep, temp in conn.execute(
            f"select date, dep, {agg}(temperature) from meteo group by date, dep"
        )
    }


def comp_mortality_by_dep(conn):
    return {
        dep: nb
        for dep, nb in conn.execute(
            f"select dep, count(*) from deces group by dep"
        )
    }


def comp_mortalite_par_temperature(conn, first_date, last_date, temps_by_date, standard2_mortality_by_date, date_delta=0):
    temps_by_date = comp_temps_by_date(conn)
    return [
        (standard2_mortality_by_date[date], temps_by_date[_add_days(date, -date_delta)])
        for date in standard2_mortality_by_date.keys()
        if date >= _add_days(first_date, date_delta) and last_date <= last_date
    ]


@main.command("estimate_mortalite_par_temperature")
@click.option("--date-delta", default=0)
@click.option("--ages")
def cmd_estimate_mortalite_par_temperature(date_delta, ages):
    with db_connect() as conn:
        estimate_mortalite_par_temperature(conn, date_delta=date_delta, ages=ages.split('-') if ages else None)

def estimate_mortalite_par_temperature(conn, date_delta=0, ages=None):
    standard2_mortality_by_date = comp_standard2_mortality_by_date(conn, FIRST_DATE, LAST_DATE, ages=ages)
    temps_by_date = comp_temps_by_date(conn)
    mortalite_par_temperature = comp_mortalite_par_temperature(conn, FIRST_DATE, LAST_DATE, temps_by_date, standard2_mortality_by_date, date_delta=date_delta)

    temps = [t for _, t in mortalite_par_temperature]
    mortalites = [m for m, _ in mortalite_par_temperature]

    #def piecewise_linear(x, x0, y0, k1):
    #    return np.piecewise(x, [x < x0], [lambda x:k1*(x-x0) + y0, lambda x:y0])
    def piecewise_linear(x, a, b, c):
        return a * x * x + b * x + c

    popt, pcov = optimize.curve_fit(piecewise_linear, np.array(temps), np.array(mortalites))#, p0=[15.0, 1.0, -0.05])
    print("TMP popt", popt)

    plt.clf()
    plt.title("[France] Mortalité par température")
    plt.scatter(temps, mortalites, s=5, alpha=0.3)
    xd = np.linspace(min(temps), max(temps), 100)
    plt.plot(xd, piecewise_linear(xd, *popt), color="red")
    plt.legend()
    fname = ["mortalite_par_temperature_est"]
    if date_delta: fname.append(f"delta{date_delta}")
    if ages: fname.append(f"ages_{'_'.join(ages)}")
    plt.savefig(os.path.join(HERE, f'results/{"_".join(fname)}.png'))

    dates = standard2_mortality_by_date.keys()
    min_date = min(dates)

    plt.clf()
    plt.title("[France] Mortalité Réelle VS Estimée")
    plt.figure(figsize=(50, 3))
    plt.plot(dates, [standard2_mortality_by_date[d] for d in dates], label="réelle")
    temps_by_date_dep = comp_temps_by_date_dep(conn)
    deps = set(dep for _, dep in temps_by_date_dep.keys())
    mortality_by_dep = comp_mortality_by_dep(conn)
    weigth_temps_by_date = {
        date: weighted_mean([
            (temps_by_date_dep[(date, dep)], mortality_by_dep.get(dep, 0))
            for dep in deps
            if (date, dep) in temps_by_date_dep
        ])
        for date in dates
    }
    temps = np.array([
        weigth_temps_by_date[max(min_date, _add_days(d, -date_delta))]
        for d in dates
    ])
    plt.plot(dates, piecewise_linear(temps, *popt).tolist(), label="estimée")
    plt.legend()
    fname = ["mortalite_reelle_vs_est"]
    if date_delta: fname.append(f"delta{date_delta}")
    if ages: fname.append(f"ages_{'_'.join(ages)}")
    plt.savefig(os.path.join(HERE, f'results/{"_".join(fname)}.png'))


def _select_pop_par_age(conn, annee):
    rows = conn.execute(
        '''SELECT age, SUM(nb) FROM ages WHERE annee = ? GROUP BY age''',
        [annee]
    )
    return {age:nb for age, nb in rows}


def _compute_standard_mortality_by_date(conn, first_year, last_year, ages=None):
    deces_standard_par_date = {}
    last_pop_par_age = _select_pop_par_age(conn, last_year)
    ages_sql = "AND age between ? and ?" if ages else ""
    ages_sql_args = ages if ages else []
    for year in range(first_year, last_year+1):
        pop_par_age = _select_pop_par_age(conn, year)
        deces_par_date_age = {
            (date, age): val
            for date, age, val in conn.execute(
                f"SELECT date_deces, age, count(*) FROM deces WHERE is_metro=true AND date_deces between ? and ? {ages_sql} GROUP BY date_deces, age",
                [str(year), str(year+1)] + ages_sql_args
            )
        }
        dates = sorted(set(d for (d, _) in deces_par_date_age.keys()))
        deces_standard_par_date_age = {
            (date, age): val * last_pop_par_age.get(age, 0) / pop_par_age.get(age, 1)
            for (date, age), val in deces_par_date_age.items()
        }
        deces_standard_par_date.update({
            date: sum(
                deces_standard_par_date_age.get((date, age),0)
                for age in range(0, 100+1)
            )
            for date in dates
        })
    return deces_standard_par_date


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

def _get_all_dates(start_date, end_date):
    date = datetime.strptime(start_date, "%Y-%m-%d")
    _end_date = datetime.strptime(end_date, "%Y-%m-%d")
    res = []
    while date <= _end_date:
        res.append(date.strftime("%Y-%m-%d"))
        date += timedelta(days=1)
    return res

def _dt_to_annees(dt):
    return int(dt.days / 365.25)

def _db_bulk_insert(conn, table_name, values):
    if len(values) == 0:
        return
    conn.cursor().executemany(
        f"INSERT INTO {table_name} ({','.join(values[0].keys())}) VALUES ({','.join('?' for _ in range(len(values[0])))})",
        [list(v.values()) for v in values])

def _add_days(date_str, n):
    if n == 0: return date_str
    date = datetime.strptime(date_str, "%Y-%m-%d")
    date += timedelta(days=n)
    return date.strftime("%Y-%m-%d")

def _get_by_paquet(ite, size):
    paquet = []
    for val in ite:
        paquet.append(val)
        if len(paquet) >= size:
            yield paquet
            paquet.clear()
    if paquet:
        yield paquet

def weighted_mean(vals):
    total = sum(a*b for a, b in vals)
    total_weigths = sum(b for _, b in vals)
    return total / total_weigths

if __name__ == "__main__":
    main()