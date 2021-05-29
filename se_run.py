#!/usr/bin/env  python3
import os
import click
import sqlite3
from collections import defaultdict
import csv
import matplotlib.pyplot as plt
from statistics import mean

import utils

HERE = os.path.dirname(__file__)
DATA_DIR = os.path.join(HERE, "data", "se")

YEARS = list(range(1980, 2020+1))

DATA_SE_DEATHS_CONF = {
    "name": "se_deaths.csv",
    "src": "https://www.statistikdatabasen.scb.se/pxweb/en/ssd/START__BE__BE0101__BE0101I/DodaFodelsearK/"
}

DATA_SE_AGE_PYRAMIDS_CONFS = [{
    "year": year,
    "name": f"se_age_pyramid_{year}.csv",
    "src": f"https://www.populationpyramid.net/api/pp/752/{year}/?csv=true",
} for year in YEARS]


@click.group()
def main():
    pass


@main.command("all")
def all():
    _init_db()
    _import_data()
    _compute_mortality_meaned_by_age()


@main.command("import_data")
def import_data_cmd():
    _init_db()
    _import_data()


def _db_connect():
    return sqlite3.connect(os.path.join(HERE, "se_data.sqlite"))


def _init_db():
    with _db_connect() as conn:
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS deaths(year integer, age integer, nb integer)''')
        cur.execute('''DELETE FROM deaths''')
        cur.execute('''CREATE TABLE IF NOT EXISTS pop(year integer, cl_age text, nb integer)''')
        cur.execute('''DELETE FROM pop''')


def _import_data():
    with _db_connect() as conn:
        print(f"import {utils.get_conf_fname(DATA_SE_DEATHS_CONF)}")
        _import_deces_file(conn, DATA_SE_DEATHS_CONF)
        for conf in DATA_SE_AGE_PYRAMIDS_CONFS:
            _import_age_pyramid_file(conn, conf)


def _import_deces_file(conn, conf):
    fname = utils.get_conf_fname(conf)
    path = os.path.join(DATA_DIR, fname)
    deaths_by_year_age = defaultdict(int)
    with open(path, newline='') as csvf:
        for row in csv.DictReader(csvf, delimiter=','):
            age = utils.parse_digits(row["age"])
            for year in YEARS:
                deaths = row[str(year)]
                deaths_by_year_age[(year, age)] += int(deaths)
    utils.db_bulk_insert(conn, "deaths", [{
        "year": year,
        "age": age,
        "nb": nb,
    } for (year, age), nb in deaths_by_year_age.items()])


def _import_age_pyramid_file(conn, conf):
    fname = utils.get_conf_fname(conf)
    path = os.path.join(DATA_DIR, fname)
    year = conf["year"]
    db_rows = []
    with open(path, newline='') as csvf:
        for row in csv.DictReader(csvf, delimiter=','):
            db_rows.append({
                "year": year,
                "cl_age": row["Age"],
                "nb": int(row["M"]) + int(row["F"])
            })
    utils.db_bulk_insert(conn, "pop", db_rows)


@main.command("compute_mortalite_moyenne_par_age")
def compute_mortality_meaned_by_age():
    _compute_mortality_meaned_by_age()

def _compute_mortality_meaned_by_age():
    print("compute _compute_mortality_meaned_by_age")
    plt.clf()
    plt.title("[Sweden] Mortality meaned by age")
    def _parse_cl_age(val):
        if '-' in val:
            years = [int(v) for v in val.split('-')]
            return list(range(years[0], years[1]+1))
        else:
            return [utils.parse_digits(val)]
    mortality_by_year_clage = {}
    with _db_connect() as conn:
        pop_by_year_clage = _select_pop_by_year_clage(conn)
        cl_ages = set(clage for _, clage in pop_by_year_clage.keys())
        deaths_by_year_age = _select_deaths_by_year_age(conn)
        for year in YEARS:
            for cl_age in cl_ages:
                pop = pop_by_year_clage[(year, cl_age)]
                deaths = sum(deaths_by_year_age[(year, age)] for age in _parse_cl_age(cl_age))
                mortality_by_year_clage[(year, cl_age)] = deaths / pop if (pop > 0) else 0
    mean_mortality_by_year = [
        mean(
            mortality_by_year_clage[(year, cl_age)]
            for cl_age in cl_ages
        )
        for year in YEARS
    ]
    plt.bar(YEARS, mean_mortality_by_year)
    plt.legend()
    plt.savefig(os.path.join(HERE, 'results/se_mortality_meaned_by_age.png'))


def _select_pop_by_year_clage(conn):
    rows = conn.cursor().execute(
        '''SELECT year, cl_age, SUM(nb) FROM pop GROUP BY year, cl_age'''
    )
    return {(year, cl_age):nb for year, cl_age, nb in rows}

def _select_deaths_by_year_age(conn):
    rows = conn.cursor().execute(
        '''SELECT year, age, SUM(nb) FROM deaths GROUP BY year, age'''
    )
    return {(year, age):nb for year, age, nb in rows}


if __name__ == "__main__":
    main()