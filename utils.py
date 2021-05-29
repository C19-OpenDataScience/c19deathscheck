import os

def get_conf_fname(conf):
    return conf.get("name") or os.path.basename(conf["src"])

def db_bulk_insert(conn, table_name, values):
    if len(values) == 0:
        return
    conn.cursor().executemany(
        f"INSERT INTO {table_name} ({','.join(values[0].keys())}) VALUES ({','.join('?' for _ in range(len(values[0])))})",
        [list(v.values()) for v in values])

def parse_digits(val):
    digits = [d for d in val if d.isdigit()]
    return int(''.join(digits))