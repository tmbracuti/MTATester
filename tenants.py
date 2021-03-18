import pyodbc
import properties
# driver = 'SQL Server Native Client 11.0'
sf_tenant_db = 'DRIVER={driver_plug};SERVER=192.168.52.14,1416;DATABASE=sf_tenant;UID=sa;PWD=Starfish098'


def get_tenant_map(mapping, props: properties.Properties):
    try:
        qry = "select external_tenant_id,private_db from sf_tenant"
        driver = props.get_value('driver', 'SQL Server')
        c = pyodbc.connect(sf_tenant_db.replace('driver_plug', driver))
        cur = c.cursor()
        cur.execute(qry)
        rows = cur.fetchall()
        for row in rows:
            new_conn = translate_to_odbc(row[1], driver)
            if new_conn is not None:
                mapping[row[0]] = new_conn
        c.close()

    except Exception as db_e:
        print(str(db_e))
        return None


def translate_to_odbc(conn_str, driver):
    toks = conn_str.split(';')
    if len(toks) < 6:
        return None
    d = {}
    for t in toks:
        ind = t.find('=')
        if t != -1:
            attr = t[0:ind].lower()
            val = t[ind+1:]
            d[attr] = val
    server = d['server'].strip()
    uid = d['user id'].strip()
    pwd = d['password'].strip()
    db = d['initial catalog']
    odbc_conn = 'DRIVER=%s;SERVER=%s;DATABASE=%s;UID=%s;PWD=%s' % (driver, server, db, uid, pwd)
    return odbc_conn
