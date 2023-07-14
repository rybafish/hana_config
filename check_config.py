import sys
import argparse

hana_user_key = None        # This is hdbuserstore entry, not a user name

#dict of changes
#each element is a tuple of three (four?) elements: new, old, alter statement, ports?
changes = {}

# table name to store the configuration baseline
table_name = 'CONFIG'

# set it to true for the initial setup/troubleshooting
verbose = False

def log (str, nocheck = False):
    if verbose or nocheck:
        print(str)

from hdbcli import dbapi
log('hdbcli import fine...')

def connect():
    '''opens SAP HANA connection checks SID and tenant (must be SystemDB)'''
    conn = None
    cursor = None
    
    try:
        conn = dbapi.connect(userkey=hana_user_key, autocommit=False)
    except dbapi.Error as ex:
        print('SAP HANA Connection error: %s' % (ex.errortext))
        
        if ex.errorcode == 10: # Auth error: need to exit
            exit(1)
        elif ex.errorcode == 414: # user is forced to change password: need to exit
            exit(1)
            
    cursor = conn.cursor()
    
    cursor.execute("select distinct value from m_host_information where key = 'sid'")
    rows = cursor.fetchmany(10)
    
    if len(rows) != 1:
        print('Unexpected number of rows returned for SID query, aborting')
        exit(2)
        
    sid = rows[0][0]
    
    cursor.execute("select database_name from m_database")
    rows = cursor.fetchmany(10)

    if len(rows) != 1:
        print('Unexpected number of rows returned for tenant query, aborting')
        exit(2)

    tenant = rows[0][0]
    log('Connected to %s@%s' % (tenant, sid))
    
    if tenant.lower() != 'systemdb':
        print('Connection needs to be opened to the SystemDB, check the HDBUSERSTORE entry %s' % (hana_user_key))
        exit(2)
        
    return conn, cursor

def check_table(cursor):
    """Checks if the configuration table exists.
    
    Table name - CONFIG"""
    
    check_sql = "select count(*) from tables where schema_name = session_user and table_name = '%s'" % (table_name)
    
    cursor.execute(check_sql)
    
    rows = cursor.fetchall()
    
    cnt = rows[0][0]
    
    return True if cnt == 1 else False

def fill_table(cursor, cleanup):
    """(re)Fills the config table with initial configuration"""
    
    insert_sql = '''insert into %s (file_name, layer_name, section, key, database_name, host, port, value) 
    (select file_name, layer_name, section, key, database_name, host, port, value from sys_databases.m_configuration_parameter_values
    where layer_name != 'DEFAULT'
    order by database_name, file_name, section, key, host
    )''' % table_name

    truncate_sql = 'truncate table %s' % table_name
    
    if cleanup:
        cursor.execute(truncate_sql)
        
    cursor.execute(insert_sql)
    
def create_table(cursor):
    """Creates configuration table structure"""
    
    table_sql = '''create column table %s (
    host              nvarchar(64),
    file_name         nvarchar(256),
    section           nvarchar(128),
    key               nvarchar(128),
    layer_name        nvarchar(8),
    database_name     nvarchar(256),
    port              integer,
    value             nvarchar(5000)
    )''' % table_name
   
    cursor.execute(table_sql)
        
def validate_output(desc):
    
    if desc[0][0] != 'HOST':
        return False

    if desc[1][0] != 'FILE_NAME':
        return False

    if desc[2][0] != 'SECTION':
        return False

    if desc[3][0] != 'KEY':
        return False

    if desc[4][0] != 'LAYER_NAME':
        return False

    if desc[5][0] != 'DATABASE_NAME':
        return False

    if desc[6][0] != 'PORT':
        return False

    if desc[7][0] != 'VALUE_OLD':
        return False

    if desc[8][0] != 'VALUE_NOW':
        return False
        
    return True
    
def process_changes():
    log('\nChanges detected:', True)
    for param_key in changes:
        log('%s, %s -> %s' % (param_key, changes[param_key]['value_old'], changes[param_key]['value_now']), True)

    log('\nImplementation:', True)
    for param_key in changes:
        log(changes[param_key]['alter'], True)
        
    log('\nDone.', True)

def collect_changes(rows):
    '''Single changes processor for both extractor functions'''

    for r in rows:
        host, file_name, section, key, layer_name, database_name, port, value_old, value_now = r
        
        param = '%s - [%s] - %s' % (file_name, section, key)
        
        if layer_name == 'SYSTEM':
            layer = 'system'
            layer_alter = "'SYSTEM'"
        elif layer_name == 'DATABASE':
            layer = 'DB (%s)' % database_name
            layer_alter = "'DATABASE', '%s'"  % database_name
        elif layer_name == 'HOST':
            layer = 'host (%s)' % host
            layer_alter = "'HOST', '%s'"  % host
        else:
            print('[E] Unexpected layer name: %s' % layer_name)
            exit(5)
        
        if value_old is None:
            alter = "alter system alter configuration ('%s', %s) unset ('%s','%s') with reconfigure;" % (file_name, layer_alter, section, key)
        else:
            alter = "alter system alter configuration ('%s', %s) set ('%s','%s') = '%s' with reconfigure;" % (file_name, layer_alter, section, key, value_old)

        param_key = '%s, %s' % (param, layer)
        
        if param_key in changes:
            # need to check if old_new pair is the same
            pass
        else:
            changes[param_key] = {'param_key': param_key, 'value_old': value_old, 'value_now': value_now, 'alter': alter}
    
def detect_changes():
    """This function gets list of 
        - changed one non-default to the other non-default
        - unset parameters: existed non-default --> default (or disappeared at all)
    """
    
    changes_sql = '''select
            cb.host, cb.file_name, cb.section, cb.key, cb.layer_name, cb.database_name, cb.port, 
            cb.value value_old, cn.value value_now
        from %s CB
        left outer join sys_databases.m_configuration_parameter_values CN
        on 
            cb.host = cn.host
            and cb.file_name = cn.file_name
            and cb.section = cn.section
            and cb.key = cn.key
            and cb.layer_name = cn.layer_name
            and cb.database_name = cn.database_name
            and cb.port = cn.port
        where 
            (cb.value != cn.value and (cb.value is not null and cn.value is not null))
            or (cn.value is null and cb.layer_name != 'DEFAULT')''' % table_name
    
    cursor.execute(changes_sql)
    rows = cursor.fetchall()
    
    if validate_output(cursor.description) == False:
        print('[E] Unexpected select output structure')
        exit(4)
    
    collect_changes(rows)
    
def detect_news():
    """This function gets list of 
        - changed from default to non-default, new values
    """
    
    news_sql = '''select
            cn.host, cn.file_name, cn.section, cn.key, cn.layer_name, cn.database_name, cn.port, 
            cb.value value_old, cn.value value_now
        from %s CB
        right outer join sys_databases.m_configuration_parameter_values CN
        on 
            cb.host = cn.host
            and cb.file_name = cn.file_name
            and cb.section = cn.section
            and cb.key = cn.key
            and cb.layer_name = cn.layer_name
            and cb.database_name = cn.database_name
            and cb.port = cn.port
        where 
            (cb.value is null and cn.layer_name != 'DEFAULT')''' %table_name

    cursor.execute(news_sql)
    rows = cursor.fetchall()
    
    if validate_output(cursor.description) == False:
        print('[E] Unexpected select output structure')
        exit(4)
    
    collect_changes(rows)

def check_config():
    """Checks if the actual configuration has any deviations compared to CONFIG table"""
    
    detect_changes()
    detect_news()    
    
def help():
        print('''SAP HANA Configuration check.
        
Example:
    check_config.py -k=HDBKEY -m=init
    
    This example will create the initial configuration of the database configured with HDBKEY.
    
    --help for help.
''')

if __name__ == '__main__':    
    
    if len(sys.argv) < 2:
        help()
        exit(1)

    parser = argparse.ArgumentParser(description="SAP HANA Configuration check.")
    
    parser.add_argument('-k', required=True, help='hdbuserstore entry to be used, this is a mandatory parameter')
    parser.add_argument('-m', default='check', required=False, help='optional execution mode: (init|check|reset). The default is "check".')
    
    args = parser.parse_args()
    
    hana_user_key = args.k

    conn, cursor = connect()
    
    if conn is None:
        print('Connection error')
        exit(1)
        
    if args.m == 'check':
        if not check_table(cursor):
            log('\n[Error] The configuration table %s does not exit. Execute with -m=init first.' % table_name, True)
            exit(1)

        check_config()
        
        if len(changes) == 0:
            log('No changes detected.', True)
        else:
            process_changes()
            
    elif args.m == 'init':
        if not check_table(cursor):
            create_table(cursor)
            log('Configuration table %s created...' % table_name, True)
            fill_table(cursor, False)
            conn.commit()
            log('Initial configuration stored.', True)
            exit(0)
        else:
            log('The configuration table %s already exists, aborting. If you need to renew the baseline run with -m=renew.' % table_name, True)
            exit(1)
            
    elif args.m == 'renew':
        fill_table(cursor, True)
        conn.commit()
        log('Configuration renewed.', True)
        exit(0)
    else:
        log('Unknown mode, check the help.', True)
        exit(1)
        

