#! python3
''' store qty of pallete for exp and brak '''

import pyodbc
from filuet_log import filuet_log

log = filuet_log(app_name='STOCK_EXP_BRAK')

def conn_str():
    ''' connection string '''
    # return 'DRIVER={SQL Server};SERVER=RU-LOB-SQL01;DATABASE=ExchangeDB;Integrated Security=SSPI;'
    return 'DRIVER={SQL Server};SERVER=RU-LOB-SQL01;DATABASE=FiluetWH;UID=exuser;PWD=good4you'

def start_process():
    log.INFO('Start process!')
    sql = 'EXEC [dbo].[add_pal_qty_brak_exp]'
    execute_sql(sql)

def execute_sql(sql):
    ''' execute sql command on db '''
    log.DEBUG(sql)
    try:
        with pyodbc.connect(conn_str()) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            cursor.commit()
            return 1
    except Exception as ex:
        log.ERROR('{0}'.format(ex))
        return None

if __name__ == '__main__':
    start_process()
