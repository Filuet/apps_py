#! python3
''' HERBALIFE Import text file with inbound order from oracle '''
import logging
import os
import csv
import datetime
import zipfile
import pyodbc


def conn_str():
    ''' connection string '''
    # return 'DRIVER={SQL Server};SERVER=RU-LOB-SQL01;DATABASE=ExchangeDB;Integrated Security=SSPI;'
    return 'DRIVER={SQL Server};SERVER=RU-LOB-SQL01;DATABASE=ExchangeDB;UID=exuser;PWD=good4you'


def init_logger():
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    handler = logging.FileHandler('in_order_import.log', 'a', 'utf-8')
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s\t%(funcName)s() <%(lineno)s> %(message)s')
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


def start_import():
    ''' let's do it '''
    try:
        init_logger()
        logging.info('START')
        path_source, path_backup = path_init()
        process_files(path_source, path_backup)
    except Exception as ex:
        logging.error(ex)
    finally:
        logging.info('EXIT')


def path_init():
    ''' Initialization of used paths to work with files '''
    try:
        path_source = os.path.join(os.getcwd(), 'Ports\\InOrder')
        path_backup = os.path.join(os.getcwd(), 'Backup\\InOrder')
        os.makedirs(path_source, exist_ok=True)
        os.makedirs(path_backup, exist_ok=True)
        logging.debug('Path Source: ' + path_source)
        logging.debug('Path Backup: ' + path_backup)
        return path_source, path_backup
    except Exception as ex:
        logging.error(ex)
        raise ex


def read_file(file_name):
    ''' read csv file to dict '''
    ord_list = []
    with open(file_name, 'r') as file:
        rows = csv.DictReader(file)
        for row in rows:
            ord_list.append(row)
    return ord_list


def add_new_ExMsg(header_id):
    ''' add new ex msg and ex msg status to db '''
    try:
        sql = '''
        SET NOCOUNT ON;
        DECLARE @TID TABLE(ID INT);
        INSERT INTO ExchangeDB.dbo.ExMsg
            (
            TypeID
            , ObjectID
            , HighPriority
            )
        OUTPUT
            inserted.ID
            INTO @TID
        VALUES (8, {}, 0);
        SELECT * FROM @TID;
        '''.format(header_id)
        logging.debug(sql)
        with pyodbc.connect(conn_str()) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            msg_id = cursor.fetchall()[0][0]
            cursor.commit()
        if msg_id > 0:
            insert_new_status(msg_id, 0)
        else:
            raise 'Error get ExMsgId'
    except pyodbc.DatabaseError as ex:
        logging.error(ex.msg)
        raise ex


def insert_new_status(msg_id, msg_status):
    ''' insert status for new ex msg '''
    with pyodbc.connect(conn_str()) as conn:
        cursor = conn.cursor()
        sql = 'INSERT INTO ExchangeDB.dbo.ExMsgStatus (ExMsgID,SystemID,Status,StatusDateTime) VALUES '
        sql += "({0},0,{1},GETDATE())".format(msg_id, msg_status)
        logging.debug(sql)
        cursor.execute(sql)
        cursor.commit()


def insert_inbound_header(ord_no):
    sql = "SET NOCOUNT ON; DECLARE @TID TABLE(ID INT); \
    INSERT INTO ExchangeDB.dbo.tblInboundOrder \
    (OrderNo, OwnerID, Status, OrderDate, DateExpected, OrderType, IsApproved) \
    OUTPUT inserted.id INTO @TID VALUES \
    (N'{}',N'HERBALIFE',0,GETDATE(),GETDATE(),N'01',1); \
    SELECT * FROM @TID;".format(ord_no)
    logging.debug(sql)
    with pyodbc.connect(conn_str()) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        header_id = cursor.fetchall()[0][0]
        cursor.commit()
        logging.debug('header id = %d', header_id)
    return header_id


def insert_inbound_items(header_id, rows):
    with pyodbc.connect(conn_str()) as conn:
        cursor = conn.cursor()
        for row in rows:
            _t2 = ''
            try:
                _d1 = datetime.datetime.strptime(
                    str(row['PLExpiryDate']), '%d-%b-%y').date()
                _t2 = "'" + _d1.isoformat() + "'"
            except:
                pass
            sql = 'INSERT INTO ExchangeDB.dbo.tblInboundOrderDetail \
            (HeaderID,SKU,SSCC,UnitCode,DateExpiry,QtyExpected,QtyReceived,LOT) VALUES '
            sql += "({0},'{1}','{2}','{3}',{4},{5},{5},'{6}')".format(header_id,
                                                                      row['PLItemNumber'], row['PLPalletNo'], 'PC', _t2 if _t2 != '' else 'NULL', row['PLQuantity'], row['PLLotNumber'])
            logging.debug(sql)
            cursor.execute(sql)
            cursor.commit()


def do_zip_file(file_name, zip_file_name):
    ''' add file to zip archive '''
    try:
        os.makedirs(os.path.dirname(zip_file_name), exist_ok=True)
        logging.info('ZIP backup %s --> %s',
                     os.path.basename(file_name), zip_file_name)
        with zipfile.ZipFile(zip_file_name, 'a') as zip_file:
            zip_file.write(file_name, os.path.basename(file_name))
    except Exception as ex:
        logging.error(ex)
        raise ex


def process_file_csv(file_name):
    try:
        rows = read_file(file_name)
        ord_no = rows[0]['PLOrderNo']
        logging.info('Order No ' + ord_no)
        header_id = insert_inbound_header(ord_no)
        insert_inbound_items(header_id, rows)
        add_new_ExMsg(header_id)
    except Exception as ex:
        logging.error(ex)
        raise ex


def process_files(path_source, path_backup):
    files = os.listdir(path_source)
    zip_file_name = os.path.join(
        path_backup, 'InOrder {0:%Y-%m}.zip'.format(datetime.date.today()))
    for _t1 in files:
        file_name = os.path.join(path_source, _t1)
        logging.info(file_name)
        if _t1.endswith('.csv'):
            try:
                process_file_csv(file_name)
                do_zip_file(file_name, zip_file_name)
                os.remove(file_name)
            except Exception as ex:
                logging.error(ex)


start_import()
