#! python3
''' read new exmsg and reply it as ommited '''

import logging
import os
import datetime
import zipfile
import pyodbc
from ftplib import FTP


def conn_str():
    ''' connection string '''
    # return 'DRIVER={SQL Server};SERVER=RU-LOB-SQL01;DATABASE=ExchangeDB;Integrated Security=SSPI;'
    return 'DRIVER={SQL Server};SERVER=RU-LOB-SQL01;DATABASE=ExchangeDB;UID=exuser;PWD=good4you'

def init_logger():
    ''' init logger with unicode support '''
    from logging.handlers import RotatingFileHandler

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(
        filename='read_new_exmsg.log', maxBytes=5000000, backupCount=5, encoding='utf-8')
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s\t%(funcName)s() <%(lineno)s> %(message)s')
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

def path_init_2d():
    ''' Initialization of used paths to work with files '''
    try:
        path_source = os.path.join(os.getcwd(), 'Ports\\Barcode2D')
        path_backup = os.path.join(os.getcwd(), 'Backup\\Barcode2D')
        os.makedirs(path_source, exist_ok=True)
        os.makedirs(path_backup, exist_ok=True)
        logging.debug('Path Source: ' + path_source)
        logging.debug('Path Backup: ' + path_backup)
        return path_source, path_backup
    except Exception as ex:
        logging.error(ex)
        raise ex


def send_file_ftp(file_name):
    server = 'eftp.hrbl.com'
    login = 'FTP_Filuet'
    password = 'Herb1237'
    directory = 'Filuet/PROD/2D_serial'

    with FTP(server) as ftp:
        ftp.login(login, password)
        ftp.cwd(directory)
        with open(file_name, 'rb') as file:
            ftp.storbinary('STOR ' + os.path.basename(file_name), file)
            logging.info('File %s copied to ftp', os.path.basename(file_name))

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


def read_new_exmsg():
    ''' get new exmessages '''
    with pyodbc.connect(conn_str()) as conn:
        cursor = conn.cursor()
        cursor.execute('EXEC ExchangeDB.dbo.BT_GetNewExMsg')
        rows = cursor.fetchall()
    return rows


def insert_new_status(msg_id, msg_status):
    ''' insert status for new ex msg '''
    with pyodbc.connect(conn_str()) as conn:
        cursor = conn.cursor()
        sql = 'INSERT INTO ExchangeDB.dbo.ExMsgStatus (ExMsgID,SystemID,Status,StatusDateTime) VALUES '
        sql += "({0},0,{1},GETDATE())".format(msg_id, msg_status)
        logging.debug(sql)
        cursor.execute(sql)
        cursor.commit()
    return


def start_process():
    ''' get new exmsg '''
    # logging.basicConfig(filename='read_new_exmsg.log', level=logging.DEBUG,
    #                     format=' %(asctime)s %(levelname)s \t%(funcName)s() <%(lineno)s> %(message)s')
    init_logger()
    logging.info('START')
    path_source_2d, path_backup_2d = path_init_2d()

    def send_dat_file():
        nonlocal path_source_2d
        nonlocal path_backup_2d

        file_name_today = 'RUSN{0:%Y%m%d230000}.DAT'.format(datetime.date.today())

        files = os.listdir(path_source_2d)
        for _t1 in files:
            if _t1 != file_name_today:
                try:
                    file_name = os.path.join(path_source_2d, _t1)
                    send_file_ftp(file_name)  # send to ftp
                    zip_file_name = os.path.join(
                        path_backup_2d, 'Barcode2D {0:%Y-%m}.zip'.format(datetime.date.today()))
                    do_zip_file(file_name, zip_file_name)  # backup to zip
                    os.remove(file_name)  # delete file
                except Exception as ex:
                    logging.error(ex)

    def process_2d(msg_id):
        ''' process exmsg with 2d code '''
        nonlocal path_source_2d
        msg_status = 2

        sql = 'SELECT tbd.OrderNo,tbd.Barcode FROM  dbo.ExMsg em INNER JOIN dbo.tblBarcode2D tbd ON em.ObjectID = tbd.id WHERE em.ID = {0};'.format(
            msg_id)
        with pyodbc.connect(conn_str()) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()

        for row in rows:
            order_code = row[0]
            bcodes = str(row[1]).split(',')
            for bcode in bcodes:
                file_name = os.path.join(
                    path_source_2d, 'RUSN{0:%Y%m%d230000}.DAT'.format(datetime.date.today()))
                with open(file_name, 'a') as dat_file:
                    dat_file.write('{0}|{1}\n'.format(order_code, bcode))
                logging.info('Barcode2D\t%s\t%s', order_code, bcode)
            msg_status = 1
        return msg_status

    try:
        rows = read_new_exmsg()
        for row in rows:
            # print(row)
            msg_status = 2
            if row[1] == 21:  # msg for 2d code
                msg_status = process_2d(row[0])
            insert_new_status(row[0], msg_status)
        logging.info('Processed %d ExMsg', len(rows))

        # check send time to FTP (should be from 00:00 till 01:00)
        # if datetime.datetime.now().hour < 1:
        send_dat_file()

    except Exception as ex:
        logging.error(ex)
    finally:
        logging.info('EXIT')


start_process()
