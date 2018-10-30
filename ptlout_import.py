#! python3
''' import PTLOUT file from FTP location and add to ExchangeDB '''

import os
from shutil import copyfile
import datetime
import zipfile
import logging
import untangle
import pyodbc
from ftplib import FTP
from logging.handlers import RotatingFileHandler


def conn_str():
    ''' connection string '''
    return 'DRIVER={SQL Server};SERVER=RU-LOB-SQL01;DATABASE=ExchangeDB;Integrated Security=SSPI;'


def init_logger():
    ''' init logger with unicode support '''
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    # handler = logging.FileHandler('ptlout_import.log', 'a', 'utf-8')
    handler = RotatingFileHandler(
        filename='ptlout_import.log', maxBytes=5000000, backupCount=5, encoding='utf-8')
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
        move_files_ftp(path_source)
        # copy_files_biztalk(path_source)
        process_file(path_source, path_backup)
    except Exception as ex:
        logging.error(ex)
    finally:
        logging.info('EXIT')


def copy_err_file(path_source, file_name):
    ''' copy files with import error '''
    try:
        path_dest = os.path.join(path_source, 'Error')
        os.makedirs(path_dest, exist_ok=True)
        file_s = os.path.join(path_source, file_name)
        file_d = os.path.join(path_dest, file_name)
        copyfile(file_s, file_d)
        os.remove(file_s)
        logging.info('Error file moved {0}'.format(file_s))
    except IOError as ex:
        logging.error(
            "Unable to move file: {0} --> {1}\n{2}".format(file_s, file_d, ex))


def copy_files_biztalk(path_source):
    ''' copy ptlout files to biztalk for old system '''
    path_dest = r'\\ru-lob-biz01\PTLOUT New'
    files = os.listdir(path_source)
    for _t1 in files:
        if _t1.endswith('.xml'):
            try:
                file_s = os.path.join(path_source, _t1)
                file_d = os.path.join(path_dest, _t1)
                copyfile(file_s, file_d)
                logging.info('File copied for BizTalk {0}'.format(file_s))
            except IOError as ex:
                logging.error(
                    "Unable to copy file: {0} --> {1}\n{2}".format(file_s, file_d, ex))


def path_init():
    ''' Initialization of used paths to work with files '''
    try:
        path_source = os.path.join(os.getcwd(), 'Ports\\PTLOUT')
        path_backup = os.path.join(os.getcwd(), 'Backup\\PTLOUT')
        os.makedirs(path_source, exist_ok=True)
        os.makedirs(path_backup, exist_ok=True)
        logging.debug('Path Source: ' + path_source)
        logging.debug('Path Backup: ' + path_backup)
        return path_source, path_backup
    except Exception as ex:
        logging.error(ex)
        raise ex


def move_files_ftp(path_d):
    ''' copy files from ftp server and delete them in ftp '''
    server = 'eftp.hrbl.com'
    login = 'FTP_Filuet'
    password = 'Herb1237'
    directory = '/Filuet'
    filematch = '*.xml'

    os.chdir(path_d)

    try:
        with FTP(server) as ftp:
            ftp.login(login, password)
            ftp.cwd(directory)
            # print('File List:')
            # files = ftp.dir()
            for ftp_file_name in ftp.nlst(filematch):
                msg = ftp_file_name
                try:
                    with open(ftp_file_name, 'wb') as ftp_file:
                        ftp.retrbinary('RETR ' + ftp_file_name, ftp_file.write)
                        msg += '\tDownload OK'
                        ftp.delete(ftp_file_name)
                        msg += '\tDeleted OK'
                except:
                    msg += '\tERROR'
                finally:
                    logging.info(msg)
    except Exception as ex:
        logging.error(ex)
        raise ex


def get_xml_file(file_path):
    def check_sku(sku, _type):
        ship_to = ['01', '02', '04', '05', '14', '16', '19', '21', '22']
        res = sku
        if _type in ship_to:
            if sku == '__0141':
                res = '0951'
            if sku == '__0143':
                res = '0953'
            if sku == '2653':
                res = '0948'

        if sku == '7051NL':
            res = '7051RU'
        elif sku == '7640EU':
            res = '7640RU'
        elif sku == '8601E':
            res = '8601RS'
        elif sku == '8602E':
            res = '8602RS'
        elif sku == '8501RS':
            res = '8501RU'
        elif sku == '565U':
            res = '110Q'
        elif sku == '566U':
            res = '111Q'
        elif sku == '567U':
            res = '112Q'
        elif sku == '568U':
            res = '114Q'
        elif sku == '569U':
            res = '115Q'
        elif sku == '570U':
            res = '116Q'
        elif sku == '571U':
            res = '131Q'
        elif sku == '572U':
            res = '132Q'
        elif sku == '573U':
            res = '134Q'
        elif sku == '574U':
            res = '135Q'
        elif sku == '575U':
            res = '136Q'
        elif sku == '576U':
            res = '147Q'
        elif sku == '577U':
            res = '148Q'
        elif sku == '578U':
            res = '141Q'
        elif sku == '579U':
            res = '142Q'
        elif sku == '580U':
            res = '143Q'
        elif sku == '581U':
            res = '144Q'
        elif sku == '582U':
            res = '154Q'
        elif sku == '583U':
            res = '155Q'
        elif sku == '584U':
            res = '156Q'
        elif sku == '585U':
            res = '166Q'
        elif sku == '586U':
            res = '167Q'
        elif sku == '587U':
            res = '168Q'
        elif sku == '588U':
            res = '169Q'
        elif sku == '589U':
            res = '170Q'
        elif sku == '590U':
            res = '171Q'
        elif sku == '591U':
            res = '172Q'
        elif sku == '592U':
            res = 'B229'
        elif sku == '593U':
            res = 'B230'
        elif sku == '594U':
            res = 'B231'
        elif sku == '595U':
            res = 'B241'
        elif sku == '596U':
            res = 'B242'
        elif sku == '597U':
            res = 'B243'
        elif sku == '598U':
            res = 'B244'
        elif sku == '599U':
            res = 'B245'
        elif sku == '600U':
            res = 'B246'
        elif sku == '601U':
            res = 'B247'
        elif sku == '602U':
            res = 'B348'
        elif sku == '603U':
            res = 'B349'
        elif sku == '604U':
            res = 'B350'
        elif sku == '605U':
            res = 'B422'
        elif sku == '606U':
            res = 'B436'
        elif sku == '607U':
            res = 'B437'
        elif sku == '608U':
            res = 'B438'
        elif sku == '609U':
            res = 'B475'
        elif sku == '610U':
            res = 'B476'
        elif sku == '611U':
            res = 'B477'
        elif sku == '612U':
            res = 'B478'
        elif sku == '613U':
            res = 'B479'
        elif sku == '614U':
            res = 'B480'
        elif sku == '615U':
            res = 'B481'
        elif sku == '616U':
            res = 'B482'
        elif sku == '617U':
            res = 'B491'
        elif sku == '618U':
            res = 'B492'
        elif sku == '619U':
            res = 'B493'
        elif sku == '620U':
            res = 'B494'
        elif sku == '621U':
            res = 'B495'
        elif sku == '622U':
            res = 'B496'
        elif sku == '623U':
            res = 'B497'
        elif sku == '624U':
            res = 'Y640'
        elif sku == '625U':
            res = 'Y641'

        return res

    def item_set_check(item):
        if item['UOM'] != 'PC':
            sql = '''
                EXEC ExchangeDB.dbo.ANT_FIL_UOMtoPC
                @SKU = N'{}',
                @SKUtype = 2,
                @UoM = N'SET';
                '''.format(item['SKU'])
            try:
                with pyodbc.connect(conn_str()) as conn:
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    qty_uom = int(cursor.fetchall()[0][0])
            except:
                qty_uom = 1
            item['QTY'] = str(int(item['QTY']) * qty_uom)
            item['UOM'] = 'PC'
            item['UOM_SET'] = '1' if qty_uom > 1 else '0'

        return item

    def order_type(route, pkp, index='000000'):
        res = '02'
        if route == 'RUS':
            res = '01'
        elif route == 'CSE':
            res = '19'
        elif route == 'SA1':
            res = '19'
        elif route == 'SN1':
            res = '19'
        elif route == 'EST' or route == 'PPF':
            res = '21'
        elif route == 'HRM':
            res = '14'
        elif route == 'HDF':
            res = '20'
        elif route == 'QIW':
            res = '16'
        elif route == 'PKP':
            res = '05' if pkp[3:4] == '2' else '04'

        # if (len(index) > 2) and (res == '02'):
        #     if index[:3] == '300':
        #         res = '19'
        #     elif index[:3] == '301':
        #         res = '19'
        #     elif index[:3] == '172':
        #         res = '19'

        return res

    obj = untangle.parse(file_path)

    header = dict()
    header['ORDERID'] = obj.ORDERS.ORDER.ORDERID.cdata
    header['ROUTE'] = obj.ORDERS.ORDER.ROUTE.cdata
    header['DSNAME'] = str(obj.ORDERS.ORDER.DSNAME.cdata).upper()
    header['ADD_DOCS'] = 1 if obj.ORDERS.ORDER.INVOICE_TO_DS.cdata == '3' else 0
    phone = ''.join(
        l for l in obj.ORDERS.ORDER.SHIPTO_MOBILE_NUMBER.cdata if l.isdigit())
    header['PHONE'] = ('8' if len(phone) == 10 else '') + phone
    adr = obj.ORDERS.ORDER.SHIPTO.STREETADDRESS1.cdata + \
        ' ' + obj.ORDERS.ORDER.SHIPTO.STREETADDRESS2.cdata
    header['ADDRESS'] = adr.strip().upper()
    header['INDEX'] = obj.ORDERS.ORDER.SHIPTO.POSTALCODE.cdata
    header['CITY'] = obj.ORDERS.ORDER.SHIPTO.CITY.cdata.strip().upper()[:30]
    header['STATE'] = obj.ORDERS.ORDER.SHIPTO.STATE.cdata.strip().upper()
    header['PKP'] = obj.ORDERS.ORDER.SHIPTO.PKP_LOCATIONID.cdata
    header['TYPE'] = order_type(
        header['ROUTE'], header['PKP'], header['INDEX'])
    logging.debug(header)

    items = list()
    sku1636 = 0
    skuN436 = 0
    for line in obj.ORDERS.ORDER.LINES.LINE:
        item = dict()
        item['SKU'] = check_sku(line.SKU.cdata, header['TYPE'])
        item['QTY'] = line.QTYORIGINAL.cdata
        item['UOM'] = 'PC' if line.INPUTUOM.cdata in (
            'EA', 'PC', 'S01', '') else 'SET'
        item['UOM_SET'] = '0'

        if item['SKU'] == '1636':
            sku1636 += int(item['QTY'])
        if item['SKU'] == 'N436':
            skuN436 += int(item['QTY'])

        items.append(item_set_check(item))

    if sku1636 > 0:
        if sku1636 != skuN436 * 3:
            for item in items:
                if item['SKU'] == 'N436':
                    items.remove(item)
            item = dict()
            item['SKU'] = 'N436'
            item['QTY'] = str(sku1636 * 3)
            item['UOM'] = 'PC'
            item['UOM_SET'] = '0'
            items.append(item)

    logging.debug(items)
    return header, items


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


def insert_new_exmsg(header_id):
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
        VALUES (9, {}, 0);
        SELECT * FROM @TID;
        '''.format(header_id)
        logging.debug(sql)
        with pyodbc.connect(conn_str()) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            msg_id = cursor.fetchall()[0][0]
            cursor.commit()
        if msg_id > 0:
            insert_new_exmsg_status(msg_id, 0)
        else:
            raise 'Error get ExMsgId'
    except Exception as ex:
        logging.error('{0}'.format(ex))
        raise ex


def insert_new_exmsg_status(msg_id, msg_status):
    ''' insert status for new ex msg '''
    with pyodbc.connect(conn_str()) as conn:
        cursor = conn.cursor()
        sql = 'INSERT INTO ExchangeDB.dbo.ExMsgStatus (ExMsgID,SystemID,Status,StatusDateTime) VALUES '
        sql += "({0},0,{1},GETDATE())".format(msg_id, msg_status)
        logging.debug(sql)
        cursor.execute(sql)
        cursor.commit()


def insert_outbound_header(header):
    sql = '''
    SET NOCOUNT ON;
    DECLARE @TID TABLE(ID INT);
    INSERT INTO ExchangeDB.dbo.tblOutboundOrder
    (
        OrderNo,
        OrderDate,
        OrderType,
        Sku_Type,
        OwnerID,
        DateScheduled,
        ConsigneeID,
        AddrName,
        AddrPostIndx,
        AddrRegion,
        AddrCity,
        AddrAddress,
        AddrPhone,
        CarrierCode,
        IsAddDocs,
        PkpTerminalNo,
        OrderAmount,
        isApproved,
        OwnerDeptID,
        temperatureControl,
        ClientID
    )
    OUTPUT
        inserted.id
        INTO @TID
    VALUES
    (
        '{ORDERID}',
        GETDATE(),
        '{TYPE}', -- OrderType - varchar
        1, -- Sku_Type - int
        'HERBALIFE', -- OwnerID - varchar
            GETDATE(), -- DateScheduled - datetime
        'PERSON', -- ConsigneeID - varchar
        '{DSNAME}', -- AddrName - varchar
        '{INDEX}', -- AddrPostIndx - varchar
        '{STATE}', -- AddrRegion - varchar
        '{CITY}', -- AddrCity - varchar
        '{ADDRESS}', -- AddrAddress - varchar
        '{PHONE}', -- AddrPhone - varchar
        '{ROUTE}', -- CarrierCode - varchar
        {ADD_DOCS}, -- IsAddDocs - int
        '{PKP}', -- PkpTerminalNo - varchar
        0, -- OrderAmount - numeric
        1, -- isApproved - int
        'LOG', -- OwnerDeptID - varchar
        0, -- temperatureControl - int
        'HERBALIFE' -- ClientID - varchar
        );
    SELECT * FROM @TID;
    '''.format(**header)
    logging.debug(sql)
    with pyodbc.connect(conn_str()) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        header_id = cursor.fetchall()[0][0]
        cursor.commit()
        logging.debug('header id = %d', header_id)
    return header_id


def insert_outbound_items(header_id, items):
    with pyodbc.connect(conn_str()) as conn:
        cursor = conn.cursor()
        for item in items:
            sql = '''INSERT INTO ExchangeDB.dbo.tblOutboundOrderDetail (HeaderID,UnitCode,Qty,SKU,UOM_SET) VALUES '''
            sql += "({0},'{1}',{2},'{3}',{4})".format(header_id,
                                                      item['UOM'], item['QTY'], item['SKU'], item['UOM_SET'])
            logging.debug(sql)
            cursor.execute(sql)
            cursor.commit()


def process_file(path_source, path_backup):
    zip_file_name = os.path.join(
        path_backup, 'PTLOUT {0:%Y-%m}.zip'.format(datetime.date.today()))
    files = os.listdir(path_source)
    for _t1 in files:
        if _t1.endswith('.xml'):
            try:
                file_name = os.path.join(path_source, _t1)
                logging.info('Process file: ' + file_name)
                header, items = get_xml_file(file_name)
                logging.info('Order No: ' + header['ORDERID'])
                header_id = insert_outbound_header(header)
                insert_outbound_items(header_id, items)
                insert_new_exmsg(header_id)
                do_zip_file(file_name, zip_file_name)
                os.remove(file_name)
            except Exception as ex:
                logging.error(ex)
                copy_err_file(path_source, _t1)


start_import()
