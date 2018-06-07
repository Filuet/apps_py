using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;


namespace ExAppFunctions
{
    public class Helper
    {
        public Helper()
        {
        }

        const string connStr = "Server=RU-LOB-WMS01;Initial Catalog=ExchangeDB;Integrated Security=False;User ID=ExUser;Password=good4you";

        public static string InsertInboundOrder(InOrder ord)
        {
            string res = "";
            string sql = "";
            int headID;
            int msgID;

            try
            {
                SqlConnection con = new SqlConnection(connStr);
                con.Open();

                // insert header into tblInboundOrder
                using (SqlCommand cmd = new SqlCommand())
                {
                    sql = sqlHeader01();
                    sql += (ord.OrderNo == null) ? "NULL," : "N'" + ord.OrderNo + "',";
                    sql += (ord.OwnerID == null) ? "NULL," : "N'" + ord.OwnerID + "',";
                    sql += (ord.SupplierID == null) ? "NULL," : "N'" + ord.SupplierID + "',";
                    sql += (ord.Status == null) ? "NULL," : "N'" + ord.Status + "',";
                    sql += (ord.InvoiceNo == null) ? "NULL," : "N'" + ord.InvoiceNo + "',";
                    sql += (ord.WayBillNo == null) ? "NULL," : "N'" + ord.WayBillNo + "',";
                    sql += (ord.OrderDate == null) ? "NULL," : "N'" + ord.OrderDate + "',";
                    sql += (ord.DateExpected == null) ? "NULL," : "N'" + ord.DateExpected + "',";
                    sql += (ord.OrderType == null) ? "NULL," : "N'" + ord.OrderType + "',";
                    sql += (ord.Notes == null) ? "NULL," : "N'" + ord.Notes + "',";
                    sql += (ord.OwnerDeptID == null) ? "NULL," : "N'" + ord.OwnerDeptID + "',";
                    sql += (ord.GTD == null) ? "NULL," : "N'" + ord.GTD + "',";
                    sql += ord.IsApproved.ToString();
                    sql += "); select * from @tID;";

                    cmd.CommandType = CommandType.Text;
                    cmd.Connection = con;
                    cmd.CommandText = sql;
                    if (!int.TryParse(cmd.ExecuteScalar().ToString(), out headID))
                        throw new Exception("Wrong Header ID");
                }

                // insert details into tblInboundOrderDetails
                if (ord.Items.Item != null)
                {
                    foreach (InOrderItemsItem item in ord.Items.Item)
                    {
                        item.SKU = CheckSKU(item.SKU, ord.OrderType);
                        using (SqlCommand cmd = new SqlCommand())
                        {
                            sql = "INSERT INTO [ExchangeDB].[dbo].[tblInboundOrderDetail] ([HeaderID],[SKU],[SSCC],[UnitCode],[DateExpiry],[QtyExpected],[QtyReceived],[LOT]) VALUES (";
                            sql += headID.ToString() + ",";
                            sql += (item.SKU == null) ? "NULL," : "N'" + item.SKU + "',";
                            sql += (item.SSCC == null) ? "NULL," : "N'" + item.SSCC + "',";
                            sql += (item.UnitCode == null) ? "NULL," : "N'" + item.UnitCode + "',";
                            sql += (item.DateExpiry == null) ? "NULL," : "N'" + item.DateExpiry + "',";
                            sql += item.QtyExpected.ToString() + ",";
                            sql += item.QtyReceived.ToString() + ",";
                            sql += (item.LOT == null) ? "NULL)" : "N'" + item.LOT + "')";

                            cmd.CommandType = CommandType.Text;
                            cmd.Connection = con;
                            cmd.CommandText = sql;
                            int r = cmd.ExecuteNonQuery();
                            if (r != 1)
                                res += "Item:" + item.SKU + " was not added!";
                        }
                    }
                }

                // insert new message into ExMsg
                using (SqlCommand cmd = new SqlCommand())
                {
                    sql = "declare @tID table (ID int);INSERT INTO [ExchangeDB].[dbo].[ExMsg] ([TypeID],[ObjectID],[HighPriority]) output inserted.ID into @tID VALUES (8,";
                    sql += headID.ToString() + ",0);  select * from @tID;";

                    cmd.CommandType = CommandType.Text;
                    cmd.Connection = con;
                    cmd.CommandText = sql;
                    if (!int.TryParse(cmd.ExecuteScalar().ToString(), out msgID))
                        throw new Exception("Wrong Message ID");
                }

                // insert status message into ExMsgStatus
                using (SqlCommand cmd = new SqlCommand())
                {
                    sql = "INSERT INTO [ExchangeDB].[dbo].[ExMsgStatus] ([ExMsgID],[SystemID],[Status],[StatusDateTime],[ErrMsg]) VALUES(";
                    sql += msgID.ToString() + ",0,0,GETDATE(),N'" + res + "')";
                    //sql += DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "',N'" + res + "')";

                    cmd.CommandType = CommandType.Text;
                    cmd.Connection = con;
                    cmd.CommandText = sql;
                    int r = cmd.ExecuteNonQuery();
                    if (r != 1)
                        res += "Status for message:" + msgID.ToString() + " was not added!";
                }
            }
            catch (Exception ex)
            {
                res += "ERROR InsertInboundOrder():" + ex.Message;
            }
            return res;
        }

        public static string InsertOutboundOrder(OutOrder ord)
        {
            string res = "";
            string sql = "";
            int headID;
            int msgID;

            if (!string.IsNullOrEmpty(ord.AddrName))
            {
                ord.AddrName = new string(ord.AddrName.Where(c => !char.IsPunctuation(c)).ToArray()).ToUpper().Trim();
            }

            if (!string.IsNullOrEmpty(ord.AddrPhone))
            {
                ord.AddrPhone = new string(ord.AddrPhone.Where(c => char.IsDigit(c)).ToArray());
                if (ord.AddrPhone.Length == 10)
                    ord.AddrPhone = "8" + ord.AddrPhone;
            }

            try
            {
                SqlConnection con = new SqlConnection(connStr);
                con.Open();

                // insert header into tblOutboundOrder
                using (SqlCommand cmd = new SqlCommand())
                {
                    sql = sqlHeader02();
                    sql += (ord.OrderNo == null) ? "NULL," : "N'" + ord.OrderNo + "',";
                    sql += (ord.OrderDate == null) ? "NULL," : "N'" + ord.OrderDate + "',";
                    sql += (ord.OrderType == null) ? "NULL," : "N'" + ord.OrderType + "',";
                    sql += ord.Sku_Type.ToString() + ",";
                    sql += (ord.OwnerID == null) ? "NULL," : "N'" + ord.OwnerID + "',";
                    sql += (ord.DateScheduled == null) ? "NULL," : "N'" + ord.DateScheduled + "',";
                    sql += (ord.ConsigneeID == null) ? "NULL," : "N'" + ord.ConsigneeID + "',";
                    sql += (ord.AddrName == null) ? "NULL," : "N'" + ord.AddrName + "',";
                    sql += (ord.AddrCountry == null) ? "NULL," : "N'" + ord.AddrCountry + "',";
                    sql += (ord.AddrPostIndx == null) ? "NULL," : "N'" + ord.AddrPostIndx + "',";
                    sql += (ord.AddrRegion == null) ? "NULL," : "N'" + ord.AddrRegion + "',";
                    sql += (ord.AddrCity == null) ? "NULL," : "N'" + ord.AddrCity.strTrim(20) + "',";
                    sql += (ord.AddrAddress == null) ? "NULL," : "N'" + ord.AddrAddress + "',";
                    sql += (ord.AddrPhone == null) ? "NULL," : "N'" + ord.AddrPhone + "',";
                    sql += (ord.AddrFax == null) ? "NULL," : "N'" + ord.AddrFax + "',";
                    sql += (ord.AddrEmail == null) ? "NULL," : "N'" + ord.AddrEmail + "',";
                    sql += (ord.AddrOrdCode == null) ? "NULL," : "N'" + ord.AddrOrdCode + "',";
                    sql += (ord.Notes == null) ? "NULL," : "N'" + ord.Notes + "',";
                    sql += (ord.CarrierCode == null) ? "NULL," : "N'" + ord.CarrierCode + "',";
                    sql += ord.IsAddDocs + ",";
                    sql += (ord.Move == null) ? "NULL," : "N'" + ord.Move + "',";
                    sql += (ord.Req == null) ? "NULL," : "N'" + ord.Req + "',";
                    sql += (ord.PickSlip == null) ? "NULL," : "N'" + ord.PickSlip + "',";
                    sql += (ord.PkpTerminalNo == null) ? "NULL," : "N'" + ord.PkpTerminalNo + "',";
                    sql += (ord.PkpTerminalName == null) ? "NULL," : "N'" + ord.PkpTerminalName + "',";
                    sql += (ord.PkpTerminalAddress == null) ? "NULL," : "N'" + ord.PkpTerminalAddress + "',";
                    sql += (ord.DeliveryNo == null) ? "NULL," : "N'" + ord.DeliveryNo + "',";
                    sql += ord.OrderAmount.ToString() + ",";
                    sql += ord.IsApproved.ToString() + ",";
                    sql += (ord.OwnerDeptID == null) ? "NULL," : "N'" + ord.OwnerDeptID + "',";
                    sql += ord.temperatureControl.ToString() + ",";
                    sql += (ord.ClientID == null) ? "NULL," : "N'" + ord.ClientID + "',";
                    sql += (ord.Status == null) ? "NULL," : "N'" + ord.Status + "'";
                    sql += "); select * from @tID;";

                    cmd.CommandType = CommandType.Text;
                    cmd.Connection = con;
                    cmd.CommandText = sql;
                    if (!int.TryParse(cmd.ExecuteScalar().ToString(), out headID))
                        throw new Exception("Wrong Header ID");
                }

                // insert details into tblOutboundOrderDetails
                if (ord.Items.Item != null)
                {
                    foreach (OutOrderItemsItem item in ord.Items.Item)
                    {
                        int UOM_SET = 0;
                        int _qtyUOM = 1;
                        if (ord.OwnerID == "HERBALIFE")
                        {
                            item.SKU = CheckSKU(item.SKU, ord.OrderType);
                            if ((item.UnitCode != null) && (item.UnitCode != "PC"))
                            {
                                try
                                {
                                    using (SqlCommand cmd1 = new SqlCommand())
                                    {
                                        cmd1.CommandType = CommandType.StoredProcedure;
                                        cmd1.Connection = con;
                                        cmd1.CommandText = "[dbo].[ANT_FIL_UOMtoPC]";
                                        //cmd1.Parameters.AddWithValue("@SKU", item.SKU);
                                        cmd1.Parameters.AddWithValue("@SKU", item.SKU);
                                        cmd1.Parameters.AddWithValue("@SKUtype", 2);
                                        cmd1.Parameters.AddWithValue("@UoM", item.UnitCode);
                                        var r1 = cmd1.ExecuteScalar();

                                        if (r1 == null)
                                        {
                                            throw (new Exception("Error: No unit " + item.UnitCode + " for item " + item.SKU));
                                        }
                                        else
                                        {
                                            _qtyUOM = Convert.ToInt32((decimal)r1);

                                            if (_qtyUOM > 0)
                                                UOM_SET = 1;
                                            else
                                                throw (new Exception("Error: No unit " + item.UnitCode + " for item " + item.SKU));
                                        }
                                    }
                                }
                                catch (Exception ex1)
                                {
                                    EventLog.WriteEntry("BT_InsertOutboundOrder", ex1.Message, EventLogEntryType.Error);
                                    res += ex1.Message;
                                    _qtyUOM = 1;
                                    UOM_SET = 0;
                                }
                            }
                        }
                        using (SqlCommand cmd = new SqlCommand())
                        {
                            sql = "INSERT INTO [ExchangeDB].[dbo].[tblOutboundOrderDetail] ([HeaderID],[UnitCode],[Qty],[SKU],[LOT],[DateExp],[DateProd],[RevNo],[UOM_SET]) VALUES (";
                            sql += headID.ToString() + ",";
                            if (UOM_SET == 1)
                                sql += "N'PC',";
                            else
                                sql += (item.UnitCode == null) ? "NULL," : "N'" + item.UnitCode + "',";
                            sql += (item.Qty * _qtyUOM).ToString() + ",";
                            sql += (item.SKU == null) ? "NULL," : "N'" + item.SKU + "',";
                            sql += (item.LOT == null) ? "NULL," : "N'" + item.LOT + "',";
                            sql += (item.DateExp == null) ? "NULL," : "N'" + item.DateExp + "',";
                            sql += (item.DateProd == null) ? "NULL," : "N'" + item.DateProd + "',";
                            sql += (item.RevNo == null) ? "NULL," : "N'" + item.RevNo + "',";
                            sql += UOM_SET.ToString() + ")";

                            cmd.CommandType = CommandType.Text;
                            cmd.Connection = con;
                            cmd.CommandText = sql;
                            int r = cmd.ExecuteNonQuery();
                            if (r != 1)
                                res += "Item:" + item.SKU + " was not added!";
                        }
                    }
                }

                // insert new message into ExMsg
                using (SqlCommand cmd = new SqlCommand())
                {
                    sql = "declare @tID table (ID int);INSERT INTO [ExchangeDB].[dbo].[ExMsg] ([TypeID],[ObjectID],[HighPriority]) output inserted.ID into @tID VALUES (9,";
                    sql += headID.ToString() + ",0);  select * from @tID;";

                    cmd.CommandType = CommandType.Text;
                    cmd.Connection = con;
                    cmd.CommandText = sql;
                    if (!int.TryParse(cmd.ExecuteScalar().ToString(), out msgID))
                        throw new Exception("Wrong Message ID");
                }

                // insert status message into ExMsgStatus
                using (SqlCommand cmd = new SqlCommand())
                {
                    sql = "INSERT INTO [ExchangeDB].[dbo].[ExMsgStatus] ([ExMsgID],[SystemID],[Status],[StatusDateTime],[ErrMsg]) VALUES(";
                    sql += msgID.ToString() + ",0,0,GETDATE(),N'" + res + "')";
                    //sql += DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "';

                    cmd.CommandType = CommandType.Text;
                    cmd.Connection = con;
                    cmd.CommandText = sql;
                    int r = cmd.ExecuteNonQuery();
                    if (r != 1)
                        res += "Status for message:" + msgID.ToString() + " was not added!";
                }
            }
            catch (Exception ex)
            {
                res += "ERROR InsertOutboundOrder() \r\nOrderNo " + ord.OrderNo + "\r\n" + ex.Message;
                EventLog.WriteEntry("BT_InsertOutboundOrder", res, EventLogEntryType.Error);
            }
            return res;
        }

        public static ExMsg InsertExMessageStatus(ExMsg msg)
        {
            string res = "";
            string sql = "";

            try
            {
                SqlConnection con = new SqlConnection(connStr);
                con.Open();

                using (SqlCommand cmd = new SqlCommand())
                {
                    sql = "INSERT INTO [ExchangeDB].[dbo].[ExMsgStatus] ([ExMsgID],[SystemID],[Status],[StatusDateTime],[ErrMsg]) VALUES (";
                    sql += msg.MsgID.ToString() + ",";
                    sql += msg.SystemID.ToString() + ",";
                    sql += msg.Status.ToString() + ",GETDATE(),";
                    //msg.StatusDateTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                    //sql += "N'" + msg.StatusDateTime + "',";
                    sql += (msg.ErrMsg == null) ? "NULL," : "N'" + msg.ErrMsg + "')";

                    cmd.CommandType = CommandType.Text;
                    cmd.Connection = con;
                    cmd.CommandText = sql;
                    if (cmd.ExecuteNonQuery() != 1)
                        throw new Exception("Status for message:" + msg.MsgID.ToString() + " was not added!");
                }
            }
            catch (Exception ex)
            {
                res += "ERROR InsertExMessageStatus():" + ex.Message;
                msg.Status = 3;
            }
            msg.ErrMsg += res;
            return msg;
        }

        public static OrderShipped GetOrderShip(int ExMsgID)
        {
            OrderShipped ordShip = new OrderShipped();
            ordShip.Boxes = new OrderShippedBoxes();
            ordShip.Boxes.Box = new List<OrderShippedBoxesBox>();
            string sql = "";
            int orderShipID = 0;

            try
            {
                SqlConnection con = new SqlConnection(connStr);
                con.Open();

                // ObjectID ExMsg
                sql = "SELECT [ObjectID]  FROM [ExchangeDB].[dbo].[ExMsg] WHERE [ID]=" + ExMsgID;
                using (SqlCommand cmd1 = new SqlCommand())
                {
                    cmd1.CommandType = CommandType.Text;
                    cmd1.Connection = con;
                    cmd1.CommandText = sql;
                    if (!int.TryParse(cmd1.ExecuteScalar().ToString(), out orderShipID))
                        throw new Exception("Wrong ObjectID");
                }

                // tblOrderShip
                sql = "SELECT [OrderDate],[OrderNo],[OwnerID],[ShipDate],[OrderType],[CarrierCode],[TotalBoxes] FROM [ExchangeDB].[dbo].[tblOrderShip] WHERE [id] =" + orderShipID;
                using (SqlCommand cmd2 = new SqlCommand())
                {
                    cmd2.CommandType = CommandType.Text;
                    cmd2.Connection = con;
                    cmd2.CommandText = sql;
                    SqlDataReader dr2 = cmd2.ExecuteReader();
                    if (!dr2.HasRows)
                        throw new Exception("No OrderShip");
                    dr2.Read();
                    ordShip.OrderDate = dr2.IsDBNull(0) ? "" : string.Format("{0:yyyy-MM-dd HH:mm:ss}", dr2.GetDateTime(0));
                    ordShip.OrderNo = dr2.GetString(1);
                    ordShip.OwnerID = dr2.GetString(2);
                    ordShip.ShipDate = dr2.IsDBNull(3) ? "" : string.Format("{0:yyyy-MM-dd HH:mm:ss}", dr2.GetDateTime(3));
                    ordShip.OrderType = dr2.GetString(4);
                    ordShip.CarrierCode = dr2.IsDBNull(5) ? "" : dr2.GetString(5);
                    ordShip.TotalBoxes = dr2.IsDBNull(6) ? 1 : dr2.GetInt32(6);
                    dr2.Close();
                    dr2.Dispose();
                }

                // tblOrderShipBox

                sql = "SELECT [id],[BoxNo],[SSCC],[BoxWeight] FROM [ExchangeDB].[dbo].[tblOrderShipBox] WHERE [HeaderID]=" + orderShipID;
                using (SqlCommand cmd3 = new SqlCommand())
                {
                    cmd3.CommandType = CommandType.Text;
                    cmd3.Connection = con;
                    cmd3.CommandText = sql;
                    SqlDataReader dr3 = cmd3.ExecuteReader();
                    if (!dr3.HasRows)
                        throw new Exception("No OrderShipBox");

                    while (dr3.Read())
                    {
                        int boxId = dr3.GetInt32(0);
                        OrderShippedBoxesBox box = new OrderShippedBoxesBox();
                        box.BoxNo = dr3.GetInt32(1);
                        box.SSCC = dr3.GetString(2);
                        box.BoxWeight = dr3.IsDBNull(3) ? 0 : dr3.GetDecimal(3);
                        box.DeliveryNo = ""; // dr.IsDBNull(4) ? "" : dr.GetString(4);
                        box.Items = new OrderShippedBoxesBoxItems();
                        box.Items.Item = new List<OrderShippedBoxesBoxItemsItem>();

                        #region tblOrderShipDetail

                        sql = "SELECT [SKU],[SellSKU],[OrderQty],[ShipQty],[UnitCode],[LOT],[DateExpire],[RevNo] FROM [ExchangeDB].[dbo].[tblOrderShipDetail] WHERE[BoxHeaderID]=" + boxId;
                        using (SqlConnection con1 = new SqlConnection(connStr))
                        {
                            con1.Open();
                            using (SqlCommand cmd4 = new SqlCommand())
                            {
                                cmd4.CommandType = CommandType.Text;
                                cmd4.Connection = con1;
                                cmd4.CommandText = sql;
                                SqlDataReader dr4 = cmd4.ExecuteReader();
                                if (!dr4.HasRows)
                                    throw new Exception("No OrderShipBoxDetail");

                                while (dr4.Read())
                                {
                                    OrderShippedBoxesBoxItemsItem item = new OrderShippedBoxesBoxItemsItem();
                                    item.SKU = dr4.GetString(0);
                                    item.SellSKU = dr4.GetString(1);
                                    item.OrderQty = dr4.GetDecimal(2);
                                    item.ShipQty = dr4.GetDecimal(3);
                                    item.UnitCode = dr4.GetString(4);
                                    item.LOT = dr4.IsDBNull(5) ? "" : dr4.GetString(5);
                                    item.DateExpire = dr4.IsDBNull(6) ? "" : string.Format("{0:yyyy-MM-dd HH:mm:ss}", dr4.GetDateTime(6));
                                    item.RevNo = dr4.IsDBNull(7) ? "" : dr4.GetString(7);
                                    box.Items.Item.Add(item);
                                }
                                dr4.Close();
                                dr4.Dispose();
                            }
                        }

                        #endregion

                        ordShip.Boxes.Box.Add(box);
                    }
                    dr3.Close();
                    dr3.Dispose();
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.EventLog.WriteEntry("GetOrderShip", ex.Message);
            }
            return ordShip;
        }

        public static Record GetBarcode2D(int ObjectID)
        {
            Record rec = new Record();
            string sql = "";

            try
            {
                SqlConnection con = new SqlConnection(connStr);
                con.Open();

                // tblOrderShip
                sql = "SELECT [OrderNo],[OwnerID],[Barcode] FROM [dbo].[tblBarcode2D] WHERE [id] =" + ObjectID;

                using (SqlCommand cmd = new SqlCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Connection = con;
                    cmd.CommandText = sql;
                    SqlDataReader dr = cmd.ExecuteReader();
                    if (!dr.HasRows)
                        throw new Exception("No Barcode2D");
                    dr.Read();
                    rec.OrderNo = dr.GetString(0);
                    rec.OwnerID = dr.GetString(1);
                    rec.Barcode = dr.GetString(2);
                    dr.Close();
                    dr.Dispose();
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.EventLog.WriteEntry("GetBarcode2D", ex.Message);
            }
            return rec;
        }

        public static string InsertStock(StockSL st)
        {
            int rec = 0;
            try
            {
                if (st.Items.Count() > 0)
                    DeleteStockData(st.Date);

                foreach (StockSLItemsItem i in st.Items)
                {
                    string sql = @"INSERT INTO [dbo].[SherlandWH]
                                           ([RecDate]
                                           ,[SKU]
                                           ,[SSCC]
                                           ,[QTY]
                                           ,[ExpiryDate]
                                           ,[Note])
                                     VALUES (";
                    sql += string.Format("N'{0:yyyy-MM-dd}',", st.Date);
                    sql += String.Format("N'{0}',", i.SKU);
                    sql += (string.IsNullOrEmpty(i.SSCC)) ? "NULL," : (String.Format("N'{0}',", i.SSCC));
                    sql += String.Format("{0},", i.Qty);
                    sql += (i.DateExpiry == null) ? "NULL," : string.Format("N'{0:yyyy-MM-dd}',", i.DateExpiry);
                    sql += "NULL)";

                    using (SqlConnection conn = new SqlConnection(connStr))
                    {
                        conn.Open();
                        using (SqlCommand cmd = new SqlCommand())
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.Connection = conn;
                            cmd.CommandText = sql;
                            rec += cmd.ExecuteNonQuery();
                        }
                        conn.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                EventLog.WriteEntry("Helper.InsertStock", ex.Message, EventLogEntryType.Error);
            }
            return rec.ToString();
        }

        private static void DeleteStockData(DateTime d)
        {
            string sql = "DELETE FROM [dbo].[SherlandWH] WHERE [RecDate] = ";
            sql += string.Format("N'{0:yyyy-MM-dd}'", d);
            try
            {
                using (SqlConnection conn = new SqlConnection(connStr))
                {
                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Connection = conn;
                        cmd.CommandText = sql;
                        cmd.ExecuteNonQuery();
                    }
                    conn.Close();
                }
            }
            catch (Exception ex)
            {
                EventLog.WriteEntry("Helper.DeleteStockData", ex.Message, EventLogEntryType.Error);
            }
        }

        private static string sqlHeader01()
        {
            string s = "declare @tID table (ID int);INSERT INTO [ExchangeDB].[dbo].[tblInboundOrder] ([OrderNo],[OwnerID],[SupplierID],[Status],[InvoiceNo],[WayBillNo],[OrderDate],[DateExpected],[OrderType],[Notes],[OwnerDeptID],[GTD],[IsApproved]) output inserted.id into @tID VALUES(";
            return s;
        }

        private static string sqlHeader02()
        {
            string s = "declare @tID table (ID int);INSERT INTO [ExchangeDB].[dbo].[tblOutboundOrder] ([OrderNo],[OrderDate],[OrderType],[Sku_Type],[OwnerID],[DateScheduled],[ConsigneeID],[AddrName],[AddrCountry],[AddrPostIndx],[AddrRegion],[AddrCity],[AddrAddress],[AddrPhone],[AddrFax],[AddrEmail],[AddrOrdCode],[Notes],[CarrierCode],[IsAddDocs],[Move],[Req],[PickSlip],[PkpTerminalNo],[PkpTerminalName],[PkpTerminalAddress],[DeliveryNo],[OrderAmount],[isApproved],[OwnerDeptID],[temperatureControl],[ClientID],[Status])  output inserted.id into @tID VALUES(";
            return s;
        }

        private static string CheckSKU(string _sku, string type)
        {
            string[] ShipTo = new string[] { "01", "02", "04", "05", "14", "19", "21", "22" };

            string r = _sku;
            if (ShipTo.Contains(type))
            {
                if (_sku == "0141") r = "0951";
                if (_sku == "0143") r = "0953";
                if (_sku == "2653") r = "0948";
            }

            switch (_sku)
            {
                case "7051NL":
                    r = "7051RU";
                    break;
                case "7640EU":
                    r = "7640RU";
                    break;
                case "8601E":
                    r = "8601RS";
                    break;
                case "8602E":
                    r = "8602RS";
                    break;
                case "8501RS":
                    r = "8501RU";
                    break;
            }
            return r;
        }
    }

    public static class MyExtentions
    {
        public static string strTrim(this string text, int len)
        {
            string r = "";
            try
            {
                if (text.Length > len)
                {
                    r = text.Substring(0, len);
                }
                else
                {
                    r = text;
                }
            }
            catch { }
            return r;
        }
    }

}
