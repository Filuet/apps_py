using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Runtime.InteropServices;
using Microsoft.BizTalk.Component.Interop;
using System.Xml;
using System.IO;
using Microsoft.BizTalk.Message.Interop;
using System.Collections;
using System.Diagnostics;

namespace ExAppPipeline
{
    [ComponentCategory(CategoryTypes.CATID_PipelineComponent)]
    [ComponentCategory(CategoryTypes.CATID_Any)]
    [Guid("460ADC48-B676-4CD3-8C12-D9EE8C4C7B9F")]

    public class HRBLBulkDisassembler : IComponentUI, IBaseComponent, IDisassemblerComponent, IPersistPropertyBag
    {
        private XmlDocument xdoc;
        Queue _msgs = new Queue();

        static HRBLBulkDisassembler()
        {
        }

        #region IBaseComponent Members
        public string Description
        {
            get
            {
                return "Bulk Order TXT file into XML";
            }
        }

        public string Name
        {
            get
            {
                return "HRBLBulkDisassembler";
            }
        }

        public string Version
        {
            get
            {
                return "1.0.0.1";
            }
        }
        # endregion

        #region IComponentUI Members
        public IntPtr Icon
        {
            get
            {
                return new System.IntPtr();
            }
        }

        public System.Collections.IEnumerator Validate(object projectSystem)
        {
            return null;
        }
        #endregion

        #region IPersistPropertyBag Members
        private string _filecopysuccess = string.Empty;
        public string FileCopySuccess
        {
            get { return _filecopysuccess; }
            set { _filecopysuccess = value; }
        }
        private string _filecopyfail = string.Empty;
        public string FileCopyFail
        {
            get { return _filecopyfail; }
            set { _filecopyfail = value; }
        }

        public void GetClassID(out Guid classID)
        {
            classID = new Guid("460ADC48-B676-4CD3-8C12-D9EE8C4C7B9F");
        }

        public void InitNew()
        {
            // Initialization not implemented
        }

        public void Load(IPropertyBag pb, int errorLog)
        {
            object val = ReadPropertyBag(pb, "FileCopySuccess");
            if (val != null) _filecopysuccess = (string)val;

            val = ReadPropertyBag(pb, "FileCopyFail");
            if (val != null) _filecopyfail = (string)val;
        }

        private object ReadPropertyBag(IPropertyBag pb, string propName)
        {
            object val = null;
            try
            {
                pb.Read(propName, out val, 0);
            }
            catch (System.ArgumentException)
            {
                return val;
            }
            catch (System.Exception e)
            {
                throw new System.ApplicationException(e.Message);
            }
            return val;
        }

        public void Save(IPropertyBag propertyBag, bool clearDirty, bool saveAllProperties)
        {
            object val = (object)_filecopysuccess;
            propertyBag.Write("FileCopySuccess", ref val);

            val = (object)_filecopyfail;
            propertyBag.Write("FileCopyFail", ref val);
        }
        #endregion

        #region IDisassemblerComponent Members

        public void Disassemble(IPipelineContext pContext, IBaseMessage pInMsg)
        {
            string fileName = Path.GetFileName(pInMsg.Context.Read("ReceivedFileName", "http://schemas.microsoft.com/BizTalk/2003/file-properties").ToString());


            Stream stream = pInMsg.BodyPart.Data;
            StreamReader sr = new StreamReader(stream);

            System.Diagnostics.Debug.WriteLine(FileCopySuccess);

            xdoc = new XmlDocument();
            XmlDeclaration decl;
            decl = xdoc.CreateXmlDeclaration("1.0", "utf-8", null);
            XmlElement root = CreateElement("HRBL_BulkOrder");
            XmlElement Items = CreateElement("Items");

            string tCons;
            string s;
            string st;
            DateTime d;
            bool headerRead = false;
            string _sku;
            int _qtyPick;
            string _unit;
            string _rev;
            string _expDate;
            //Ola edited 2015-11-10
            string _lot;

            do
            {
                s = sr.ReadLine();
                if (s.Contains("*****HL Pick Slip*****"))
                {
                    if (!headerRead)
                    {
                        #region Header

                        XmlElement OwnerID = CreateElement("OwnerID");
                        OwnerID.InnerText = "HERBALIFE";
                        root.AppendChild(OwnerID);

                        XmlElement OrderType = CreateElement("OrderType");
                        OrderType.InnerText = "12";
                        root.AppendChild(OrderType);

                        XmlElement Sku_Type = CreateElement("Sku_Type");
                        Sku_Type.InnerText = "0";
                        root.AppendChild(Sku_Type);

                        XmlElement IsAddDocs = CreateElement("IsAddDocs");
                        IsAddDocs.InnerText = "1";
                        root.AppendChild(IsAddDocs);

                        XmlElement isApproved = CreateElement("isApproved");
                        isApproved.InnerText = "1";
                        root.AppendChild(isApproved);

                        XmlElement temperatureControl = CreateElement("temperatureControl");
                        temperatureControl.InnerText = "1";
                        root.AppendChild(temperatureControl);

                        s = sr.ReadLine();
                        s = sr.ReadLine();
                        s = sr.ReadLine();
                        s = sr.ReadLine();
                        XmlElement PickSlip = CreateElement("PickSlip");
                        st = s.Substring(22, 10).Trim();
                        PickSlip.InnerText = st;
                        root.AppendChild(PickSlip);

                        XmlElement Move = CreateElement("Move");
                        st = s.Substring(87).Trim();
                        Move.InnerText = st;
                        root.AppendChild(Move);

                        s = sr.ReadLine();
                        XmlElement OrderNo = CreateElement("OrderNo");
                        st = s.Substring(22).Trim();
                        OrderNo.InnerText = st;
                        root.AppendChild(OrderNo);

                        s = sr.ReadLine();
                        s = sr.ReadLine();
                        s = sr.ReadLine();
                        s = sr.ReadLine();
                        XmlElement Req = CreateElement("Req");
                        Req.InnerText = s.Substring(22, 10).Trim();
                        root.AppendChild(Req);

                        tCons = s.Substring(71, 46).Trim();

                        s = sr.ReadLine();
                        XmlElement OrderDate = CreateElement("OrderDate");
                        st = s.Substring(22, 11).Trim();
                        if (DateTime.TryParse(st, out d))
                            OrderDate.InnerText = string.Format("{0:yyyy-MM-dd}", d);
                        else
                            OrderDate.InnerText = "";
                        root.AppendChild(OrderDate);

                        tCons += " " + s.Substring(71, 46).Trim();

                        s = sr.ReadLine();
                        tCons += " " + s.Substring(71, 46).Trim();


                        s = sr.ReadLine();

                        tCons += " " + s.Substring(71, 46).Trim();

                        XmlElement ConsigneeID = CreateElement("ConsigneeID");
                        int i01 = tCons.IndexOf("RUC");
                        if (i01 == -1)
                        {
                            ConsigneeID.InnerText = "FILUET";
                        }
                        else
                        {
                            string t02 = tCons.Substring(i01, tCons.Length-i01-1);
                            string[] t03 = t02.Split(new char[] { ' ' });
                            ConsigneeID.InnerText = t03[0];
                        }

                        //string[] s1 = tCons.Split(new char[] { ' ' });
                        //ConsigneeID.InnerText = s1[s1.Length - 3];

                        root.AppendChild(ConsigneeID);
                        if (ConsigneeID.InnerText == "RUCB20")
                            OrderType.InnerText = "07";

                        XmlElement DateScheduled = CreateElement("DateScheduled");
                        st = s.Substring(22, 11).Trim();
                        if (DateTime.TryParse(st, out d))
                            DateScheduled.InnerText = string.Format("{0:yyyy-MM-dd}", d);
                        else
                            DateScheduled.InnerText = "";
                        root.AppendChild(DateScheduled);
                        headerRead = true;

                        for (int x = 1; x < 8; x++)
                            s = sr.ReadLine();

                        #endregion
                    }
                    else
                    {
                        for (int x = 1; x < 20; x++)
                            s = sr.ReadLine();
                    }
                }

                if (s.Length > 4)
                {
                    st = s.Substring(0, 4).Trim();
                    int i;
                    if (int.TryParse(st, out i))
                    {
                        _sku = s.Substring(4, 10).Trim();

                        st = s.Substring(62, 10).Trim();
                        int.TryParse(st, out _qtyPick);

                        st = s.Substring(73, 4).Trim();
                        _unit = (st == "EA") ? "PC" : "";

                        _rev = s.Substring(109, 3).Trim();

                        st = s.Substring(137, 9).Trim();
                        if (DateTime.TryParse(st, out d))
                            _expDate = string.Format("{0:yyyy-MM-dd}", d);
                        else
                            _expDate = "";

                        //Ola edited 2015-11-10
                        _lot = s.Substring(123, 15).Split('-')[0].Trim();

                        XmlElement Item = CreateElement("Item");

                        XmlElement SKU = CreateElement("SKU");
                        SKU.InnerText = _sku;
                        Item.AppendChild(SKU);

                        XmlElement UnitCode = CreateElement("UnitCode");
                        UnitCode.InnerText = _unit;
                        Item.AppendChild(UnitCode);

                        XmlElement Qty = CreateElement("Qty");
                        Qty.InnerText = _qtyPick.ToString();
                        Item.AppendChild(Qty);

                        XmlElement DateExp = CreateElement("DateExp");
                        DateExp.InnerText = _expDate;
                        Item.AppendChild(DateExp);

                        XmlElement RevNo = CreateElement("RevNo");
                        RevNo.InnerText = _rev;
                        Item.AppendChild(RevNo);

                        XmlElement LOT = CreateElement("LOT");
                        //Ola edited 2015-11-10
                        LOT.InnerText = _lot;
                        Item.AppendChild(LOT);

                        Items.AppendChild(Item);
                    }
                }

            } while (!sr.EndOfStream);

            root.AppendChild(Items);
            xdoc.AppendChild(root);
            xdoc.InsertBefore(decl, root);

            if (headerRead)
            {
                StringBuilder sb = new StringBuilder();
                XmlWriterSettings settings = new XmlWriterSettings();
                settings.Indent = true;
                settings.OmitXmlDeclaration = true;
                XmlWriter writer = XmlWriter.Create(sb, settings);
                xdoc.WriteTo(writer);
                writer.Close();

                //System.Diagnostics.EventLog.WriteEntry("test", sb.ToString());
                byte[] byteArray = Encoding.ASCII.GetBytes(sb.ToString());
                MemoryStream NewStream = new MemoryStream(byteArray);

                pInMsg.BodyPart.Data = NewStream;
                pInMsg.Context.Promote("MessageType", "http://schemas.microsoft.com/BizTalk/2003/system-properties", "http://ExchangeApp.HRBL_Bulk#HRBL_BulkOrder");
                _msgs.Enqueue(pInMsg);

                string fPath = this.FileCopySuccess + string.Format("{0:yyyy} год\\{0:yyMMdd}\\", DateTime.Today);
                string x = fPath + fileName;
                FileInfo info = new FileInfo(x);
                int i = 1;
                while (info.Exists)
                {
                    x = fPath + Path.GetFileNameWithoutExtension(fileName) + " copy #" + i.ToString() + Path.GetExtension(fileName);
                    info = new FileInfo(x);
                    i++;
                }
                Directory.CreateDirectory(fPath);
                SaveFile(stream, x);
            }
            else
            {
                string x = this.FileCopyFail + fileName;
                FileInfo info = new FileInfo(x);
                int i = 1;
                while (info.Exists)
                {
                    x = this.FileCopyFail + Path.GetFileNameWithoutExtension(fileName) + " copy #" + i.ToString() + Path.GetExtension(fileName);
                    info = new FileInfo(x);
                    i++;
                }
                Directory.CreateDirectory(this.FileCopyFail);
                SaveFile(stream, x);
            }
        }

        private void SaveFile(Stream st, string file)
        {
            try
            {
                using (Stream fs = File.Create(file, (int)st.Length, FileOptions.None))
                {
                    st.Seek(0, SeekOrigin.Begin);
                    st.CopyTo(fs);
                }
            }
            catch (Exception ex)
            {
                EventLog.WriteEntry("ExchangeApp", "Error create copy of bulk order file (" + file + ").\r\n" + ex.Message,EventLogEntryType.Error);
            }
        }

        public IBaseMessage GetNext(IPipelineContext pContext)
        {
            if (_msgs.Count > 0)
                return (IBaseMessage)_msgs.Dequeue();
            else
                return null;
        }

        #endregion

        private XmlElement CreateElement(string nodeName)
        {
            return xdoc.CreateElement("", nodeName, @"http://ExchangeApp.HRBL_Bulk");
        }


    }
}
