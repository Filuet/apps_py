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

namespace ExAppPipelineInOrder
{
    [ComponentCategory(CategoryTypes.CATID_PipelineComponent)]
    [ComponentCategory(CategoryTypes.CATID_Any)]
    [Guid("EC551D8D-B6BA-42B3-BA50-189CA0DB11B1")]

    public class HRBLInOrderDisassembler : IComponentUI, IBaseComponent, IDisassemblerComponent, IPersistPropertyBag
    {
        Queue _msgs = new Queue();
        string xFile;

        //static void HRBLInOrderDisassembler()
        //{ }

        #region IBaseComponent Members
        public string Description
        {
            get
            {
                return "Inbound Order TXT file into XML";
            }
        }

        public string Name
        {
            get
            {
                return "HRBLInOrderDisassembler";
            }
        }

        public string Version
        {
            get
            {
                return "1.0.0.0";
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
            classID = new Guid("EC551D8D-B6BA-42B3-BA50-189CA0DB11B1");
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

            try
            {
                XmlDocument xdoc;
                xdoc = new XmlDocument();
                XmlDeclaration decl;
                decl = xdoc.CreateXmlDeclaration("1.0", "utf-8", null);
                XmlElement root = CreateElement(xdoc, "InOrderHL");
                XmlElement Items = CreateElement(xdoc, "Items");

                xdoc.AppendChild(root);
                xdoc.InsertBefore(decl, root);

                #region Header

                XmlElement OrdNo = CreateElement(xdoc, "OrderNo");
                OrdNo.InnerText = "";
                root.AppendChild(OrdNo);

                XmlElement OwnerID = CreateElement(xdoc, "OwnerID");
                OwnerID.InnerText = "HERBALIFE";
                root.AppendChild(OwnerID);

                XmlElement OrderType = CreateElement(xdoc, "OrderType");
                OrderType.InnerText = "01";
                root.AppendChild(OrderType);

                XmlElement isApproved = CreateElement(xdoc, "isApproved");
                isApproved.InnerText = "1";
                root.AppendChild(isApproved);

                XmlElement Status = CreateElement(xdoc, "Status");
                Status.InnerText = "0";
                root.AppendChild(Status);

                #endregion

                root.AppendChild(Items);

                string _s = sr.ReadLine(); // headers
                string ordNo = string.Empty;

                while (!sr.EndOfStream)
                {
                    string[] ts = sr.ReadLine().Replace("\"",string.Empty).Split(',');
                    if (ts.Length != 6)
                        throw (new Exception("Wrong number of fields in csv file"));

                    if (string.IsNullOrEmpty(ordNo))
                        ordNo = ts[0];
                    else if (ordNo != ts[0])
                    {
                        AddNewMessage(xdoc, pInMsg); // add new message to queue
                        Items.RemoveAll();
                        ordNo = ts[0];
                    }
                    OrdNo.InnerText = ordNo;

                    XmlElement Item = CreateElement(xdoc, "Item");

                    XmlElement SKU = CreateElement(xdoc, "SKU");
                    SKU.InnerText = ts[2];
                    Item.AppendChild(SKU);

                    XmlElement SSCC = CreateElement(xdoc, "SSCC");
                    SSCC.InnerText = ts[1];
                    Item.AppendChild(SSCC);

                    XmlElement UnitCode = CreateElement(xdoc, "UnitCode");
                    UnitCode.InnerText = "PC";
                    Item.AppendChild(UnitCode);

                    DateTime _expDate = new DateTime();
                    if (!DateTime.TryParse(ts[4], out _expDate))
                        _expDate = DateTime.Today;
                    XmlElement DateExp = CreateElement(xdoc, "DateExp");
                    DateExp.InnerText = string.Format("{0:yyyy-MM-dd}", _expDate);
                    Item.AppendChild(DateExp);

                    double _qty = 0;
                    double.TryParse(ts[5], out _qty);
                    XmlElement Qty = CreateElement(xdoc, "Qty");
                    Qty.InnerText = _qty.ToString();
                    Item.AppendChild(Qty);

                    XmlElement LOT = CreateElement(xdoc, "LOT");
                    LOT.InnerText = ts[3];
                    Item.AppendChild(LOT);

                    Items.AppendChild(Item);
                }

                AddNewMessage(xdoc, pInMsg);

                xFile = this.FileCopySuccess + fileName;
            }
            catch (Exception ex)
            {
                EventLog.WriteEntry("ExchangeApp Receive Pipeline InOrder", ex.Message, EventLogEntryType.Error);
                xFile = this.FileCopyFail + fileName;
            }
            finally
            {
                FileInfo info = new FileInfo(xFile);
                int i = 1;
                while (info.Exists)
                {
                    xFile = this.FileCopySuccess + Path.GetFileNameWithoutExtension(fileName) + " copy #" + i.ToString() + Path.GetExtension(fileName);
                    info = new FileInfo(xFile);
                    i++;
                }
                SaveFile(stream, xFile);
            }
        }

        private void AddNewMessage(XmlDocument xdoc, IBaseMessage pInMsg)
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
            pInMsg.Context.Promote("MessageType", "http://schemas.microsoft.com/BizTalk/2003/system-properties", "http://ExchangeApp.HRBL_InOrder#InOrderHL");
            _msgs.Enqueue(pInMsg);
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
                EventLog.WriteEntry("ExchangeApp Receive Pipeline InOrder", "Error create copy of Inbound Order file (" + file + ").\r\n" + ex.Message, EventLogEntryType.Error);
            }
        }

        private XmlElement CreateElement(XmlDocument xdoc, string nodeName)
        {
            return xdoc.CreateElement("ns0", nodeName, @"http://ExchangeApp.HRBL_InOrder");
        }

        public IBaseMessage GetNext(IPipelineContext pContext)
        {
            if (_msgs.Count > 0)
                return (IBaseMessage)_msgs.Dequeue();
            else
                return null;
        }

        #endregion
    }
}
