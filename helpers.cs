using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Linq;
using System.Text;
using System.Xml;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Net.Http;
using System.Net.Http.Headers;


namespace cdd
{
    public class Helpers
    {

        // Compare 2 DataTables and output the findings
        public static DataTable GetSQLTable(string dbserver, string database, string TableName)
        {
            DataTable SQLTable = new DataTable();
            try
            {
                string connString = GetConnectionString(dbserver, database);
                string query = "SELECT COLUMN_NAME FROM DMAS.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'" + TableName + "'";

                SqlConnection conn = new SqlConnection(connString);
                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(SQLTable);
                conn.Close();
                da.Dispose();

            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred in Get SQL Table: '{0}'", e);
      
                
            }

            return SQLTable;
        }

        public static DataTable GetAGTable(DataTable datatable)
        {
            DataTable columntable = new DataTable();
            try
            {
                string[] columnNames = (from dc in datatable.Columns.Cast<DataColumn>()
                                        select dc.ColumnName).ToArray();
                DataColumn NewCol;

                NewCol = new DataColumn
                {
                    ColumnName = "COLUMN_NAME",
                    DataType = System.Type.GetType("System.String")
                };
                columntable.Columns.Add(NewCol);

                DataRow NewRow = columntable.NewRow();
                foreach (var str in columnNames)
                {
                    columntable.Rows.Add(str);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred in Get AG Table vs AG: '{0}'", e);
 
            }
            return columntable;
        }


        public static Tuple<bool, string> GetMappedColumn(string[] MappedConfiguration, string AGColumnName)
        {
            for (int i = 0; i < MappedConfiguration.Length; i++)
            {
                string MappedAGColumn = MappedConfiguration[i].Substring(MappedConfiguration[i].LastIndexOf(':') + 1);
                if  (MappedAGColumn == AGColumnName)
                {
                    string MappedSQLColumn = MappedConfiguration[i].Substring(0, MappedConfiguration[i].IndexOf(':'));
                    return Tuple.Create(true, MappedSQLColumn);
                }
            } 
            return Tuple.Create(false,"");
        }

        public static DataTable CompareRows(DataTable SQLtable, DataTable AGtable, DataTable Resulttable,string[] listmappedcolumn)
        {
            string handled_Column = string.Empty;
            try
            {
                //DataTable exceptiontable = new DataTable();
                //DataColumn NewCol;
                Tuple<bool, string> MappedColumn;

                //NewCol = new DataColumn();
                //NewCol.ColumnName = "COLUMN_NAME";
                //NewCol.DataType = System.Type.GetType("System.String");
                //exceptiontable.Columns.Add(NewCol);

                //NewCol = new DataColumn();
                //NewCol.ColumnName = "EXCEPTION";
                //NewCol.DataType = System.Type.GetType("System.String");
                //exceptiontable.Columns.Add(NewCol);

                //DataRow NewRow;
                /*
                Console.WriteLine("-----------------------------------------------------------------------------------------");
                Console.WriteLine("TABLE NAME : {0} ", Resulttable.TableName);
                int j = 0;
                foreach (DataColumn dc in Resulttable.Columns)
                {
                    Console.WriteLine("--> {0} ", Resulttable.Columns[j].ColumnName.ToString());
                    j++;
                }
                Console.WriteLine("-----------------------------------------------------------------------------------------");
                */

                for (int i = AGtable.Rows.Count - 1; i >= 0; i--)
                {
                    bool isfound = false;
                    foreach (DataRow SQLrow in SQLtable.Rows)
                    {
                        var SQLarray = SQLrow.ItemArray;
                        string CurrentColumnValue = AGtable.Rows[i]["COLUMN_NAME"].ToString();



                        if (SQLarray.Contains(CurrentColumnValue))
                        {
                            isfound = true;
                            break;
                        }
                    }

                    if (!isfound)
                    {
                        // case where column should be mapped to a new column

                        MappedColumn = GetMappedColumn(listmappedcolumn, AGtable.Rows[i]["COLUMN_NAME"].ToString());
                        
                        if (MappedColumn.Item1 == true)
                        {
                            Resulttable.Columns[i].ColumnName = MappedColumn.Item2.ToString();
                        }
                        else
                        { 
                            handled_Column = Resulttable.Columns[i].ToString();
                            Resulttable.Columns.Remove(Resulttable.Columns[i]);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred in Compare Rows SQL table vs AG: '{0}' ---- Column issue : {1} ------", e, handled_Column);
            }
            return Resulttable;
        }

        // method to kill the program entirely when it is invoked : avoid the infinite loop when requesting API
        public static void Killprogram(string dbserver, string database, string configtable)
        {
            DataTable datatable = new DataTable();

            try
            {
                string connString = GetConnectionString(dbserver, database);
                string query = "select * from " + configtable + " Where [Category]='process' and [Key]='kill'";

                SqlConnection conn = new SqlConnection(connString);
                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(datatable);
                conn.Close();
                da.Dispose();

            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred in Kill Process : '{0}'", e);
     
            }
            
             if (datatable.Rows[0]["Value"].ToString() == "YES")
             {
                 System.Environment.Exit(0);
             }
        }


        // log informations into text file in system folder

        private static void AddText(FileStream fs, string value)
        {
            byte[] info = new UTF8Encoding(true).GetBytes(value);
            fs.Write(info, 0, info.Length);
        }

        public static void FileWatcher(string filepath)
        {
            List<string> linesList = File.ReadAllLines(filepath).ToList();
            try
            {
                linesList.RemoveAt(0);
            }
            catch (Exception e)
            {

            }
            File.WriteAllLines(filepath, linesList.ToArray());

            using (FileStream fs = File.Create(filepath))
            {
                string line = "Load CDD into ODS completed => " +  string.Format("{0:yyyy-MM-dd HH:mm}", DateTime.Now);
                AddText(fs, line);
            }
        }

        public static void Loginfo(string infotolog, string logtype)
        {
            string path = string.Empty;
            if (logtype == "INFO")
            {
              path = ConfigurationManager.AppSettings["logpath"].ToString() + "LOADPROCESS_LOG_" + string.Format("{0:yyyy-MM-dd}", DateTime.Now) + ".txt"; // path to file
            }
            else
            {
                if (logtype == "ERROR")
                {
                    path = ConfigurationManager.AppSettings["logpath"].ToString() + "LOADPROCESS_ERROR_LOG_" + string.Format("{0:yyyy-MM-dd}", DateTime.Now) + ".txt"; // path to file
                }
                else
                {
                    path = ConfigurationManager.AppSettings["logpath"].ToString() + "LOADPROCESS_STEPBYSTEP_LOG_" + string.Format("{0:yyyy-MM-dd}", DateTime.Now) + ".txt";
                }
            }

            if (File.Exists(path))
            {
                File.Delete(path);
            }          

            using (FileStream fs = File.Create(path))
            {
             
                string[] lines = infotolog.Split(new string[] { "\r\n", "\n" }, StringSplitOptions.None);

                int i = 0;
                for (i = 0; i < lines.Count(); i++)
                {
                    
                    AddText(fs, lines[i]);
                    AddText(fs, "\r\n");
                }

            }
        }


        // Connection string to the db Server and related database declacred in app.config
        public static string GetConnectionString(string dbserver, string database)
        {
            string ConnectionString = string.Empty;
            try
            {
                ConnectionString = @"Data Source=" + dbserver + ";Integrated Security=true;Initial Catalog=" + database + ";";
            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred in get connection: '{0}'", e);
               
            }

            return ConnectionString;
        }


        // method to load configuration data from Configtable declared in app.config
        public static DataTable LoadConfiguration(string dbserver, string database, string configtable)
        {
            DataTable datatable = new DataTable();

            try
            {
                string connString = GetConnectionString(dbserver, database);
                string query = "select * from " + configtable;

                SqlConnection conn = new SqlConnection(connString);
                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(datatable);
                conn.Close();
                da.Dispose();

            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred in load configuration : '{0}'", e);
  
            }
            return datatable;
        }


        // given a Category and a Key, this function returns the related value.
        public static string ReadConfiguration(DataTable datatable, string category, string key)
        {
            DataRow[] value;
            string expression = "Category Like '" + category + "' and Key Like '" + key + "'";

            value = datatable.Select(expression);
            return value[0][3].ToString();
        }

        // given a Category and a Key, this function returns the list of related value but only for those who are enabled.
        public static string[] ReadListConfiguration(DataTable datatable, string category, string key)
        {
            DataRow[] value;

            value = datatable.Select("Category Like '" + category + "' and Key Like '" + key + "' and Enabled = 1");

            string[] result = new string[value.Length];
            int i = 0;

            try
            {

                foreach (var dr in value)
                {
                    result[i] = value[i][3].ToString();
                    i++;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred in list configuration : '{0}'", e);
  
            }
            return result;

        }

  

        public static HttpClient WebAuthenticationWithToken()
        {
            HttpClient confClient = new HttpClient();

            try
            {
                // PROD
                confClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + Cdd.Token) ; //"eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6ImZiYmVkdHNyZXBvcnRpbmdAYm5wcGFyaWJhc2ZvcnRpcy5jb20iLCJ0ZW5hbnRJZCI6IjAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMCIsImVtYWlsIjpudWxsLCJmaXJzdE5hbWUiOm51bGwsImxhc3ROYW1lIjpudWxsLCJyb2xlcyI6bnVsbCwidXNlcklkIjozMjY1MDEsImp0aSI6IjA2ODI5N2ZiLWE1ODYtNDk4OS1hOTU0LThhODU2MzgyMjExMyIsImV4cCI6MTU3MTc0NjAwN30.WgHZDdSRNKNaGRJmflje0_looKBH-ozh7Ikp_pdswg_OsUBxhtu13NTEx18Qw-Ea3RPiaZ6EHQiVjH3lklvqBA");

                //TEST
                //confClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + "eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6InJlcG9ydGluZ0BibnBwYXJpYmFzZm9ydGlzLmNvbSIsInRlbmFudElkIjoiMDAwMDAwMDAtMDAwMC0wMDAwLTAwMDAtMDAwMDAwMDAwMDAwIiwidXNlcklkIjo5NTk5MywianRpIjoiMDc3NTNjODEtOTg4Ny00ZWMyLTlmODgtNjEyMTNkOWFmZGJjIiwiZXhwIjoxNTk5NjM2NDIyfQ.6z6AIBUA8dDWhAzbnZo8Txb_Gh1RRuOsEhgmXDvmPAMfOWPDhDkMFoIIuFjZ17RaSjxBs4Un9h9dxRBNQyUQEg");

                //QA
                //confClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + "eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6ImFsaS5iZWxjYWlkQGJucHBhcmliYXNmb3J0aXMuY29tIiwidGVuYW50SWQiOiIwMDAwMDAwMC0wMDAwLTAwMDAtMDAwMC0wMDAwMDAwMDAwMDAiLCJ1c2VySWQiOjExNTkzMzcsImp0aSI6ImI4MzZiZmU0LWY5YjgtNGYxMS1hOWQxLWVmODgwNjYyMzU5OSIsImV4cCI6MTYxMDYzMDgzNH0.iIT03F3J1bYNX59s-i5zEjdFUFQp6kTVGk1tYYPQTWkwDsrxF9R_rCOcEhvVsOWhPn63c1qAw6LafSnA9T6i8A");

                // QA 7.3 : eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6ImFsaS5iZWxjYWlkQGJucHBhcmliYXNmb3J0aXMuY29tIiwidGVuYW50SWQiOiIwMDAwMDAwMC0wMDAwLTAwMDAtMDAwMC0wMDAwMDAwMDAwMDAiLCJ1c2VySWQiOjExNTkzMzcsImp0aSI6ImI4MzZiZmU0LWY5YjgtNGYxMS1hOWQxLWVmODgwNjYyMzU5OSIsImV4cCI6MTYxMDYzMDgzNH0.iIT03F3J1bYNX59s-i5zEjdFUFQp6kTVGk1tYYPQTWkwDsrxF9R_rCOcEhvVsOWhPn63c1qAw6LafSnA9T6i8A


                confClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                confClient.Timeout = TimeSpan.FromMilliseconds(10000000);

            }
            catch (WebException e)
            {
                Console.WriteLine("An error occurred in HTTP request (web exception) : {0}- trygain ", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred in HTTP request (other exception) : {0}- trygain ", e.Message);
 
            }
            return confClient;
        }

        public static string WebRequestWithToken(HttpClient confClient, string url)
        {
            string json = string.Empty;
            bool tryagain = true;

            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;

            while (tryagain)
            {
                try
                {
                    HttpResponseMessage message = confClient.GetAsync(url).Result;

                    if (message.IsSuccessStatusCode)
                    {
                        var inter = message.Content.ReadAsStringAsync();
                        //json = JsonConvert.DeserializeObject<string>(inter.Result);
                        json = inter.Result;


                        json.Replace("\"", @"""");
                        if (json.TrimStart().StartsWith("<") == false)
                        {
                            tryagain = false;
                        }

                    }
                    else
                    {
                        //Console.WriteLine("Url : {0} - The status Message is : {1}", url, message.StatusCode);
                        //Cdd.logger.Error("Url : {0} - The status Message is : {1}", url, message.StatusCode);
                        if (message.StatusCode.ToString() == "NotFound")
                        {
                            //Console.WriteLine("The status code is : {0} for this url : <{1}>", message.StatusCode.ToString(), url);
                            //Cdd.logger.Error("The status code is : {0} for this url : <{1}>", message.StatusCode.ToString(), url);
                            tryagain = false;
                            return "NotFound";
                        }
                    }
                }
                catch (WebException e)
                {
                    Console.WriteLine("URL : {0} + '\n' + An error occurred in HTTP request (web exception) : {1}- trygain ", url, e.Message);
                  
                    if (e.Status == WebExceptionStatus.ProtocolError)
                    {
                        WebResponse resp = e.Response;
                        using (StreamReader sr = new StreamReader(resp.GetResponseStream()))
                        {
                            string srrep = sr.ReadToEnd();
                            
                        }
                    }
                }
                catch (Exception e)
                {
                    tryagain = false;
                    return "NotFound";
                }
            }
            return json;
        }


        public static void RenameXMLNode(XmlDocument doc, XmlNode oldRoot, string newname)
        {
            XmlNode newRoot = doc.CreateElement(newname);

            foreach (XmlNode childNode in oldRoot.ChildNodes)
            {
                newRoot.AppendChild(childNode.CloneNode(true));
            }
            XmlNode parent = oldRoot.ParentNode;
            //parent.AppendChild(newRoot);
            parent.InsertBefore(newRoot, oldRoot);
            parent.RemoveChild(oldRoot);
        }

        public static void TraverseNodes(XmlDocument doc, XmlNodeList nodes, string childrenTable)
        {
            foreach (XmlNode node in nodes)
            {
                if (childrenTable == "applications")
                {

                    if (node.Name == "applications")
                    {
                        XmlNode nextsibling = node.NextSibling;
                        RenameXMLNode(doc, node, node.ParentNode.Name + "_app");

                        while (nextsibling != null && nextsibling.Name == "applications")
                        {
                            XmlNode CurrentNextSibling = nextsibling.NextSibling;
                            RenameXMLNode(doc, nextsibling, nextsibling.ParentNode.Name + "_app");
                            nextsibling = CurrentNextSibling;
                        }
                    }
                    TraverseNodes(doc, node.ChildNodes, "applications");
                }

                if (childrenTable == "environments")
                {

                    if (node.Name == "environments")
                    {
                        XmlNode nextsibling = node.NextSibling;
                        RenameXMLNode(doc, node, node.ParentNode.Name + "_env");

                        while (nextsibling != null && nextsibling.Name == "environments")
                        {
                            XmlNode CurrentNextSibling = nextsibling.NextSibling;
                            RenameXMLNode(doc, nextsibling, nextsibling.ParentNode.Name + "_env");
                            nextsibling = CurrentNextSibling;
                        }
                    }
                    TraverseNodes(doc, node.ChildNodes, "environments");
                }
            }
        }

        // in order to avoid to many children tables in SQL stagging, I aggregate  all those table in "data" which become the master table with all 1-1 attributes
        // the "_data" are left with this integration as the relation with "data" table is 1-N
        //public static void DatasetPostProcessing(DataSet dataset)
        //{
        //    //if (ContainsRepeatedData(dataset))
        //    //{
        //    //    foreach (DataTable dt in dataset.Tables)
        //    //    {
        //    //        if (dt.TableName != "data" && dt.TableName != "root")
        //    //        {
        //    //            if (dt.TableName.Contains("_data"))
        //    //            {
        //    //                try
        //    //                {
        //    //                    dataset.Tables[dt.TableName].Merge(dataset.Tables[dt.TableName.Remove(dt.TableName.Length - 5, 5)]);
        //    //                    dataset.Tables[dt.TableName].AcceptChanges();
        //    //                }
        //    //                catch (Exception e)
        //    //                {
        //    //                    Cdd.logger.Info(e, "An error occurred in Merge data table : '{0}'");
        //    //                }
        //    //            }
        //    //            else
        //    //            {
        //    //                if (!DataTableWithDataExist(dataset, dt))
        //    //                {
        //    //                    for (int i = 0; i < dt.Columns.Count; i++)
        //    //                    {
        //    //                        if (dt.Columns[i].ColumnName != "data_Id")
        //    //                        {
        //    //                            dt.Columns[i].ColumnName = dt.TableName + "_at_" + dt.Columns[i].ColumnName;
        //    //                        }
        //    //                    }

        //    //                    try
        //    //                    {
        //    //                        dataset.Tables["data"].Merge(dt);
        //    //                        dataset.Tables["data"].AcceptChanges();
        //    //                    }
        //    //                    catch (Exception e)
        //    //                    {
        //    //                        Cdd.logger.Info(e, "An error occurred in Merge data table : '{0}'");
        //    //                    }
        //    //                }
        //    //            }
        //    //        }
        //    //    }
        //        // first process _data by adding all parent attributes


        //    //}
        //    //else
        //    //{

        //        // before adding tables, look to the tables where no data_id column and add them to their parents

        //        foreach (DataTable dt in dataset.Tables)
        //        {
        //           if (!dt.Columns.Contains("data_Id"))
        //           {
        //            string ParentTableName = LookForParentTable(dt);
        //            for (int i = 0; i < dt.Columns.Count; i++)
        //            {
        //                if (dt.Columns[i].ColumnName != ParentTableName + "_Id")
        //                {
        //                    dt.Columns[i].ColumnName = dt.TableName + "_at2_" + dt.Columns[i].ColumnName;
        //                }
        //                dataset.Tables[ParentTableName].Merge(dt);
        //                dataset.Tables[ParentTableName].AcceptChanges();
        //            }
        //           }

        //        }

        //        foreach (DataTable dt in dataset.Tables)
        //        {
        //            if (dt.TableName != "data" && dt.TableName != "root")
        //            {               
        //                for (int i = 0; i < dt.Columns.Count; i++)
        //                {
        //                    if (dt.Columns[i].ColumnName != "data_Id")
        //                    {
        //                        dt.Columns[i].ColumnName = dt.TableName + "_at1_" + dt.Columns[i].ColumnName;
        //                    }
        //                }
        //                dataset.Tables["data"].Merge(dt);
        //                dataset.Tables["data"].AcceptChanges();
        //            }
        //        }
        //    //}
        //}



        public static bool ContainsRepeatedData(DataSet dataset)
        {
            foreach (DataTable dt in dataset.Tables)
            {
                if (dt.TableName.Contains("_data"))
                {
                    return true;
                }
            }

            return false;
        }

        public static bool DataTableWithDataExist(DataSet ds, DataTable dt)
        {
            foreach (DataTable dtc in ds.Tables)
            {
                if (dtc.TableName == dt.TableName + "_data")
                {
                    return true;
                }
            }

            return false;
        }

        public static void GenerateSQLScript(DataSet dataset, string table, string[] GeneratedTableList)
        {
            string[] SQLQueries;
            //GeneratedTableList = new string[10];

            if (dataset.Tables.Count > 0)
            {
                SQLQueries = new string[100];
            }
            else
            {
                SQLQueries = new string[2];
            }
            int j = 2;
            int i = 0;
            SQLQueries[0] = "USE DMAS";
            SQLQueries[1] = "GO" + "\n";

            if (dataset.Tables.Count > 0)
            {
                foreach (DataTable dt in dataset.Tables)
                {
                    if (dt.TableName == "data")
                    {
                        var sql = SQLCreator.GetCreateFromDataTableSQLWithoutPK("DMAS].[dbo].[TB_STG_CDD_" + table.ToUpper(), dt);
                        SQLQueries[j] = sql;
                        j++;

                        GeneratedTableList[i] = table;
                        i++;
                    }
                    else
                    {
                        //if (dt.TableName.Contains("_data"))
                        //{
                            //var sql = SQLCreator.GetCreateFromDataTableSQLWithoutPK("DMAS].[dbo].[TB_STG_Cdd_" + table.ToUpper() + "_" + dt.TableName.Remove(dt.TableName.Length - 5, 5), dt);
                        var sql = SQLCreator.GetCreateFromDataTableSQLWithoutPK("DMAS].[dbo].[TB_STG_CDD_" + table.ToUpper() + "_" + dt.TableName, dt);
                        SQLQueries[j] = sql;
                            j++;

                            //GeneratedTableList[i] = dt.TableName.Remove(dt.TableName.Length - 5, 5);
                        GeneratedTableList[i] = dt.TableName;
                        i++;
                        //}
                    }
                }
            }
            SQLCreator.SQLCreateFile(SQLQueries, table);

            /*
            for (int cur = 0; cur < GeneratedTableList.Length; cur++)
            {
                Console.Write("{0} | ", GeneratedTableList[cur]);
                Console.WriteLine("****************************************************************");
            }
            */
        }


        public static DataSet GeneratedTableForBulkinsert(DataSet dataset, string table, string[] GeneratedTableList)
        {

            int i = 0;
            DataSet datasetSQL = new DataSet();

            if (dataset.Tables.Count > 0)
            {
                foreach (DataTable dt in dataset.Tables)
                {
                    if (dt.TableName == "data")
                    {
                        GeneratedTableList[i] = table;
                        datasetSQL.Tables.Add(dt.Copy());
                        i++;
                    }
                    else
                    {
                        if (dt.TableName.Contains("_data"))
                        {
                            try
                            {
                                GeneratedTableList[i] = dt.TableName;
                                dt.Constraints.Clear();
                                datasetSQL.Tables.Add(dt.Copy());
                                datasetSQL.Tables[i].TableName = datasetSQL.Tables[i].TableName.Remove(datasetSQL.Tables[i].TableName.Length - 5, 5);
                                i++;
                            }

                            catch (Exception e)
                            {
                                Cdd.logger.Info(e, "An error occurred in Copy data table : '{0}'");
                            }
                        }
                    }
                }
            }

            return datasetSQL;

        }

        public static void DatasetPostProcessingUpdateDataID(DataSet dataset)
        {
            foreach (DataTable dt in dataset.Tables)
            {
                if (dt.TableName != "data")
                {
                    foreach (DataRow rw in dt.Rows)
                    {
                        try
                        {
                            rw["data_Id"] = rw[dt.TableName + "_Id"];
                        }
                        catch (Exception e)
                        {
                            Cdd.logger.Info(e, "An error occurred in update data_id attribute : '{0}'");
                        }

                    }
                }
            }
        }

        public static string LookForParentTable(DataTable dt)
        {
            string ParentTableName = string.Empty;
            for (int i = 0; i < dt.Columns.Count; i++)
            {
                if (dt.Columns[i].ColumnName.Contains("_Id"))
                    ParentTableName = dt.Columns[i].ColumnName.Remove(dt.Columns[i].ColumnName.Length - 3, 3);
            }

            return ParentTableName;
        }


        public static void CreateTrackLinks(string trackid, string releaseid)
        {
            string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
            string database = ConfigurationManager.AppSettings["database"].ToString();
            string configtable = ConfigurationManager.AppSettings["configtable"].ToString();
            DataTable ConfigTable = Helpers.LoadConfiguration(dbserver, database, configtable);

            // Create the links

            DataTable dt_links = new DataTable();
            DataColumn Col;

            Col = new DataColumn
            {
                ColumnName = "trackid",
                DataType = System.Type.GetType("System.String")
            };
            dt_links.Columns.Add(Col);

            Col = new DataColumn
            {
                ColumnName = "releaseid",
                DataType = System.Type.GetType("System.String")
            };
            dt_links.Columns.Add(Col);

            Col = new DataColumn
            {
                ColumnName = "releasename",
                DataType = System.Type.GetType("System.String")
            };
            dt_links.Columns.Add(Col);

            Col = new DataColumn
            {
                ColumnName = "releaseVersion",
                DataType = System.Type.GetType("System.String")
            };
            dt_links.Columns.Add(Col);

            Col = new DataColumn
            {
                ColumnName = "obj_id",
                DataType = System.Type.GetType("System.String")
            };
            dt_links.Columns.Add(Col);

            Col = new DataColumn
            {
                ColumnName = "type",
                DataType = System.Type.GetType("System.String")
            };
            dt_links.Columns.Add(Col);

            Col = new DataColumn
            {
                ColumnName = "phasename",
                DataType = System.Type.GetType("System.String")
            };
            dt_links.Columns.Add(Col);

            Col = new DataColumn
            {
                ColumnName = "phaseid",
                DataType = System.Type.GetType("System.String")
            };
            dt_links.Columns.Add(Col);

            Col = new DataColumn
            {
                ColumnName = "status",
                DataType = System.Type.GetType("System.String")
            };
            dt_links.Columns.Add(Col);

            Col = new DataColumn
            {
                ColumnName = "phasestatus",
                DataType = System.Type.GetType("System.String")
            };
            dt_links.Columns.Add(Col);

            string query_header = "DECLARE @trackid NVARCHAR(10) DECLARE @releaseid NVARCHAR(10) SET @trackid = " + trackid + " SET @releaseid = " + releaseid + " ";
            string query_body = Helpers.ReadConfiguration(ConfigTable, "query", "tracks_links");
            string query = query_header + query_body;


            string connString = Helpers.GetConnectionString(dbserver, database);
            SqlConnection conn = new SqlConnection(connString);


            using (SqlConnection connection = new SqlConnection(connString))
            {
                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(dt_links);
                conn.Close();
                da.Dispose();
            }

            // Bulkinsert SQL Table
            connString = Helpers.GetConnectionString(dbserver, database);
            conn = new SqlConnection(connString);
            using (SqlConnection connection = new SqlConnection(connString))
            {
                conn.Open();
                SqlBulkCopy bulkcopy = new SqlBulkCopy(conn)
                {
                    DestinationTableName = "[DMAS].[dbo].[TB_STG_CDD_TRACKS_links]"
                };
                bulkcopy.WriteToServer(dt_links);
                dt_links.Clear();
                conn.Close();
            }

        }


    }
}


