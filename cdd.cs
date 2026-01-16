using System;
using System.IO;
using System.Linq;
using System.Xml;
using System.Data;
using System.Data.SqlClient;
using Newtonsoft.Json;
using System.Configuration;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Net.Http;
using NLog;
using System.Runtime.InteropServices;
using System.Diagnostics;

namespace cdd
{

    class Cdd
    {

        // this is used to hide (make the console window application in silent mode) console window
        [DllImport("user32.dll")]
        static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);

        // public properties to store global data across program class 
        public static string ExecutionSteps { get; set; }
        public static string ErrorReporting { get; set; }
        public static string StepbyStep { get; set; }
        public static string Emailadr { get; set; }

        public static string ServerUrl { get; set; }
        public static string Token { get; set; }

        public static HttpClient httpclient;

        public static Logger logger = LogManager.GetCurrentClassLogger();

        // SQLStatement takes a list of tables and apply SQL statement as direct input or via Stored procedure
        static void SQLstatement(string dbserver, string database, string[] listtable, bool is_stagging)
        {

            var connectionString = Helpers.GetConnectionString(dbserver, database);
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                SqlCommand cmd = new SqlCommand
                {
                    Connection = connection
                };
                try
                {
                    if (is_stagging == true)
                    {
                        foreach (string t in listtable)
                        {
                            //Console.WriteLine(t);
                            cmd.CommandText = "dbo.st_truncate_table";
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.CommandTimeout = 0;
                            cmd.Parameters.Clear();
                            cmd.Parameters.Add(new SqlParameter("@Tablename", "[DMAS].[dbo].[TB_STG_CDD_" + t.ToString() + "]"));
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                catch (Exception e)
                {
                    logger.Error(e, "Error in Sqlstatement");
                    ErrorReporting = ErrorReporting + e;
                    //Email.SendMail(Cdd.Emailadr, "REPORTED ERROR in  @ SQLstatement -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ErrorReporting);
                }

                connection.Close();
            }
        }

        public static DataSet Initialize_Dataset(DataSet dataset, DataTable ConfigTable, string table)
        {
            string urlbase = ServerUrl + "design/0000/v1/releases/";
            //string urlbase = "https://cdd-qa.resnp.sysnp.shared.fortis:8443/cdd/design/0000/v1/releases/";
            //string urlbase = "https://spal005m:8443/cdd/design/0000/v1/releases/";
            
            string credentials = Helpers.ReadConfiguration(ConfigTable, "url", "credentials");
            string json = string.Empty;
           

            string url = string.Empty;
            switch (table)
            {
                case "phases":
                    url = urlbase + "333261/phases?page_size=10000"; // PROD
                    //url = urlbase + "1173643/phases?page_size=10000";
                    //url = urlbase + "333261/phases?page_size=10000"; // just for test in QA
                    //url = urlbase + "65540/phases?page_size=10000"; // just for test in TEST
                    break;
                case "tasks":
                    url = urlbase + "1574754/phases/1574845/tasks?page_size=10000"; // PROD
                    //url = urlbase + "333261/phases/333391/tasks?page_size=10000"; // just for test in QA
                    //url = urlbase + "65540/phases/65990/tasks?page_size=10000"; // just for test in TEST
                    break;
                case "activities":
                    //url = urlbase + "2713283/activities?page_size=100"; // PROD
                    url = urlbase + "772090/activities?page_size=100"; // just for test in QA
                    //url = urlbase + "65540/activities?page_size=100"; // just for test in TEST

                    break;
                case "application-versions":
                    url = urlbase + "333261/application-versions?page_size=100"; // PROD
                    //url = urlbase + "7809/application-versions?page_size=100"; // just for test in QA
                    //url = urlbase + "65540/application-versions?page_size=100"; // just for test in TEST
                    //url = urlbase + "1162183/application-versions?page_size=100";
                    //url = urlbase + "1457613/application-versions?page_size=100"; 
                    break;
                case "selected-application-versions":
                    url = urlbase + "333261/applications/333072/application-versions?page_size=100";
                    //url = urlbase + "333261/applications/333072/application-versions?page_size=100"; //just for test in TEST
                    break;
                case "available-dependencies":
                    url = urlbase + "1162183/application-versions?page_size=100"; ;
                    break;
            }

            //json = Helpers.WebRequestWithCredentials(url, credentials);
            json = Helpers.WebRequestWithToken(httpclient, url);
            

            string jsonempty = "{\"data\":[]}";

            try
            {

                if (json.TrimStart().StartsWith("<") == false)
                {
                    if (!json.Contains(jsonempty))
                    {
                        XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json, "root");

                        // in case of multiple "data" nodes

                        XmlElement root = doc.DocumentElement;
                        XmlNodeList GlobalNodeList = root.ChildNodes;

                        foreach (XmlNode node in GlobalNodeList)
                        {
                            Helpers.TraverseNodes(doc, node.ChildNodes, "applications");
                            //Console.WriteLine("--------------------------------------------------");
                        }


                        root = doc.DocumentElement;
                        GlobalNodeList = root.ChildNodes;

                        foreach (XmlNode node in GlobalNodeList)
                        {
                            Helpers.TraverseNodes(doc, node.ChildNodes, "environments");
                            //Console.WriteLine("--------------------------------------------------");
                        }


                        

                        // *******************************************

                        json = string.Empty;

                        foreach (DataTable dataTable in dataset.Tables)
                            dataTable.BeginLoadData();

                        dataset.ReadXml(new XmlTextReader(new StringReader(doc.OuterXml)));

                        foreach (DataTable dataTable in dataset.Tables)
                            dataTable.EndLoadData();
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error(e, "An error occurred in Dataset initialisation :");
                ErrorReporting = ErrorReporting + e;
                //Email.SendMail(Cdd.Emailadr, "REPORTED ERROR in  @ Dataset Initialisation -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ErrorReporting);

                Console.WriteLine(ErrorReporting);
            }

            logger.Info("Data Initialization has completed");

           

            return dataset;
        }

        static void LoadWorkItems(DataSet dataset, DataTable ConfigTable)
        {

            string urlbase = ServerUrl + "design/0000/v1/releases/";
            int pagesize = 100;

            //https://cdd.res.sys.shared.fortis:8443/cdd/design/0000/v1/releases/1457613/content-sources/content-items

            //string urlbase = "https://cdd-qa.resnp.sysnp.shared.fortis:8443/cdd/design/0000/v1/releases/";
            //string urlbase = "https://spal005m:8443/cdd/design/0000/v1/releases/";

            string json = string.Empty;
            //string table = "available-dependencies";

            //int pagesize = 100;

            string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
            string database = ConfigurationManager.AppSettings["database"].ToString();
            string connString = Helpers.GetConnectionString(dbserver, database);
            DataTable dtLinks = new DataTable();


            //string query = "SELECT dbo.TB_STG_CDD_RELEASE_data.data_Id, dbo.TB_STG_CDD_RELEASE_data.id AS ReleaseId, dbo.TB_STG_CDD_RELEASE_applications.id AS ApplicationId " +
            //               "FROM dbo.TB_STG_CDD_RELEASE_data INNER JOIN " +
            //               "dbo.TB_STG_CDD_RELEASE_applications ON dbo.TB_STG_CDD_RELEASE_data.data_Id = dbo.TB_STG_CDD_RELEASE_applications.data_Id";

            string query = "SELECT dbo.TB_STG_CDD_RELEASE_data.id AS ReleaseId FROM dbo.TB_STG_CDD_RELEASE_data" ;

            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(query, conn);
            conn.Open();
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            da.Fill(dtLinks);
            conn.Close();
            da.Dispose();
            query = string.Empty;

            try
            {
                string url = urlbase;
                int current_position_in_dataset = 0;

                foreach (DataRow rw in dtLinks.Rows)
                {
                    //url = url + "release_id=" + rw["ReleaseId"].ToString() + "&application_id=" + rw["ApplicationId"].ToString();      // " / " + table + "?page_size=" + pagesize;

                    url = url + rw["ReleaseId"].ToString() + "/content-sources/content-items?page_size=" + pagesize;

                    json = Helpers.WebRequestWithToken(httpclient, url);
                    //string jsonempty = "{\"data\":[]}";
                    try
                    {
                        if (json.TrimStart().StartsWith("<") == false)
                        {
                            if (!json.Contains("\"data\":[]"))
                            {
                                XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json, "root");

                                json = string.Empty;

                                foreach (DataTable dataTable in dataset.Tables)
                                    dataTable.BeginLoadData();

                                dataset.ReadXml(new XmlTextReader(new StringReader(doc.OuterXml)));

                                foreach (DataTable dataTable in dataset.Tables)
                                    dataTable.EndLoadData();

                                // fill releaseid and applicationid

                                if (!dataset.Tables["data"].Columns.Contains("releaseid")) // && !dataset.Tables["data"].Columns.Contains("applicationid"))
                                {
                                    dataset.Tables["data"].Columns.Add("releaseid");
                                    //dataset.Tables["data"].Columns.Add("applicationid");
                                }

                                for (int i = current_position_in_dataset; i < dataset.Tables["data"].Rows.Count; i++)
                                {
                                    dataset.Tables["data"].Rows[i]["releaseid"] = rw["ReleaseId"];
                                    //dataset.Tables["data"].Rows[i]["applicationid"] = rw["applicationId"];
                                    current_position_in_dataset++;
                                }

                            }
                            url = urlbase;
                            //start = start + pagesize;
                        }

                    }
                    catch (Exception e)
                    {
                        
                        logger.Error(e, "An error occurred in load WorkItem Dataset - DeserializeXmlNode :");
                        ErrorReporting = ErrorReporting + e;
                        //Email.SendMail(Cdd.Emailadr, "REPORTED ERROR in  @ load workitem Dataset - DeserializeXmlNode -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ErrorReporting);
                        Console.WriteLine(ErrorReporting);
                    }

                }


                // display columns names of each datatable
                /*
                foreach (DataTable dataTable in dataset.Tables)
                {
                    Console.WriteLine("TABLE : {0}", dataTable.TableName.ToString());
                    foreach (DataColumn dtcol in dataTable.Columns)
                    {
                        Console.WriteLine("Column : {0}", dtcol.ColumnName.ToString());
                    }
                    Console.WriteLine("*************************************************************************************");
                }
                */
                //Console.WriteLine("table {0} is loaded - nb rows {1}", table, TotalResultCount);
            }
            catch (Exception e)
            {
                logger.Error(e, "An error occurred in load WorkItem Dataset :");
                ErrorReporting = ErrorReporting + e;
                //Email.SendMail(Cdd.Emailadr, "REPORTED ERROR in  @ LoadDataset -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ErrorReporting);
                Console.WriteLine(ErrorReporting);

            }
            finally
            {

            }

        }
      
        static void LoadSubDataset(DataSet dataset, DataTable ConfigTable, string table, int level)
        {

            string urlbase = ServerUrl+"design/0000/v1/releases/";
            //string urlbase = "https://cdd-qa.resnp.sysnp.shared.fortis:8443/cdd/design/0000/v1/releases/";
            //string urlbase = "https://spal005m:8443/cdd/design/0000/v1/releases/";

            string credentials = Helpers.ReadConfiguration(ConfigTable, "url", "credentials");
            string json = string.Empty;
            // StringBuilder json;

            int pagesize = 100;

            string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
            string database = ConfigurationManager.AppSettings["database"].ToString();
            string connString = Helpers.GetConnectionString(dbserver, database);
            DataTable dtLinks = new DataTable();

            if (level == 1)
            {
                string allowedReleasesQuery = ConfigurationManager.AppSettings["allowedReleasesQuery"].ToString();
                //string query = "select id from TB_STG_CDD_RELEASE_data"; // where id in (9337427,9338412,9339115)";
                SqlConnection conn = new SqlConnection(connString);
                SqlCommand cmd = new SqlCommand(allowedReleasesQuery, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(dtLinks);
                conn.Close();
                da.Dispose();
                //query = string.Empty;
            }
            else
            {
                string query = "select releaseId, id from TB_STG_CDD_PHASES_data Where className = 'PhaseDto' group by releaseId, id";
                SqlConnection conn = new SqlConnection(connString);
                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(dtLinks);
                conn.Close();
                da.Dispose();
                query = string.Empty;   
            }
            try
            {
                string url = urlbase;
                Initialize_Dataset(dataset, ConfigTable, table);
                foreach (DataRow rw in dtLinks.Rows)
                {
                    if (level == 1)
                    {
                        url = url + rw["id"].ToString() + "/" + table + "?page_size=" + pagesize;
                        //url = "https://cdd.res.sys.shared.fortis:8443/cdd/design/0000/v1/releases/9337427/phases";
                    }
                    else
                    {
                        url = url + rw["releaseId"].ToString() + "/phases/" + rw["id"].ToString() + "/" + table + "?page_size=" + pagesize; 
                        //url = "https://cdd.res.sys.shared.fortis:8443/cdd/design/0000/v1/releases/6382276/phases/6384069/tasks?page_size=100";
                    }

                    //json = Helpers.WebRequestWithCredentials(url, credentials);

                    

                    json = Helpers.WebRequestWithToken(httpclient, url);
                    string jsonempty = "{\"data\":[]}";

                    try
                    {

                        if (json.TrimStart().StartsWith("<") == false)
                        {
                            if (!json.Contains(jsonempty) && json != "NotFound")
                            {
                                XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json, "root");



                                // in case of multiple "applications" nodes

                                XmlElement root = doc.DocumentElement;
                                XmlNodeList GlobalNodeList = root.ChildNodes;

                                foreach (XmlNode node in GlobalNodeList)
                                {
                                    Helpers.TraverseNodes(doc, node.ChildNodes, "applications");
                                    //Console.WriteLine("--------------------------------------------------");
                                }

                                // *******************************************

                                // in case of multiple "applications" nodes

                                root = doc.DocumentElement;
                                GlobalNodeList = root.ChildNodes;

                                foreach (XmlNode node in GlobalNodeList)
                                {
                                    Helpers.TraverseNodes(doc, node.ChildNodes, "environments");
                                    //Console.WriteLine("--------------------------------------------------");
                                }



                                // *******************************************
                                // remove milestonePhaseRelations

                                XmlNodeList nodes = doc.SelectNodes("/root/data/milestonePhaseRelations");
                                for (int i = nodes.Count - 1; i >= 0; i--)
                                {
                                        nodes[i].ParentNode.RemoveChild(nodes[i]);
                                }
                                // *******************************************

                                json = string.Empty;

                                try
                                {

                                    foreach (DataTable dataTable in dataset.Tables)
                                        dataTable.BeginLoadData();

                                    dataset.ReadXml(new XmlTextReader(new StringReader(doc.OuterXml)));

                                    foreach (DataTable dataTable in dataset.Tables)
                                        dataTable.EndLoadData();

                                }
                                catch (Exception e)
                                {

                                }


                                // add forgotten tables in dataset
                                
                                if (table == "phases")
                                {
                                    if (!dataset.Tables.Contains("ownerParties"))
                                    {
                                        DataTable nextTasks = new DataTable
                                        {
                                            TableName = "ownerParties"
                                        };
                                        DataColumn NewCol1 = new DataColumn
                                        {
                                            ColumnName = "email",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol1);

                                        DataColumn NewCol2 = new DataColumn
                                        {
                                            ColumnName = "notificationEnabled",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol2);

                                        DataColumn NewCol3 = new DataColumn
                                        {
                                            ColumnName = "firstName",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol3);

                                        DataColumn NewCol4 = new DataColumn
                                        {
                                            ColumnName = "lastName",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol4);


                                        DataColumn NewCol5 = new DataColumn
                                        {
                                            ColumnName = "superUser",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol5);

                                        DataColumn NewCol6 = new DataColumn
                                        {
                                            ColumnName = "role",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol6);


                                        DataColumn NewCol7 = new DataColumn
                                        {
                                            ColumnName = "name",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol7);

                                        DataColumn NewCol8 = new DataColumn
                                        {
                                            ColumnName = "id",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol8);

                                        DataColumn NewCol9 = new DataColumn
                                        {
                                            ColumnName = "className",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol9);


                                        DataColumn NewCol10 = new DataColumn
                                        {
                                            ColumnName = "data_Id",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol10);

                                        DataColumn NewCol11 = new DataColumn
                                        {
                                            ColumnName = "ownerParties_Id",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol11);

                                        dataset.Tables.Add(nextTasks);
                                    }
                                } 

                                if (table == "tasks")
                                {
                                    if (!dataset.Tables.Contains("prevTasks"))
                                    {
                                        DataTable prevTasks = new DataTable
                                        {
                                            TableName = "prevTasks"
                                        };
                                        DataColumn NewCol1 = new DataColumn
                                        {
                                            ColumnName = "data_Id",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        prevTasks.Columns.Add(NewCol1);

                                        DataColumn NewCol2 = new DataColumn
                                        {
                                            ColumnName = "id",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        prevTasks.Columns.Add(NewCol2);

                                        DataColumn NewCol3 = new DataColumn
                                        {
                                            ColumnName = "className",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        prevTasks.Columns.Add(NewCol3);

                                        dataset.Tables.Add(prevTasks);
                                    }

                                    if (!dataset.Tables.Contains("nextTasks"))
                                    {
                                        DataTable nextTasks = new DataTable
                                        {
                                            TableName = "nextTasks"
                                        };
                                        DataColumn NewCol1 = new DataColumn
                                        {
                                            ColumnName = "data_Id",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol1);

                                        DataColumn NewCol2 = new DataColumn
                                        {
                                            ColumnName = "id",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol2);

                                        DataColumn NewCol3 = new DataColumn
                                        {
                                            ColumnName = "className",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol3);

                                        dataset.Tables.Add(nextTasks);
                                    }
                                    if (!dataset.Tables.Contains("ownerParties"))
                                    {
                                        DataTable nextTasks = new DataTable
                                        {
                                            TableName = "ownerParties"
                                        };
                                        DataColumn NewCol1 = new DataColumn
                                        {
                                            ColumnName = "email",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol1);

                                        DataColumn NewCol2 = new DataColumn
                                        {
                                            ColumnName = "notificationEnabled",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol2);

                                        DataColumn NewCol3 = new DataColumn
                                        {
                                            ColumnName = "firstName",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol3);

                                        DataColumn NewCol4 = new DataColumn
                                        {
                                            ColumnName = "lastName",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol4);


                                        DataColumn NewCol5 = new DataColumn
                                        {
                                            ColumnName = "superUser",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol5);

                                        DataColumn NewCol6 = new DataColumn
                                        {
                                            ColumnName = "role",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol6);


                                        DataColumn NewCol7 = new DataColumn
                                        {
                                            ColumnName = "name",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol7);

                                        DataColumn NewCol8 = new DataColumn
                                        {
                                            ColumnName = "id",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol8);

                                        DataColumn NewCol9 = new DataColumn
                                        {
                                            ColumnName = "className",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol9);


                                        DataColumn NewCol10 = new DataColumn
                                        {
                                            ColumnName = "data_Id",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol10);

                                        DataColumn NewCol11 = new DataColumn
                                        {
                                            ColumnName = "ownerParties_Id",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        nextTasks.Columns.Add(NewCol11);

                                        dataset.Tables.Add(nextTasks);
                                    }
                                }


                                // add forgotten columns
                                DataTableCollection tables = dataset.Tables;
                                if (tables.Contains("executionData"))
                                {
                                    DataTable table1 = dataset.Tables["executionData"];
                                    DataColumnCollection columns = table1.Columns;
                                    if (!columns.Contains("startDate"))
                                    {
                                        DataColumn NewCol1 = new DataColumn
                                        {
                                            ColumnName = "startDate",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        dataset.Tables["executionData"].Columns.Add(NewCol1);

                                        DataColumn NewCol2 = new DataColumn
                                        {
                                            ColumnName = "endDate",
                                            DataType = System.Type.GetType("System.String")
                                        };
                                        dataset.Tables["executionData"].Columns.Add(NewCol2);
                                    }
                                }

                            }
                            url = urlbase;
                            //start = start + pagesize;
                        }

                    }
                    catch (Exception e)
                    {
                       
                        logger.Error(e, "An error occurred in load sub Datasets - DeserializeXmlNode :");
                        ErrorReporting = ErrorReporting + e + '\n' + '\n' + "***** Related Json *****" + '\n' + '\n' + json;
                        //Email.SendMail(Cdd.Emailadr, "REPORTED ERROR in  @ load Datasets - DeserializeXmlNode -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ErrorReporting);
                        Console.WriteLine(ErrorReporting);
                    }
                    try
                    {
                        // Bulkinsert SQL Tables
                        if (json != "NotFound") // skip those are not found
                        {
                            Bulkinsertdynamic(dataset, ConfigTable, dbserver, database, table.ToString());
                            dataset.Clear();
                        }
                    }
                    catch (Exception e)
                    {
                        
                    }
                }

                // display columns names of each datatable
                /*
                foreach (DataTable dataTable in dataset.Tables)
                {
                    Console.WriteLine("TABLE : {0}", dataTable.TableName.ToString());
                    foreach (DataColumn dtcol in dataTable.Columns)
                    {
                        Console.WriteLine("Column : {0}", dtcol.ColumnName.ToString());
                    }
                    Console.WriteLine("*************************************************************************************");
                }
                */
                //Console.WriteLine("table {0} is loaded - nb rows {1}", table, TotalResultCount);
                ExecutionSteps = ExecutionSteps + "table " + table + " is loaded - nb rows : " + '\n';
            }
            catch (Exception e)
            {
                
                logger.Error(e, "An error occurred in SubLoadDataset: ");
                ErrorReporting = ErrorReporting + e;
                //Email.SendMail(Cdd.Emailadr, "REPORTED ERROR in  @ LoadDataset -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ErrorReporting);
                Console.WriteLine(ErrorReporting);

            }
            finally
            {

            }

        }

        //*********************************************************************************************************************************************************


        static void LoadSubDatasetactivities(DataSet dataset, DataTable ConfigTable, string table, int level)
        {

            string urlbase = ServerUrl + "design/0000/v1/releases/";
            //string urlbase = "https://cdd-qa.resnp.sysnp.shared.fortis:8443/cdd/design/0000/v1/releases/";
            //string urlbase = "https://spal005m:8443/cdd/design/0000/v1/releases/";

            //string SQLReleaseID = "select Id from TB_STG_CDD_RELEASE_Data"; 

            string SQLReleaseID = "SELECT  dbo.TB_STG_CDD_RELEASE_data.id "+
                                  "FROM dbo.TB_STG_CDD_RELEASE_data "+
                                  "INNER JOIN dbo.TB_STG_CDD_RELEASE_applications ON dbo.TB_STG_CDD_RELEASE_data.data_Id = dbo.TB_STG_CDD_RELEASE_applications.data_Id "+
                                  "INNER JOIN  dbo.TB_STG_CDD_ENVIRONMENT_applications ON dbo.TB_STG_CDD_RELEASE_applications.id = dbo.TB_STG_CDD_ENVIRONMENT_applications.Id "+
                                  "INNER JOIN  dbo.TB_STG_CDD_ENVIRONMENT_data ON dbo.TB_STG_CDD_ENVIRONMENT_applications.data_Id = dbo.TB_STG_CDD_ENVIRONMENT_data.data_Id "+
                                  "WHERE dbo.TB_STG_CDD_ENVIRONMENT_data.name = 'PROD' "+
                                  "Group by dbo.TB_STG_CDD_RELEASE_data.id" ;

            string json = string.Empty;
            // StringBuilder json;

            int pagesize = 100;

            string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
            string database = ConfigurationManager.AppSettings["database"].ToString();
            string connString = Helpers.GetConnectionString(dbserver, database);
            DataTable dtLinks = new DataTable();

            if (level == 1)
            {
                //string query = "select id from TB_STG_CDD_RELEASE_data";
                //string query = "select releaseId from TB_STG_CDD_RELEASE_executionData where status <> 'DONE'";
                SqlConnection conn = new SqlConnection(connString);
                SqlCommand cmd = new SqlCommand(SQLReleaseID, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(dtLinks);
                conn.Close();
                da.Dispose();
                SQLReleaseID = string.Empty;
            }
            else
            {
                string query = "select releaseId, id from TB_STG_CDD_PHASES_data Where className = 'PhaseDto'";
                SqlConnection conn = new SqlConnection(connString);
                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(dtLinks);
                conn.Close();
                da.Dispose();
                query = string.Empty;
            }
            try
            {
                string url = urlbase;
                Initialize_Dataset(dataset, ConfigTable, table);
                foreach (DataRow rw in dtLinks.Rows)
                {


                    url = url + rw["id"].ToString() + "/" + table; // + "?page_size=" + pagesize;

                    //string json_count = Helpers.WebRequestWithCredentials(url, credentials);
                    string json_count = Helpers.WebRequestWithToken(httpclient, url);
                    string[] tokens = json_count.Split(',');
                    double nbIteration = 0;
                    int TotalResultCount = 0;
                    pagesize = 100;
                    int index = Array.FindIndex(tokens, m => m.Contains("totalResultsCount"));
                    TotalResultCount = Convert.ToInt32(Regex.Match(tokens[index], @"\d+").Value);
                    nbIteration = Math.Ceiling((double)TotalResultCount / pagesize);

                    string initialurl = url + "?page_number=";

                    for (int i = 0; i < nbIteration; i++)
                    {
                        url = initialurl + i + "&page_size=" + pagesize; 
                        //json = Helpers.WebRequestWithCredentials(url, credentials);
                        json = Helpers.WebRequestWithToken(httpclient, url);
                        string jsonempty = "{\"data\":[]}";

                        try
                        {
                            if (json.TrimStart().StartsWith("<") == false)
                            {
                                if (!json.Contains(jsonempty) && json != "NotFound")
                                {
                                    XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json, "root");
                                    json = string.Empty;

                                    foreach (DataTable dataTable in dataset.Tables)
                                        dataTable.BeginLoadData();

                                    dataset.ReadXml(new XmlTextReader(new StringReader(doc.OuterXml)));

                                    foreach (DataTable dataTable in dataset.Tables)
                                        dataTable.EndLoadData();

                                }

                            }
                        }
                        catch (Exception e)
                        {
                            
                            logger.Error(e, "An error occurred in LoadDatasetActivities - XML: ");
                            ErrorReporting = ErrorReporting + e + '\n' + '\n' + "***** Related Json *****" + '\n' + '\n' + json;
                            //Email.SendMail(Cdd.Emailadr, "REPORTED ERROR in  @ load Datasets - DeserializeXmlNode -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ErrorReporting);
                            Console.WriteLine(ErrorReporting);

                        }
                    }
                    url = urlbase;
                    // Bulkinsert SQL Tables
                    if (json != "NotFound") // skip those are not found
                    {
                        Bulkinsertdynamic(dataset, ConfigTable, dbserver, database, table.ToString());
                        dataset.Clear();
                    }
                }


                // display columns names of each datatable
                /*
                foreach (DataTable dataTable in dataset.Tables)
                {
                    Console.WriteLine("TABLE : {0}", dataTable.TableName.ToString());
                    foreach (DataColumn dtcol in dataTable.Columns)
                    {
                        Console.WriteLine("Column : {0}", dtcol.ColumnName.ToString());
                    }
                    Console.WriteLine("*************************************************************************************");
                }
                */
                //Console.WriteLine("table {0} is loaded - nb rows {1}", table, TotalResultCount);
                ExecutionSteps = ExecutionSteps + "table " + table + " is loaded - nb rows : " + '\n';
            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred in LoadDataset: '{0}'", e);

                ErrorReporting = ErrorReporting + e;
                //Email.SendMail(Cdd.Emailadr, "REPORTED ERROR in  @ LoadDataset -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ErrorReporting);
                Helpers.Loginfo(ErrorReporting, "ERROR");
                Console.WriteLine(ErrorReporting);
            }
            finally
            {

            }

        }


        // capture links between track milestones and phases as well production stages

        static void TrackLinks()
        {
            string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
            string database = ConfigurationManager.AppSettings["database"].ToString();

            // Get all links for tracks with their respective releases
            DataTable dt_json = new DataTable();
            DataColumn NewCol;

            NewCol = new DataColumn
            {
                ColumnName = "trackid",
                DataType = System.Type.GetType("System.String")
            };
            dt_json.Columns.Add(NewCol);

            NewCol = new DataColumn
            {
                ColumnName = "releaseid",
                DataType = System.Type.GetType("System.String")
            };
            dt_json.Columns.Add(NewCol);

            NewCol = new DataColumn
            {
                ColumnName = "json",
                DataType = System.Type.GetType("System.String")
            };
            dt_json.Columns.Add(NewCol);

            string query = string.Empty;
            string connString = Helpers.GetConnectionString(dbserver, database);
            SqlConnection conn = new SqlConnection(connString);


            using (SqlConnection connection = new SqlConnection(connString))
            {
                query = " select t.id as trackid, r.id as releaseid from [DMAS].[dbo].[TB_STG_CDD_TRACKS_data] t inner join[dbo].[TB_STG_CDD_TRACKS_releases] r on (t.data_Id = r.data_Id)";

                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(dt_json);
                conn.Close();
                da.Dispose();
            }

            string url = string.Empty;

            try
            {
                // Store Jsons
                foreach (DataRow row in dt_json.Rows)
                {
                    url = ServerUrl + "design/0000/v1/tracks/" + row["trackid"] + "/releases/" + row["releaseid"];
                    
                    row["json"] = Helpers.WebRequestWithToken(httpclient, url);
                    

                }

                // Bulkinsert SQL Table
                connString = Helpers.GetConnectionString(dbserver, database);
                conn = new SqlConnection(connString);
                using (SqlConnection connection = new SqlConnection(connString))
                {
                    conn.Open();
                    SqlBulkCopy bulkcopy = new SqlBulkCopy(conn)
                    {
                        DestinationTableName = "[DMAS].[dbo].[TB_STG_CDD_TRACKS_json]"
                    };
                    bulkcopy.WriteToServer(dt_json);
                    //dt_json.Clear();
                    conn.Close();
                }

                

                foreach (DataRow row in dt_json.Rows)
                {
                  
                    Helpers.CreateTrackLinks(row["trackid"].ToString(), row["releaseid"].ToString());

                }

            }
            catch (Exception e)
            {
                logger.Error(e, "An error occurred in load links :");
                Console.WriteLine(ErrorReporting);
            }
            finally
            {

            }

        }

  

        // LoadDataset is used to convert json in xml then load it into datasets
        // because CAAGILE has limitation to bring 2000 records at once, a loop is managed to get all needed records

        static void LoadDataset(DataSet dataset, DataTable ConfigTable, string table, string table_count)
        {
            string url_count = ServerUrl + Helpers.ReadConfiguration(ConfigTable, "url", table);
            string urlbase = ServerUrl + Helpers.ReadConfiguration(ConfigTable, "url", table);
            string credentials = Helpers.ReadConfiguration(ConfigTable, "url", "credentials");
            string json = string.Empty;
            // StringBuilder json;
            int pagesize = 0;

            //string json_count = Helpers.WebRequestWithCredentials(url_count, credentials);
            string json_count = Helpers.WebRequestWithToken(httpclient, url_count);
            string[] tokens = json_count.Split(',');

            int tokenIndex = Array.FindIndex(tokens, x => x.Contains("totalResultsCount"));
            double nbIteration = 0;
            int TotalResultCount = 0;
            if (table != "environment")
            {
                pagesize = 100;
                TotalResultCount = Convert.ToInt32(Regex.Match(tokens[tokenIndex], @"\d+").Value);
                nbIteration = Math.Ceiling((double)TotalResultCount / pagesize);

            }
            else
            {
                nbIteration = 1;
                pagesize = 100;
            }

            //int start = 1;
            //maximum can be loaded via API 2.0
            string url = urlbase;

            try
            {
                for (int i = 0; i < nbIteration; i++)
                {
                    url = url + "?page_size=" + pagesize + "&page_number=" + i;
                    //url = "https://cdd.res.sys.shared.fortis:8443/cdd/design/0000/v1/releases/1175865/applications/382601/application-versions";
                    //url = "https://cdd.res.sys.shared.fortis:8443/cdd/design/0000/v1/tracks/6452412/releases";

                    //json = Helpers.WebRequestWithCredentials(url, credentials);

                    //Console.WriteLine("Length Json handled {0} : ", json.Length);

                    json = Helpers.WebRequestWithToken(httpclient, url);
                    if (json == "NotFound")
                    {
                        url = urlbase;
                        continue;
                    }


                    //json = "{  \"data\": {\"track\": {  \"name\": \"EBW22M03\",  \"id\": 6452412,  \"className\": \"NamedIdentifiableDto\"},\"releaseVersion\": \"22M03\",\"productionStagePhasesMap\": {  \"6452413\": {\"name\": \"PILOT Deploy\",\"id\": 6452353,\"className\": \"NamedIdentifiableDto\"  },  \"6452591\": {\"name\": \"PILOT validation\",\"id\": 6474905,\"className\": \"NamedIdentifiableDto\"  },  \"6471222\": {\"name\": \"PROD BS Deploy\",\"id\": 6445921,\"className\": \"NamedIdentifiableDto\"  },  \"6471223\": {\"name\": \"PROD BS validation\",\"id\": 6475128,\"className\": \"NamedIdentifiableDto\"  },  \"6474841\": {\"name\": \"PROD BN Deploy\",\"id\": 6445930,\"className\": \"NamedIdentifiableDto\"  },  \"6474992\": {\"name\": \"PROD BN validation\",\"id\": 6475152,\"className\": \"NamedIdentifiableDto\"  },  \"6475058\": {\"name\": \"QAM1 deploy\",\"id\": 6445939,\"className\": \"NamedIdentifiableDto\"  },  \"6475189\": {\"name\": \"QAM1 validation\",\"id\": 6475158,\"className\": \"NamedIdentifiableDto\"  }},\"milestonePhasesMap\": {  \"6474424\": {\"name\": \"QA0 deploy\",\"id\": 6445918,\"className\": \"NamedIdentifiableDto\"  },  \"6474640\": {\"name\": \"NRT QA0\",\"id\": 6474495,\"className\": \"NamedIdentifiableDto\"  }},\"productionStageExecutionData\": {  \"6452413\": {\"allowedStatuses\": [],\"releaseId\": 6445604,\"status\": \"NOT_APPROVED\",\"id\": 6452413,\"className\": \"TrackProductionPhaseExecutionDto\"  },  \"6452591\": {\"allowedStatuses\": [],\"releaseId\": 6445604,\"status\": \"NOT_APPROVED\",\"id\": 6452591,\"className\": \"TrackProductionPhaseExecutionDto\"  },  \"6471222\": {\"allowedStatuses\": [],\"releaseId\": 6445604,\"status\": \"NOT_APPROVED\",\"id\": 6471222,\"className\": \"TrackProductionPhaseExecutionDto\"  },  \"6471223\": {\"allowedStatuses\": [],\"releaseId\": 6445604,\"status\": \"NOT_APPROVED\",\"id\": 6471223,\"className\": \"TrackProductionPhaseExecutionDto\"  },  \"6474841\": {\"allowedStatuses\": [],\"releaseId\": 6445604,\"status\": \"NOT_APPROVED\",\"id\": 6474841,\"className\": \"TrackProductionPhaseExecutionDto\"  },  \"6474992\": {\"allowedStatuses\": [],\"releaseId\": 6445604,\"status\": \"NOT_APPROVED\",\"id\": 6474992,\"className\": \"TrackProductionPhaseExecutionDto\"  },  \"6475058\": {\"allowedStatuses\": [],\"releaseId\": 6445604,\"status\": \"NOT_APPROVED\",\"id\": 6475058,\"className\": \"TrackProductionPhaseExecutionDto\"  },  \"6475189\": {\"allowedStatuses\": [],\"releaseId\": 6445604,\"status\": \"NOT_APPROVED\",\"id\": 6475189,\"className\": \"TrackProductionPhaseExecutionDto\"  }},\"milestoneStatusMap\": {  \"6474424\": \"PENDING_APPROVAL\",  \"6474640\": \"PENDING_APPROVAL\"},\"project\": {  \"name\": \"Base\",  \"id\": 2714408,  \"className\": \"NamedIdentifiableDto\"},\"phaseStatusMap\": {  \"6474424\": \"DESIGN\",  \"6474640\": \"DESIGN\"},\"name\": \"EBW22M03EBWS\",\"id\": 6445604,\"className\": \"ReleaseInTrackDto\"  }}";
                    try
                    {
                        if (json.TrimStart().StartsWith("<") == false)
                        {
                            XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json, "root");
                            json = string.Empty;

                            foreach (DataTable dataTable in dataset.Tables)
                                dataTable.BeginLoadData();

                            dataset.ReadXml(new XmlTextReader(new StringReader(doc.OuterXml)));

                            foreach (DataTable dataTable in dataset.Tables)
                                dataTable.EndLoadData();

                            // Insert Track chid table to release when it doesn't exist

                            if (table == "release")
                            {
                                if (!dataset.Tables.Contains("track"))
                                {
                                    DataTable track = new DataTable
                                    {
                                        TableName = "track"
                                    };
                                    DataColumn NewCol1 = new DataColumn
                                    {
                                        ColumnName = "name",
                                        DataType = System.Type.GetType("System.String")
                                    };
                                    track.Columns.Add(NewCol1);

                                    DataColumn NewCol2 = new DataColumn
                                    {
                                        ColumnName = "id",
                                        DataType = System.Type.GetType("System.String")
                                    };
                                    track.Columns.Add(NewCol2);

                                    DataColumn NewCol3 = new DataColumn
                                    {
                                        ColumnName = "className",
                                        DataType = System.Type.GetType("System.String")
                                    };
                                    track.Columns.Add(NewCol3);

                                    DataColumn NewCol4 = new DataColumn
                                    {
                                        ColumnName = "data_Id",
                                        DataType = System.Type.GetType("System.String")
                                    };
                                    track.Columns.Add(NewCol4);


                                    dataset.Tables.Add(track);
                                }
                                //----------------------------------------------------------
                            }


                            url = urlbase;
                            //start = start + pagesize;
                        }
                    }
                    catch (Exception e)
                    {
                        
                        ErrorReporting = "TABLE : " + table + '\n' + "ErrorReporting : " + e + '\n' + " ***** Related Json *****" + '\n' + '\n' + json;
                        logger.Error(e, "An error occurred in load Datasets - DeserializeXmlNode in table {0}:", table);
                        //Email.SendMail(Cdd.Emailadr, "REPORTED ERROR in  @ load Datasets - DeserializeXmlNode -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ErrorReporting);
                        Console.WriteLine(ErrorReporting);
                        url = urlbase;

                    }
                }


                // display columns names of each datatable
                /*
                foreach (DataTable dataTable in dataset.Tables)
                {
                    Console.WriteLine("TABLE : {0}", dataTable.TableName.ToString());
                    foreach (DataColumn dtcol in dataTable.Columns)
                    {
                        Console.WriteLine("Column : {0}", dtcol.ColumnName.ToString());
                    }
                    Console.WriteLine("*************************************************************************************");
                }
                */
                //Console.WriteLine("table {0} is loaded - nb rows {1}", table, TotalResultCount);
                ExecutionSteps = ExecutionSteps + "table " + table + " is loaded - nb rows : " + TotalResultCount + '\n';
            }
            catch (Exception e)
            {
                logger.Error(e, "An error occurred in load Datasets in table {0} :", table);
                ErrorReporting = ErrorReporting + e;
                //Email.SendMail(Cdd.Emailadr, "REPORTED ERROR in  @ LoadDataset -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ErrorReporting);
                Console.WriteLine(ErrorReporting);
            }
            finally
            {

            }

        }

        static void Bulkinsertdynamic(DataSet dataset, DataTable datatable, string dbserver, string database, string rootnode)
        {
            string[] listtable = Helpers.ReadListConfiguration(datatable, "Bulkinsert", rootnode);
            string[] listmappedcolumn = Helpers.ReadListConfiguration(datatable, "mapping", rootnode);

            string connString = Helpers.GetConnectionString(dbserver, database);
            using (SqlConnection connection = new SqlConnection(connString))
            {
                connection.Open();
                SqlBulkCopy bulkcopy = new SqlBulkCopy(connString)
                {
                    BulkCopyTimeout = 0
                };
                int i = 0;

                try
                {
                    DataTable inputDataTableMapping = new DataTable();
                    DataTable ResultDataTableMapping = new DataTable();
                    for (int count = 0; count <= dataset.Tables.Count - 1; count++)
                    {

                        if (listtable.Contains(dataset.Tables[count].TableName.ToString()))
                        {
                            inputDataTableMapping = dataset.Tables[count];
                            //if (i == 0)
                            //{
                            //    ResultDataTableMapping = Helpers.CompareRows(Helpers.GetSQLTable(dbserver, database, "TB_STG_CDD_" + rootnode.ToUpper()), Helpers.GetAGTable(inputDataTableMapping), inputDataTableMapping, listmappedcolumn);
                            //    bulkcopy.DestinationTableName = "[DMAS].[dbo].[TB_STG_CDD_" + rootnode.ToUpper() + "]";
                            //}
                            //else
                            //{
                            ResultDataTableMapping = Helpers.CompareRows(Helpers.GetSQLTable(dbserver, database, "TB_STG_CDD_" + rootnode.ToUpper() + "_" + inputDataTableMapping), Helpers.GetAGTable(inputDataTableMapping), inputDataTableMapping, listmappedcolumn);
                            bulkcopy.DestinationTableName = "[DMAS].[dbo].[TB_STG_CDD_" + rootnode.ToUpper() + "_" + inputDataTableMapping.TableName.ToString() + "]";
                            //}

                            bulkcopy.ColumnMappings.Clear();

                            int length = ResultDataTableMapping.Columns.Count;

                            for (int k = length - 1; k >= 0; k--)
                            {
                                //Console.WriteLine("Column : {0} -- {1}", ResultDataTableMapping.Columns[k].ColumnName, ResultDataTableMapping.Columns[k].DataType);
                                bulkcopy.ColumnMappings.Add(ResultDataTableMapping.Columns[k].ColumnName.Trim(), ResultDataTableMapping.Columns[k].ColumnName.Trim());
                            }

                            Console.WriteLine("table {0} : {1} rows were loaded into SQL Server", ResultDataTableMapping.TableName, ResultDataTableMapping.Rows.Count);
                            Console.WriteLine("*******************************************************************************************************");
                            //ExecutionSteps = ExecutionSteps + "table " + ResultDataTableMapping.TableName + " is loaded into SQL Server - nb rows : " + ResultDataTableMapping.Rows.Count + '\n';

                            bulkcopy.WriteToServer(ResultDataTableMapping);

                            i++;
                        }
                    }
                }
                catch (Exception e)
                {
                    
                    ErrorReporting = ErrorReporting + e;
                    logger.Error(e, "An error occurred in Bulkinsert in TABLE {0}:", rootnode);
                    //Email.SendMail(Cdd.Emailadr, "REPORTED ERROR in  @ Bulkinsert -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ErrorReporting);
                    Console.WriteLine(ErrorReporting);

                }
                bulkcopy.Close();
                connection.Close();
            }
        }

        static void LoadDataFromCdd(DataSet SQLTargetSet, string dbserver, string database, DataTable ConfigTable, string table)
        {
            //if (table == "release")
            //{
                Console.WriteLine("task load for {0} table has started", table.ToString());
                logger.Info("task load for {0} table has started", table.ToString());
                LoadDataset(SQLTargetSet, ConfigTable, table.ToString(), table.ToString() + "_count");
                Bulkinsertdynamic(SQLTargetSet, ConfigTable, dbserver, database, table.ToString());
                logger.Info("task load for {0} table has completed", table.ToString());
            //}


        }

        // Main program to trigger the required methods

        static void Main(string[] args)
        {
            //GetSchema();

            string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
            string database = ConfigurationManager.AppSettings["database"].ToString();
            string environment = ConfigurationManager.AppSettings["ENV"].ToString();
            string configtable = ConfigurationManager.AppSettings["configtable"].ToString();

            DataTable ConfigTable = Helpers.LoadConfiguration(dbserver, database, configtable);
            string[] listtabletotruncate = Helpers.ReadListConfiguration(ConfigTable, "dbstatement", "truncate");           
            string[] listtabletoload = Helpers.ReadListConfiguration(ConfigTable, "load", "load");
            //string credentials = Helpers.readconfiguration(ConfigTable, "url", "credentials");

            ExecutionSteps = string.Empty;
            ErrorReporting = string.Empty;
            StepbyStep = string.Empty;
            Emailadr = string.Empty;

            Emailadr = ConfigurationManager.AppSettings["Email"].ToString();
            ServerUrl = ConfigurationManager.AppSettings["ServerUrl"].ToString();
            Token = ConfigurationManager.AppSettings["Token"].ToString();


            httpclient = Helpers.WebAuthenticationWithToken();

            //hide console window: to show it again put "5" instead of "0"
            //IntPtr h = Process.GetCurrentProcess().MainWindowHandle;
            //ShowWindow(h, 0);

            // truncate all tables before the load
            SQLstatement(dbserver, database, listtabletotruncate, true);

            //for test
            //DataSet SQLTarget = new DataSet();
            //LoadDataFromCdd(SQLTarget, dbserver, database, ConfigTable, "release");


            // for test
            //DataSet SQLTarget = new DataSet();
            //LoadDataFromCdd(SQLTarget, dbserver, database, ConfigTable, "tracks");

            try
            {
                SQLstatement(dbserver, database, listtabletotruncate, true);

                logger.Info("Load CDD started");

                Task[] tasks = new Task[listtabletoload.Length];              //new Task[listtabletoload.Length];
                int i = 0;
                foreach (var table in listtabletoload)
                {
                    DataSet SQLTargetSet = new DataSet();
                    tasks[i] = Task.Factory.StartNew(() => LoadDataFromCdd(SQLTargetSet, dbserver, database, ConfigTable, table), TaskCreationOptions.LongRunning);
                    i++;

                }

                Task.WaitAll(tasks);

                // Process tracks-releases links

                TrackLinks();

                // Gather all related phases to releases

                DataSet SQLPhases = new DataSet();
                Console.WriteLine("task load for Phases table has started");
                logger.Info("Load for Phases table has started");
                //LoadDatasetfortest(SQLPhases, ConfigTable, "phases", "test");
                LoadSubDataset(SQLPhases, ConfigTable, "phases", 1);
                //Bulkinsertdynamic(SQLPhases, ConfigTable, dbserver, database, "phases");
                logger.Info("Load for Phases table has completed");

                //Gather all related Application-Versions to releases

                DataSet SQLAppVersions = new DataSet();
                logger.Info("Load for application-versions table has started");
                LoadSelectedAppVersion(SQLAppVersions, ConfigTable, "selected-application-versions", 1);
                Bulkinsertdynamic(SQLAppVersions, ConfigTable, dbserver, database, "selected-application-versions");
                SQLAppVersions.Clear();
                LoadSubDataset(SQLAppVersions, ConfigTable, "application-versions", 1);
                //Bulkinsertdynamic(SQLAppVersions, ConfigTable, dbserver, database, "application-versions");
                logger.Info("Load for application-versions table has completed");


                // Gather all Work Items (aka US and FE)

                //DataSet SQLWorkItems = new DataSet();
                //logger.Info("Load for workitems table has started");
                //LoadWorkItems(SQLWorkItems, ConfigTable);
                //Bulkinsertdynamic(SQLWorkItems, ConfigTable, dbserver, database, "workitems");
                //logger.Info("Load for workitems table has completed");


                // Gather all related activities to releases

                //DataSet SQLActivities = new DataSet();
                //logger.Info("Load for activities has started");
                //LoadSubDatasetactivities(SQLActivities, ConfigTable, "activities", 1);
                ////Bulkinsertdynamic(SQLActivities, ConfigTable, dbserver, database, "activities");
                //logger.Info("Load for activities table has completed");


                // Gather all related tasks to phases per Release

                DataSet SQLTasks = new DataSet();
                logger.Info("Load for tasks table has started");
                LoadSubDataset(SQLTasks, ConfigTable, "tasks", 2);
                //Bulkinsertdynamic(SQLTasks, ConfigTable, dbserver, database, "tasks");
                logger.Info("Load for tasks table has completed");

                logger.Info("Load for CDD completed");
            }
            catch (Exception ex)
            {
                logger.Error(ex, "An error has occured : ");
                return;
            }
            //Console.ReadKey();
        }

        // Selected AppVersion(s) for a release
        static void LoadSelectedAppVersion(DataSet dataset, DataTable ConfigTable, string table, int level)
        {

            string urlbase = ServerUrl + "design/0000/v1/releases/";
            //string urlbase = "https://cdd-qa.resnp.sysnp.shared.fortis:8443/cdd/design/0000/v1/releases/";
            //string urlbase = "https://spal005m:8443/cdd/design/0000/v1/releases/";


            string credentials = Helpers.ReadConfiguration(ConfigTable, "url", "credentials");
            string json = string.Empty;
            // StringBuilder json;

            int pagesize = 100;

            string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
            string database = ConfigurationManager.AppSettings["database"].ToString();
            string connString = Helpers.GetConnectionString(dbserver, database);
            DataTable dtLinks = new DataTable();
           
            string query = "SELECT dbo.TB_STG_CDD_RELEASE_data.data_Id, dbo.TB_STG_CDD_RELEASE_data.id AS ReleaseId, dbo.TB_STG_CDD_RELEASE_applications.id AS ApplicationId " +
                    "FROM dbo.TB_STG_CDD_RELEASE_data INNER JOIN " +
                    "dbo.TB_STG_CDD_RELEASE_applications ON dbo.TB_STG_CDD_RELEASE_data.data_Id = dbo.TB_STG_CDD_RELEASE_applications.data_Id";
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(query, conn);
            conn.Open();
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            da.Fill(dtLinks);
            conn.Close();
            da.Dispose();
            query = string.Empty;
            
            try
            {
                string url = urlbase;
                int current_position_in_dataset = 0;
                //Initialize_Dataset(dataset, ConfigTable, table);

                foreach (DataRow rw in dtLinks.Rows)
                {                    
                    url = url + rw["ReleaseId"].ToString() + "/applications/" + rw["ApplicationId"].ToString() + "/application-versions";

                    json = Helpers.WebRequestWithToken(httpclient, url);
                    string jsonempty = "{\"data\":[]}";

                    try
                    {
                        if (!String.IsNullOrEmpty(json))
                        {

                            if (json.TrimStart().StartsWith("<") == false)
                            {
                                if (!json.Contains(jsonempty) && json != "NotFound")
                                {
                                    XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json, "root");

                                    json = string.Empty;

                                    foreach (DataTable dataTable in dataset.Tables)
                                        dataTable.BeginLoadData();

                                    dataset.ReadXml(new XmlTextReader(new StringReader(doc.OuterXml)));

                                    foreach (DataTable dataTable in dataset.Tables)
                                        dataTable.EndLoadData();

                                    // fill releaseid and applicationid

                                    if (!dataset.Tables["data"].Columns.Contains("releaseid") && !dataset.Tables["data"].Columns.Contains("applicationid"))
                                    {
                                        dataset.Tables["data"].Columns.Add("releaseid");
                                        dataset.Tables["data"].Columns.Add("applicationid");
                                    }

                                    for (int i = current_position_in_dataset; i < dataset.Tables["data"].Rows.Count; i++)
                                    {
                                        dataset.Tables["data"].Rows[i]["releaseid"] = rw["ReleaseId"];
                                        dataset.Tables["data"].Rows[i]["applicationid"] = rw["applicationId"];
                                        current_position_in_dataset++;

                                        //Console.WriteLine("Next CurrentPosition : {0}", current_position_in_dataset);
                                    }

                                   
                                }
                                
                                //start = start + pagesize;
                            }
                        }

                        url = urlbase;
                    }
                    catch (Exception e)
                    {

                        logger.Error(e, "An error occurred in load Selected App Version - DeserializeXmlNode :");
                        ErrorReporting = ErrorReporting + e + '\n' + '\n' + "***** Related Json *****" + '\n' + '\n' + json;
                        //Email.SendMail(Cdd.Emailadr, "REPORTED ERROR in  @ load Datasets - DeserializeXmlNode -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ErrorReporting);
                        Console.WriteLine(ErrorReporting);
                    }

                    // Bulkinsert SQL Tables
                    //Bulkinsertdynamic(dataset, ConfigTable, dbserver, database, table.ToString());
                    //dataset.Clear();

                }

                // display columns names of each datatable
                /*
                foreach (DataTable dataTable in dataset.Tables)
                {
                    Console.WriteLine("TABLE : {0}", dataTable.TableName.ToString());
                    foreach (DataColumn dtcol in dataTable.Columns)
                    {
                        Console.WriteLine("Column : {0}", dtcol.ColumnName.ToString());
                    }
                    Console.WriteLine("*************************************************************************************");
                }
                */
                //Console.WriteLine("table {0} is loaded - nb rows {1}", table, TotalResultCount);
                ExecutionSteps = ExecutionSteps + "table " + table + " is loaded - nb rows : " + '\n';
            }
            catch (Exception e)
            {

                logger.Error(e, "An error occurred in SubLoadDataset: ");
                ErrorReporting = ErrorReporting + e;
                //Email.SendMail(Cdd.Emailadr, "REPORTED ERROR in  @ LoadDataset -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ErrorReporting);
                Console.WriteLine(ErrorReporting);

            }
            finally
            {

            }
        }
    }
}
