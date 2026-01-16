using System;
using System.Diagnostics;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Runtime.InteropServices;
using NLog;

namespace cdd
{

    class CddSorted
    {
        // this is used to hide (make the console window application in silent mode) console window
        [DllImport("user32.dll")]
        static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);

        public static string Emailadr { get; set; }
        public static string ExecutionSteps { get; set; }
        public static Logger logger = LogManager.GetCurrentClassLogger();

        static void ExecuteODS(string dbserver, string database)
        {

            var connectionString = cdd.Helpers.GetConnectionString(dbserver, database);
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                SqlCommand cmd = new SqlCommand();

                cmd.Connection = connection;

                try
                {

                    cmd.CommandText = "dbo.SP_TF_ITRPT_CFGSQL_V2";
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 0;
                    cmd.Parameters.Clear();
                    cmd.Parameters.Add(new SqlParameter("@GRP", "CDD"));
                    SqlParameter returnedvalue = new SqlParameter("Returns integer", SqlDbType.Int);
                    returnedvalue.Direction = ParameterDirection.ReturnValue;
                    cmd.Parameters.Add(returnedvalue);
                    cmd.ExecuteNonQuery();

                    // return the number of rows inserted into ODS tables

                    cmd.CommandText = "dbo.SP_TF_ITRPT_CFGSQL_V2";
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 0;
                    cmd.Parameters.Clear();
                    cmd.Parameters.Add(new SqlParameter("@GRP", "CDD_COUNT"));
                    SqlParameter returnedvalue_count = new SqlParameter("Returns integer", SqlDbType.Int);
                    returnedvalue_count.Direction = ParameterDirection.ReturnValue;
                    cmd.Parameters.Add(returnedvalue);
                    cmd.ExecuteNonQuery();

                    DataTable odstable = new DataTable();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(odstable);
                    da.Dispose();

                    logger.Info("load into SQL Server ODS has been completed");

                    logger.Info("=> nb rows loaded into ACTIVITIES ODS table : {0} ", odstable.Rows[0]["activities"].ToString());
                    logger.Info("=> nb rows loaded into APPLICATION VERSIONS ODS table : {0} ", odstable.Rows[0]["appversions"].ToString());
                    logger.Info("=> nb rows loaded into RELEASES ODS table : {0}", odstable.Rows[0]["releases"].ToString());
                    logger.Info("=> nb rows loaded into PHASES ODS table : {0}", odstable.Rows[0]["phases"].ToString());
                    logger.Info("=> nb rows loaded into TASKS ODS table : {0}", odstable.Rows[0]["tasks"].ToString());
                    logger.Info("=> nb rows loaded into TRACKS ODS table : {0}", odstable.Rows[0]["tracks"].ToString());
                    

                    // email prepapration

                    ExecutionSteps = ExecutionSteps + "=> nb rows loaded into ACTIVITIES ODS table : " + odstable.Rows[0]["activities"].ToString() + '\n';
                    ExecutionSteps = ExecutionSteps + "=> nb rows loaded into APPLICATION VERSIONS ODS table : " + odstable.Rows[0]["appversions"].ToString() + '\n';
                    ExecutionSteps = ExecutionSteps + "=> nb rows loaded into RELEASES ODS table : " + odstable.Rows[0]["releases"].ToString() + '\n';
                    ExecutionSteps = ExecutionSteps + "=> nb rows loaded into PHASES ODS table : " + odstable.Rows[0]["phases"].ToString() + '\n';
                    ExecutionSteps = ExecutionSteps + "=> nb rows loaded into TASKS ODS table : " + odstable.Rows[0]["tasks"].ToString() + '\n';
                    ExecutionSteps = ExecutionSteps + "=> nb rows loaded into TRACKS ODS table : " + odstable.Rows[0]["tracks"].ToString() + '\n';
                   


                    if (returnedvalue.Value.ToString() == "1")
                    {
                        logger.Error("Error happened when inserting data into ODS.Please to check TB_APP_ITRPT_CFGSQLlog table to get more details.");
                        System.Environment.Exit(0);
                    }
                }
                catch (Exception e)
                {
                    //Console.WriteLine("An error occurred in SQLstatement: '{0}'", e);
                    logger.Error(e, "An error occurred in SQLstatement: '{0}'");
                    //ErrorReporting = ErrorReporting + e;
                    //email.SendMail(emailadr, "REPORTED ERROR in  @ SQLstatement -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ErrorReporting);
                    //Octane.Helpers.loginfo(ErrorReporting, "ERROR");
                }

                connection.Close();
            }
        }



        // SQLStatement takes a list of tables and apply SQL statement as direct input or via Stored procedure
        static void InitializeTaskSortedTable(string dbserver, string database)
        {

            string SQL = ConfigurationManager.AppSettings["SQL"].ToString();

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
                        cmd.CommandText = SQL;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandTimeout = 0;
                        cmd.Parameters.Clear();
                        //cmd.Parameters.Add(new SqlParameter("@Tablename", "[DMAS].[dbo].[TB_STG_CddSorted_" + t.ToString() + "]"));
                        cmd.ExecuteNonQuery();                    
                }
                catch (Exception e)
                {
                    Console.WriteLine("An error occurred in SQL SORTED TASKS: '{0}'", e);

                }

                connection.Close();
            }
        }


        static void InitializePhaseSortedTable(string dbserver, string database)
        {

            string SQL = ConfigurationManager.AppSettings["SQLPHASES"].ToString();

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
                    cmd.CommandText = SQL;
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandTimeout = 0;
                    cmd.Parameters.Clear();
                    //cmd.Parameters.Add(new SqlParameter("@Tablename", "[DMAS].[dbo].[TB_STG_CddSorted_" + t.ToString() + "]"));
                    cmd.ExecuteNonQuery();
                }
                catch (Exception e)
                {
                    Console.WriteLine("An error occurred in SQL SORTED PHASES: '{0}'", e);

                }

                connection.Close();
            }
        }


        static DataTable TaskSorted(string dbserver, string database, string connString)
        {

            DataTable dtSorted = new DataTable();

            InitializeTaskSortedTable(dbserver, database);

            string query = "select * FROM  dbo.TB_STG_CDD_TASKS_data_ordered ORDER BY phaseId,idprev";
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(query, conn);
            conn.Open();
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            da.Fill(dtSorted);
            conn.Close();
            da.Dispose();
            query = string.Empty;
            
            DataTable ResultSorted = dtSorted.Clone();
            DataTable CurrentdtSorted = dtSorted.Clone();
            string previous_phaseID = dtSorted.Rows[0]["phaseId"].ToString(); 

            foreach (DataRow rw in dtSorted.Rows)
            {
                
                string current_phaseID = rw["phaseId"].ToString();

                  

                    if (previous_phaseID == current_phaseID)
                    {
                        CurrentdtSorted.NewRow();
                        CurrentdtSorted.ImportRow(rw);

                    }
                    else
                    {
                        if (previous_phaseID == "2008967")
                        {
                        Console.WriteLine("Here we are");
                        }
                        LoopOnCurrentDtSortedTask(CurrentdtSorted);
                        ResultSorted.Merge(CurrentdtSorted);
                        CurrentdtSorted.Clear();
                        CurrentdtSorted.NewRow();
                        CurrentdtSorted.ImportRow(rw);
                    }
                    previous_phaseID = current_phaseID;
                  
            }

            return ResultSorted;
        }

        static DataTable PhaseSorted(string dbserver, string database, string connString)
        {

            DataTable dtSorted = new DataTable();

            InitializePhaseSortedTable(dbserver, database);

            string query = "select * FROM  dbo.TB_STG_CDD_PHASES_data_ordered ORDER BY releaseId,idprev";
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(query, conn);
            conn.Open();
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            da.Fill(dtSorted);
            conn.Close();
            da.Dispose();
            query = string.Empty;

            DataTable ResultSorted = dtSorted.Clone();
            DataTable CurrentdtSorted = dtSorted.Clone();
            string previous_releaseID = dtSorted.Rows[0]["releaseId"].ToString();

            foreach (DataRow rw in dtSorted.Rows)
            {

                string current_releaseID = rw["releaseId"].ToString();



                if (previous_releaseID == current_releaseID)
                {
                    CurrentdtSorted.NewRow();
                    CurrentdtSorted.ImportRow(rw);

                }
                else
                {
                    LoopOnCurrentDtSortedPhase(CurrentdtSorted);
                    ResultSorted.Merge(CurrentdtSorted);
                    CurrentdtSorted.Clear();
                    CurrentdtSorted.NewRow();
                    CurrentdtSorted.ImportRow(rw);
                }
                previous_releaseID = current_releaseID;

            }

            return ResultSorted;
        }

    static void LoopOnCurrentDtSortedPhase(DataTable dt)
    {
      int SortedCounter = 1;
      int CurrentNextId = 0;
      int RowIndex = 0;
            try
            {

                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    try
                    {
                        
                            if (DBNull.Value.Equals(dt.Rows[RowIndex]["idprev"]))
                            {
                                dt.Rows[i]["sortedid"] = 1;
                            }

                            try
                            {
                                if (!DBNull.Value.Equals(dt.Rows[RowIndex]["idnext"]))
                                {
                                    Console.WriteLine("RowIndex = {0}", RowIndex);
                                    CurrentNextId = Convert.ToInt32(dt.Rows[RowIndex]["idnext"]);
                                    DataRow row = dt.Select("id = '" + CurrentNextId + "'").FirstOrDefault();
                                    RowIndex = dt.Rows.IndexOf(row);
                                    row["sortedid"] = ++SortedCounter;
                                }
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("An error occurred in second if!!!: " + e.Message);
                            }
                        
                    }
                    catch (Exception e)
                    {
                        DataTable cdt = dt.Copy();
                        Console.WriteLine("An error occurred in first loop!!!: " + e.Message);
                    }
                }

                // handle remaining sortedid = null

                for (int i = 0; i < dt.Rows.Count; i++)
                {

                    if (DBNull.Value.Equals(dt.Rows[i]["sortedid"]) && !DBNull.Value.Equals(dt.Rows[i]["idnext"]))
                    {
                        //dt.Rows[i]["sortedid"] = ReturnSortedIDofIdNext(dt, dt.Rows[i]["idnext"].ToString());
                        string idnextCuri = dt.Rows[i]["idnext"].ToString();
                        for (int j = 0; j < dt.Rows.Count; j++)
                        {
                            try
                            {
                                string sortedid = dt.Rows[j]["sortedid"].ToString();
                                string idnextCurj = dt.Rows[j]["idnext"].ToString();
                                if (idnextCurj.Equals(idnextCuri) && !DBNull.Value.Equals(sortedid))
                                {
                                    dt.Rows[i]["sortedid"] = dt.Rows[j]["sortedid"];
                                    //break;
                                }
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("An error occurred in second loop!!!: " + e.Message);
                            }
                        }

                    }

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred in LoopOnCurrentDtSorted " + e.Message);
            }
        }


        static void LoopOnCurrentDtSortedTask(DataTable dt)
        {
            int SortedCounter = 1;
            int CurrentNextId = 0;
            int RowIndex = 0;
            try
            {

                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    try
                    {

                        if (DBNull.Value.Equals(dt.Rows[RowIndex]["idprev"]))
                        {
                            dt.Rows[i]["sortedid"] = 1;
                            // Handle tasks that have same level (aka parallel tasks)
                            if (!DBNull.Value.Equals(dt.Rows[i]["idnext"]))
                            {
                                AssignTaskSameLevel(dt, Convert.ToInt32(dt.Rows[i]["idnext"]), Convert.ToInt32(dt.Rows[i]["sortedid"]));
                            }
                        }

                        try
                        {
                            if (!DBNull.Value.Equals(dt.Rows[RowIndex]["idnext"]))
                            {
                                Console.WriteLine("RowIndex = {0}", RowIndex);
                                CurrentNextId = Convert.ToInt32(dt.Rows[RowIndex]["idnext"]);

                                DataRow row = dt.Select("id = '" + CurrentNextId + "'").FirstOrDefault();
                                RowIndex = dt.Rows.IndexOf(row);
                                row["sortedid"] = ++SortedCounter;

                                // Handle tasks that have same level (aka parallel tasks)
                                if (!DBNull.Value.Equals(row["idnext"]))
                                { 
                                    AssignTaskSameLevel(dt, Convert.ToInt32(row["idnext"]), Convert.ToInt32(row["sortedid"]));
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("An error occurred in second if!!!: " + e.Message);
                        }

                    }
                    catch (Exception e)
                    {
                        DataTable cdt = dt.Copy();
                        Console.WriteLine("An error occurred in first loop!!!: " + e.Message);
                    }
                }

                // handle remaining sortedid = null

                //for (int i = 0; i < dt.Rows.Count; i++)
                //{

                //    if (DBNull.Value.Equals(dt.Rows[i]["sortedid"]) && !DBNull.Value.Equals(dt.Rows[i]["idnext"]))
                //    {
                //        //dt.Rows[i]["sortedid"] = ReturnSortedIDofIdNext(dt, dt.Rows[i]["idnext"].ToString());
                //        string idnextCuri = dt.Rows[i]["idnext"].ToString();
                //        for (int j = 0; j < dt.Rows.Count; j++)
                //        {
                //            try
                //            {
                //                string sortedid = dt.Rows[j]["sortedid"].ToString();
                //                string idnextCurj = dt.Rows[j]["idnext"].ToString();
                //                if (idnextCurj.Equals(idnextCuri) && !DBNull.Value.Equals(sortedid))
                //                {
                //                    dt.Rows[i]["sortedid"] = dt.Rows[j]["sortedid"];
                //                    //break;
                //                }
                //            }
                //            catch (Exception e)
                //            {
                //                Console.WriteLine("An error occurred in second loop!!!: " + e.Message);
                //            }
                //        }

                //    }

                //}
            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred in LoopOnCurrentDtSorted " + e.Message);
            }
        }

        static void AssignTaskSameLevel(DataTable dt, int CurrentNextId, int SortedCounter)
        {
            int CurrentNextId_dt = 0;
            try
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    if (!DBNull.Value.Equals(dt.Rows[i]["idnext"]))
                    {
                        CurrentNextId_dt = Convert.ToInt32(dt.Rows[i]["idnext"]);
                    }
                    
                    if (DBNull.Value.Equals(dt.Rows[i]["sortedid"]) && CurrentNextId_dt == CurrentNextId)
                    {
                        dt.Rows[i]["sortedid"] = SortedCounter;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred in AssignTaskSameLevel " + e.Message);
            }
        }

        static string ReturnSortedIDofIdNext(DataTable dt, string idnext)
        {
            try
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    string sortedid = dt.Rows[i]["sortedid"].ToString();
                    string idnextCur = dt.Rows[i]["idnext"].ToString();
                    if (idnextCur.Equals(idnext) && int.Parse(sortedid) > 0)
                    {
                        return dt.Rows[i]["sortedid"].ToString();
                    }
                }

                
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred in : ReturnSortedIDofIdNext function " + ex.Message);
            }

            return "0";
        }
        // Main program to trigger the required methods

        static void Main(string[] args)
        {


            string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
            string database = ConfigurationManager.AppSettings["database"].ToString();
            string environment = ConfigurationManager.AppSettings["ENV"].ToString();
            Emailadr = ConfigurationManager.AppSettings["Email"].ToString();

            string connString = Helpers.GetConnectionString(dbserver, database);

            // hide console window  : to show it again put "5" instead of "0"
            //IntPtr h = Process.GetCurrentProcess().MainWindowHandle;
            //ShowWindow(h, 0);


            ExecutionSteps = "---------------------------------------------------------" + '\n';
            ExecutionSteps = ExecutionSteps + "CDD Process SQL started @ " + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now) + " - Environment : " + environment + '\n';
            ExecutionSteps = ExecutionSteps + "---------------------------------------------------------" + '\n';

            try
            {
                DataTable TaskDtSorted = new DataTable();
                TaskDtSorted = TaskSorted(dbserver, database, connString);

                DataTable PhaseDtSorted = new DataTable();
                PhaseDtSorted = PhaseSorted(dbserver, database, connString);

                try
                {

                    using (SqlConnection connection = new SqlConnection(connString))
                    {
                        connection.Open();

                        SqlCommand cmd = new SqlCommand()
                        {
                            Connection = connection,
                            CommandText = "dbo.st_truncate_table",
                            CommandType = CommandType.StoredProcedure,
                            CommandTimeout = 0
                        };
                        cmd.Parameters.Clear();
                        cmd.Parameters.Add(new SqlParameter("@Tablename", "[DMAS].[dbo].[TB_STG_CDD_PHASES_data_ordered]"));
                        cmd.ExecuteNonQuery();


                        SqlBulkCopy bulkcopy = new SqlBulkCopy(connection)
                        {
                            DestinationTableName = "[DMAS].[dbo].[TB_STG_CDD_PHASES_data_ordered]"
                        };
                        bulkcopy.WriteToServer(PhaseDtSorted);
                        PhaseDtSorted.Clear();
                        connection.Close();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("An error occurred in bulkinsert PhaseSorted!!!: " + ex.Message);
                }


                try
                {

                    using (SqlConnection connection = new SqlConnection(connString))
                    {
                        connection.Open();

                        SqlCommand cmd = new SqlCommand()
                        {
                            Connection = connection,
                            CommandText = "dbo.st_truncate_table",
                            CommandType = CommandType.StoredProcedure,
                            CommandTimeout = 0
                        };
                        cmd.Parameters.Clear();
                        cmd.Parameters.Add(new SqlParameter("@Tablename", "[DMAS].[dbo].[TB_STG_CDD_TASKS_data_ordered]"));
                        cmd.ExecuteNonQuery();


                        SqlBulkCopy bulkcopy = new SqlBulkCopy(connection)
                        {
                            DestinationTableName = "[DMAS].[dbo].[TB_STG_CDD_TASKS_data_ordered]"
                        };
                        bulkcopy.WriteToServer(TaskDtSorted);
                        TaskDtSorted.Clear();
                        connection.Close();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("An error occurred in bulkinsert PhaseSorted!!!: " + ex.Message);
                }


            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred!!!: " + ex.Message);
                return;
            }



            ExecuteODS(dbserver, database);

            ExecutionSteps = ExecutionSteps + "---------------------------------------------------------" + '\n';
            ExecutionSteps = ExecutionSteps + "CDD Process SQL finished @ " + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now) + " - Environment : " + environment + '\n';
            ExecutionSteps = ExecutionSteps + "---------------------------------------------------------" + '\n';

            Console.WriteLine(ExecutionSteps);

            //Email.SendMail(Emailadr, "CDD processSQL - Post load Processing -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now) + " - Environment : " + environment, ExecutionSteps);


            //Console.WriteLine("Process finished you can hit the key");
            //Console.ReadKey();

        }

    }
}
