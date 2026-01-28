using System;
using System.Diagnostics;
using System.Configuration;



namespace cdd
{
        
    class Launcher
    {
        // public properties to store global data across program class 
        public static string ExecutionSteps { get; set; }
        public static string ErrorReporting { get; set; }
        public static string StepbyStep { get; set; }
        public static string emailadr { get; set; }


        // SQLStatement takes a list of tables and apply SQL statement as direct input or via Stored procedure
 

        // Main program to trigger the required methods

        static void Main(string[] args)
        {
            string environment = ConfigurationManager.AppSettings["ENV"].ToString();
            string prgload = ConfigurationManager.AppSettings["PRGLOAD"].ToString();
            string prgsql = ConfigurationManager.AppSettings["PRGSQL"].ToString();
            string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
            string database = ConfigurationManager.AppSettings["database"].ToString();
            string filewatcher = ConfigurationManager.AppSettings["FILEWATCHER"].ToString();

            ExecutionSteps = string.Empty;
            ErrorReporting = string.Empty;
            StepbyStep = string.Empty;
            emailadr = string.Empty;

            emailadr = ConfigurationManager.AppSettings["Email"].ToString();

            ExecutionSteps = "---------------------------------------------------------" + '\n';
            ExecutionSteps = ExecutionSteps + "CDD Launcher started @ " + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now)+ " - Environment : " + environment + '\n';
            ExecutionSteps = ExecutionSteps+ "---------------------------------------------------------" + '\n';

            try
            {
 
                //**** load 1 ****
                Process LoadProc = new Process();
                LoadProc.StartInfo.FileName = prgload;
                LoadProc.EnableRaisingEvents = true;

                LoadProc.Start();

                LoadProc.WaitForExit();
                ExecutionSteps = ExecutionSteps + "Load CDD stagging completed @ " + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now) + " - Environment : " + environment + '\n';

                //You may want to perform different actions depending on the exit code.
                Console.WriteLine("Load CDD stagging process exited: " + LoadProc.ExitCode);

                Process SqlProc = new Process();
                SqlProc.StartInfo.FileName = prgsql;
                SqlProc.Start();
                SqlProc.WaitForExit();
                ExecutionSteps = ExecutionSteps + "Load CDD ODS completed @ " + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now) + " - Environment : " + environment + '\n';

            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred!!!: " + ex.Message);
                return;
            }
            
            ExecutionSteps = ExecutionSteps + "---------------------------------------------------------" + '\n';
            ExecutionSteps = ExecutionSteps + "CDD Launcher finished @ " + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now) + " - Environment : " + environment +'\n';
            ExecutionSteps = ExecutionSteps + "---------------------------------------------------------" + '\n';
          
            //Email.SendMail(emailadr, "CDD Launcher -" + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now) + " - Environment : " + environment, ExecutionSteps);

            Helpers.FileWatcher(filewatcher);
                                
           //Console.WriteLine("Process finished you can hit the key");            
           //Console.ReadKey();
            
         }

    }
}
