using System;
using System.Diagnostics;
using System.Configuration;
using System.Threading.Tasks;

namespace cdd
{
    class Launcher
    {
        public static string ExecutionSteps { get; set; }
        public static string ErrorReporting { get; set; }
        public static string StepbyStep { get; set; }
        public static string emailadr { get; set; }

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
            ExecutionSteps = ExecutionSteps + "CDD Launcher started @ " + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now) + " - Environment : " + environment + '\n';
            ExecutionSteps = ExecutionSteps + "---------------------------------------------------------" + '\n';

            try
            {
                // Step 1: Execute cdd.exe 1 (truncate and load initial tables)
                Console.WriteLine("Starting Step 1: Initial data load...");
                int exitCode = ExecuteProcess(prgload, "1");
                if (exitCode != 0)
                {
                    throw new Exception($"Step 1 failed with exit code {exitCode}");
                }
                ExecutionSteps = ExecutionSteps + "Step 1 completed @ " + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now) + '\n';

                // Step 2, 3, 4: Execute in parallel
                Console.WriteLine("Starting Steps 2, 3, 4 in parallel...");
                
                Task<int> step2Task = Task.Run(() => ExecuteProcess(prgload, "2"));
                Task<int> step3Task = Task.Run(() => ExecuteProcess(prgload, "3"));
                Task<int> step4Task = Task.Run(() => ExecuteProcess(prgload, "4"));

                // Wait for all parallel tasks to complete
                Task.WaitAll(step2Task, step3Task, step4Task);

                // Check exit codes
                if (step2Task.Result != 0)
                {
                    throw new Exception($"Step 2 failed with exit code {step2Task.Result}");
                }
                if (step3Task.Result != 0)
                {
                    throw new Exception($"Step 3 failed with exit code {step3Task.Result}");
                }
                if (step4Task.Result != 0)
                {
                    throw new Exception($"Step 4 failed with exit code {step4Task.Result}");
                }

                ExecutionSteps = ExecutionSteps + "Steps 2, 3, 4 completed @ " + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now) + '\n';
                ExecutionSteps = ExecutionSteps + "Load CDD staging completed @ " + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now) + " - Environment : " + environment + '\n';

                // Execute cddsorted.exe
                Console.WriteLine("Starting cddsorted process...");
                Process sqlProc = new Process();
                sqlProc.StartInfo.FileName = prgsql;
                sqlProc.Start();
                sqlProc.WaitForExit();

                if (sqlProc.ExitCode != 0)
                {
                    throw new Exception($"cddsorted failed with exit code {sqlProc.ExitCode}");
                }

                ExecutionSteps = ExecutionSteps + "Load CDD ODS completed @ " + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now) + " - Environment : " + environment + '\n';
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred!!!: " + ex.Message);
                ErrorReporting = ex.Message + '\n' + ex.StackTrace;
                ExecutionSteps = ExecutionSteps + "ERROR: " + ex.Message + '\n';
                
                // Send error notification if needed
                // Email.SendMail(emailadr, "CDD Launcher ERROR - " + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now), ExecutionSteps + ErrorReporting);
                
                Environment.Exit(-1);
            }

            ExecutionSteps = ExecutionSteps + "---------------------------------------------------------" + '\n';
            ExecutionSteps = ExecutionSteps + "CDD Launcher finished @ " + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now) + " - Environment : " + environment + '\n';
            ExecutionSteps = ExecutionSteps + "---------------------------------------------------------" + '\n';

            // Email.SendMail(emailadr, "CDD Launcher - " + string.Format("{0:yyyy-MM-dd : HH:mm:ss}", DateTime.Now) + " - Environment : " + environment, ExecutionSteps);
            Helpers.FileWatcher(filewatcher);
        }

        // Helper method to execute a process and return its exit code
        static int ExecuteProcess(string fileName, string arguments)
        {
            Process proc = new Process();
            proc.StartInfo.FileName = fileName;
            proc.StartInfo.Arguments = arguments;
            proc.StartInfo.UseShellExecute = false;
            proc.StartInfo.RedirectStandardOutput = true;
            proc.StartInfo.RedirectStandardError = true;
            proc.EnableRaisingEvents = true;

            proc.Start();
            
            // Optional: Read output for logging
            string output = proc.StandardOutput.ReadToEnd();
            string error = proc.StandardError.ReadToEnd();
            
            proc.WaitForExit();

            if (!string.IsNullOrEmpty(output))
            {
                Console.WriteLine($"[{arguments}] Output: {output}");
            }
            if (!string.IsNullOrEmpty(error))
            {
                Console.WriteLine($"[{arguments}] Error: {error}");
            }

            return proc.ExitCode;
        }
    }
}
