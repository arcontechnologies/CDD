using System;
using System.Diagnostics;
using System.Configuration;
using System.Threading.Tasks;
using System.IO;

namespace cdd
{
    class Launcher
    {
        public static string ExecutionSteps { get; set; }
        public static string ErrorReporting { get; set; }
        public static string emailadr { get; set; }

        static void Main(string[] args)
        {
            string environment = ConfigurationManager.AppSettings["ENV"]?.ToString() ?? "Unknown";
            // Get filenames from config
            string prgloadConfig = ConfigurationManager.AppSettings["PRGLOAD"]?.ToString();
            string prgsqlConfig = ConfigurationManager.AppSettings["PRGSQL"]?.ToString();
            string filewatcher = ConfigurationManager.AppSettings["FILEWATCHER"]?.ToString();
            emailadr = ConfigurationManager.AppSettings["Email"]?.ToString();

            // Resolve full paths to ensure UseShellExecute=false works
            string prgload = ResolvePath(prgloadConfig);
            string prgsql = ResolvePath(prgsqlConfig);

            ExecutionSteps = "---------------------------------------------------------" + '\n';
            ExecutionSteps += $"CDD Launcher started @ {DateTime.Now:yyyy-MM-dd : HH:mm:ss} - Environment : {environment}\n";
            ExecutionSteps += "---------------------------------------------------------" + '\n';

            try
            {
                // Validate files exist before starting
                if (!File.Exists(prgload)) throw new FileNotFoundException($"Cannot find cdd executable at: {prgload}");
                if (!File.Exists(prgsql)) throw new FileNotFoundException($"Cannot find cddsorted executable at: {prgsql}");

                // ---------------------------------------------------------
                // Step 1: Sequential Execution (cdd.exe 1)
                // ---------------------------------------------------------
                Console.WriteLine("Starting Step 1: Initial data load...");
                int exitCode1 = ExecuteProcess(prgload, "1");
                if (exitCode1 != 0) throw new Exception($"Step 1 failed with exit code {exitCode1}");

                ExecutionSteps += $"Step 1 completed @ {DateTime.Now:yyyy-MM-dd : HH:mm:ss}\n";

                // ---------------------------------------------------------
                // Step 2, 3, 4: Parallel Execution
                // ---------------------------------------------------------
                Console.WriteLine("Starting Steps 2, 3, 4 in parallel...");

                // Create tasks for parallel execution
                Task<int> task2 = Task.Run(() => ExecuteProcess(prgload, "2"));
                Task<int> task3 = Task.Run(() => ExecuteProcess(prgload, "3"));
                Task<int> task4 = Task.Run(() => ExecuteProcess(prgload, "4"));

                // Wait for all to finish
                Task.WaitAll(task2, task3, task4);

                // Validate results
                if (task2.Result != 0) throw new Exception($"Step 2 failed with exit code {task2.Result}");
                if (task3.Result != 0) throw new Exception($"Step 3 failed with exit code {task3.Result}");
                if (task4.Result != 0) throw new Exception($"Step 4 failed with exit code {task4.Result}");

                ExecutionSteps += $"Steps 2, 3, 4 completed @ {DateTime.Now:yyyy-MM-dd : HH:mm:ss}\n";
                ExecutionSteps += $"Load CDD staging completed\n";

                // ---------------------------------------------------------
                // Final Step: Execute cddsorted.exe
                // ---------------------------------------------------------
                Console.WriteLine("Starting cddsorted process...");
                int exitCodeSql = ExecuteProcess(prgsql, ""); // Assuming no args for cddsorted
                
                if (exitCodeSql != 0) throw new Exception($"cddsorted failed with exit code {exitCodeSql}");

                ExecutionSteps += $"Load CDD ODS completed @ {DateTime.Now:yyyy-MM-dd : HH:mm:ss}\n";
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nCRITICAL ERROR: {ex.Message}");
                ErrorReporting = ex.ToString();
                ExecutionSteps += $"ERROR: {ex.Message}\n";
                
                // Optional: Email sending logic here
                // Email.SendMail(emailadr, "CDD Launcher ERROR", ExecutionSteps + ErrorReporting);
                
                Environment.Exit(-1); // Exit with error code
            }

            ExecutionSteps += "---------------------------------------------------------\n";
            ExecutionSteps += $"CDD Launcher finished @ {DateTime.Now:yyyy-MM-dd : HH:mm:ss}\n";
            
            // Only trigger file watcher if successful
            if (!string.IsNullOrEmpty(filewatcher))
            {
                Helpers.FileWatcher(filewatcher);
            }
        }

        // Helper to resolve absolute path
        static string ResolvePath(string fileName)
        {
            if (string.IsNullOrEmpty(fileName)) return string.Empty;
            if (Path.IsPathRooted(fileName)) return fileName;

            // Combine with the directory where the Launcher is running
            return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, fileName);
        }

        static int ExecuteProcess(string fileName, string arguments)
        {
            try
            {
                Process proc = new Process();
                proc.StartInfo.FileName = fileName;
                proc.StartInfo.Arguments = arguments;
                
                // Crucial settings for capturing output and avoiding path errors
                proc.StartInfo.UseShellExecute = false; 
                proc.StartInfo.WorkingDirectory = Path.GetDirectoryName(fileName); // Run in the exe's folder
                proc.StartInfo.RedirectStandardOutput = true;
                proc.StartInfo.RedirectStandardError = true;
                proc.StartInfo.CreateNoWindow = true;

                Console.WriteLine($"[Executing] {Path.GetFileName(fileName)} {arguments}");

                proc.Start();

                // Read output asynchronously to prevent deadlocks
                string output = proc.StandardOutput.ReadToEnd();
                string error = proc.StandardError.ReadToEnd();

                proc.WaitForExit();

                // Log output if useful (or only on error)
                if (!string.IsNullOrEmpty(output)) Console.WriteLine($"[{arguments} OUT]: {output.Trim()}");
                if (!string.IsNullOrEmpty(error)) Console.WriteLine($"[{arguments} ERR]: {error.Trim()}");

                return proc.ExitCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Execution Failed] {fileName} {arguments}: {ex.Message}");
                return -1;
            }
        }
    }
}
