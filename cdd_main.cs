static void Main(string[] args)
{
    string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
    string database = ConfigurationManager.AppSettings["database"].ToString();
    string environment = ConfigurationManager.AppSettings["ENV"].ToString();
    string configtable = ConfigurationManager.AppSettings["configtable"].ToString();
    DataTable ConfigTable = Helpers.LoadConfiguration(dbserver, database, configtable);
    string[] listtabletotruncate = Helpers.ReadListConfiguration(ConfigTable, "dbstatement", "truncate");
    string[] listtabletoload = Helpers.ReadListConfiguration(ConfigTable, "load", "load");
    ServerUrl = ConfigurationManager.AppSettings["ServerUrl"].ToString();
    Token = ConfigurationManager.AppSettings["Token"].ToString();
    httpclient = Helpers.WebAuthenticationWithToken();

    try
    {
        // Check if arguments are provided
        if (args.Length == 0)
        {
            logger.Error("No step argument provided. Usage: cdd.exe [1|2|3|4]");
            Environment.Exit(-1);
        }

        int step = 0;
        if (!int.TryParse(args[0], out step))
        {
            logger.Error("Invalid step argument. Must be a number (1, 2, 3, or 4)");
            Environment.Exit(-1);
        }

        logger.Info($"Starting CDD Step {step}");

        switch (step)
        {
            case 1:
                ExecuteStep1(dbserver, database, ConfigTable, listtabletotruncate, listtabletoload);
                break;
            case 2:
                ExecuteStep2();
                break;
            case 3:
                ExecuteStep3(ConfigTable);
                break;
            case 4:
                ExecuteStep4(ConfigTable);
                break;
            default:
                logger.Error($"Invalid step: {step}. Valid steps are 1, 2, 3, or 4");
                Environment.Exit(-1);
                break;
        }

        logger.Info($"CDD Step {step} completed successfully");
    }
    catch (Exception ex)
    {
        logger.Error(ex, $"An error has occurred in step execution");
        Environment.Exit(-1);
    }
}

// Step 1: Truncate tables and load initial data in parallel
static void ExecuteStep1(string dbserver, string database, DataTable ConfigTable, 
                         string[] listtabletotruncate, string[] listtabletoload)
{
    // Truncate all tables before the load
    SQLstatement(dbserver, database, listtabletotruncate, true);
    logger.Info("Load CDD started");

    Task[] tasks = new Task[listtabletoload.Length];
    int i = 0;
    foreach (var table in listtabletoload)
    {
        DataSet SQLTargetSet = new DataSet();
        tasks[i] = Task.Factory.StartNew(() => LoadDataFromCdd(SQLTargetSet, dbserver, database, ConfigTable, table), 
                                         TaskCreationOptions.LongRunning);
        i++;
    }

    Task.WaitAll(tasks);
    logger.Info("Load first set of tables completed");
}

// Step 2: Process tracks-releases links
static void ExecuteStep2()
{
    TrackLinks();
    logger.Info("Load track links completed");
}

// Step 3: Load application-versions
static void ExecuteStep3(DataTable ConfigTable)
{
    string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
    string database = ConfigurationManager.AppSettings["database"].ToString();

    // Gather all related Application-Versions to releases
    DataSet SQLAppVersions = new DataSet();
    logger.Info("Load for application-versions table has started");
    LoadSelectedAppVersion(SQLAppVersions, ConfigTable, "selected-application-versions", 1);
    Bulkinsertdynamic(SQLAppVersions, ConfigTable, dbserver, database, "selected-application-versions");
    SQLAppVersions.Clear();
    LoadSubDataset(SQLAppVersions, ConfigTable, "application-versions", 1);
    logger.Info("Load for application-versions table has completed");
}

// Step 4: Load phases and tasks
static void ExecuteStep4(DataTable ConfigTable)
{
    string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
    string database = ConfigurationManager.AppSettings["database"].ToString();

    DataSet SQLPhases = new DataSet();
    Console.WriteLine("task load for Phases table has started");
    logger.Info("Load for Phases table has started");
    LoadSubDataset(SQLPhases, ConfigTable, "phases", 1);
    logger.Info("Load for Phases table has completed");

    // Gather all related tasks to phases per Release
    DataSet SQLTasks = new DataSet();
    logger.Info("Load for tasks table has started");
    LoadSubDataset(SQLTasks, ConfigTable, "tasks", 2);
    logger.Info("Load for tasks table has completed");
}
