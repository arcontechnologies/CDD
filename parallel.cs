using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

static void LoadSubDatasetParallel(DataSet dataset, DataTable ConfigTable, string table, int level)
{
    string urlbase = ServerUrl + "design/0000/v1/releases/";
    string credentials = Helpers.ReadConfiguration(ConfigTable, "url", "credentials");
    int pagesize = 100;

    string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
    string database = ConfigurationManager.AppSettings["database"].ToString();
    string connString = Helpers.GetConnectionString(dbserver, database);
    DataTable dtLinks = new DataTable();

    // Load the links data (same as before)
    if (level == 1)
    {
        string allowedReleasesQuery = ConfigurationManager.AppSettings["allowedReleasesQuery"].ToString();
        using (SqlConnection conn = new SqlConnection(connString))
        using (SqlCommand cmd = new SqlCommand(allowedReleasesQuery, conn))
        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
        {
            conn.Open();
            da.Fill(dtLinks);
        }
    }
    else
    {
        string query = "select releaseId, id from TB_STG_CDD_PHASES_data Where className = 'PhaseDto' group by releaseId, id";
        using (SqlConnection conn = new SqlConnection(connString))
        using (SqlCommand cmd = new SqlCommand(query, conn))
        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
        {
            conn.Open();
            da.Fill(dtLinks);
        }
    }

    // Thread-safe collections for error reporting
    ConcurrentBag<string> errors = new ConcurrentBag<string>();
    int processedCount = 0;
    int totalCount = dtLinks.Rows.Count;

    // Convert DataTable rows to a list for parallel processing
    var rowsList = dtLinks.Rows.Cast<DataRow>().ToList();

    // Configure parallelism - adjust MaxDegreeOfParallelism based on your server capacity
    // Start with 4-8 for API calls, increase if the API can handle more
    var parallelOptions = new ParallelOptions
    {
        MaxDegreeOfParallelism = 8 // Adjust this value based on testing
    };

    try
    {
        Parallel.ForEach(rowsList, parallelOptions, (rw) =>
        {
            // Each thread gets its own DataSet
            DataSet localDataset = new DataSet();
            string json = string.Empty;
            string url = urlbase;

            try
            {
                // Build URL
                if (level == 1)
                {
                    url = url + rw["id"].ToString() + "/" + table + "?page_size=" + pagesize;
                }
                else
                {
                    url = url + rw["releaseId"].ToString() + "/phases/" + rw["id"].ToString() + "/" + table + "?page_size=" + pagesize;
                }

                // Initialize local dataset (thread-safe copy of schema)
                Initialize_Dataset_ThreadSafe(localDataset, ConfigTable, table);

                // Make HTTP request
                json = Helpers.WebRequestWithToken(httpclient, url);
                string jsonempty = "{\"data\":[]}";

                if (json.TrimStart().StartsWith("<") == false)
                {
                    if (!json.Contains(jsonempty) && json != "NotFound")
                    {
                        XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json, "root");

                        // Process XML nodes
                        XmlElement root = doc.DocumentElement;
                        XmlNodeList GlobalNodeList = root.ChildNodes;

                        foreach (XmlNode node in GlobalNodeList)
                        {
                            Helpers.TraverseNodes(doc, node.ChildNodes, "applications");
                        }

                        root = doc.DocumentElement;
                        GlobalNodeList = root.ChildNodes;

                        foreach (XmlNode node in GlobalNodeList)
                        {
                            Helpers.TraverseNodes(doc, node.ChildNodes, "environments");
                        }

                        // Remove milestonePhaseRelations
                        XmlNodeList nodes = doc.SelectNodes("/root/data/milestonePhaseRelations");
                        for (int i = nodes.Count - 1; i >= 0; i--)
                        {
                            nodes[i].ParentNode.RemoveChild(nodes[i]);
                        }

                        // Load XML into local dataset
                        foreach (DataTable dataTable in localDataset.Tables)
                            dataTable.BeginLoadData();

                        localDataset.ReadXml(new XmlTextReader(new StringReader(doc.OuterXml)));

                        foreach (DataTable dataTable in localDataset.Tables)
                            dataTable.EndLoadData();

                        // Add forgotten tables (same logic as before)
                        AddMissingTables(localDataset, table);
                        AddMissingColumns(localDataset);

                        // Bulk insert - this needs to be thread-safe
                        if (json != "NotFound")
                        {
                            BulkinsertdynamicThreadSafe(localDataset, ConfigTable, dbserver, database, table);
                        }
                    }
                }

                // Progress tracking
                int current = Interlocked.Increment(ref processedCount);
                if (current % 100 == 0)
                {
                    Console.WriteLine($"Processed {current}/{totalCount} items for {table}");
                }
            }
            catch (Exception e)
            {
                string errorMsg = $"Error processing URL {url}: {e.Message}\nJSON: {json?.Substring(0, Math.Min(500, json?.Length ?? 0))}";
                errors.Add(errorMsg);
                logger.Error(e, $"An error occurred in LoadSubDatasetParallel for URL: {url}");
            }
            finally
            {
                localDataset?.Dispose();
            }
        });

        // Report any errors
        if (errors.Any())
        {
            ErrorReporting = ErrorReporting + string.Join("\n\n", errors);
            Console.WriteLine($"Completed with {errors.Count} errors");
        }

        ExecutionSteps = ExecutionSteps + "table " + table + " is loaded (parallel)\n";
        Console.WriteLine($"Completed loading {table}: {processedCount} items processed");
    }
    catch (AggregateException ae)
    {
        foreach (var e in ae.InnerExceptions)
        {
            logger.Error(e, "An error occurred in parallel processing: ");
        }
        throw;
    }
}

// Thread-safe version of Initialize_Dataset
static void Initialize_Dataset_ThreadSafe(DataSet dataset, DataTable ConfigTable, string table)
{
    // Clone the schema without locking the original ConfigTable
    // You may need to adjust this based on what Initialize_Dataset actually does
    lock (ConfigTable)
    {
        Initialize_Dataset(dataset, ConfigTable, table);
    }
}

// Thread-safe bulk insert using separate connections
static void BulkinsertdynamicThreadSafe(DataSet dataset, DataTable ConfigTable, string dbserver, string database, string table)
{
    // Each call creates its own connection, so it's inherently thread-safe
    // Just ensure your Bulkinsertdynamic doesn't use shared state
    Bulkinsertdynamic(dataset, ConfigTable, dbserver, database, table);
}

// Helper method to add missing tables
static void AddMissingTables(DataSet dataset, string table)
{
    if (table == "phases")
    {
        if (!dataset.Tables.Contains("ownerParties"))
        {
            dataset.Tables.Add(CreateOwnerPartiesTable());
        }
    }

    if (table == "tasks")
    {
        if (!dataset.Tables.Contains("prevTasks"))
        {
            dataset.Tables.Add(CreatePrevTasksTable());
        }

        if (!dataset.Tables.Contains("nextTasks"))
        {
            dataset.Tables.Add(CreateNextTasksTable());
        }

        if (!dataset.Tables.Contains("ownerParties"))
        {
            dataset.Tables.Add(CreateOwnerPartiesTable());
        }
    }
}

// Helper method to add missing columns
static void AddMissingColumns(DataSet dataset)
{
    if (dataset.Tables.Contains("executionData"))
    {
        DataTable table1 = dataset.Tables["executionData"];
        if (!table1.Columns.Contains("startDate"))
        {
            table1.Columns.Add(new DataColumn("startDate", typeof(string)));
            table1.Columns.Add(new DataColumn("endDate", typeof(string)));
        }
    }
}

// Factory methods for creating table schemas
static DataTable CreateOwnerPartiesTable()
{
    DataTable dt = new DataTable { TableName = "ownerParties" };
    dt.Columns.Add("email", typeof(string));
    dt.Columns.Add("notificationEnabled", typeof(string));
    dt.Columns.Add("firstName", typeof(string));
    dt.Columns.Add("lastName", typeof(string));
    dt.Columns.Add("superUser", typeof(string));
    dt.Columns.Add("role", typeof(string));
    dt.Columns.Add("name", typeof(string));
    dt.Columns.Add("id", typeof(string));
    dt.Columns.Add("className", typeof(string));
    dt.Columns.Add("data_Id", typeof(string));
    dt.Columns.Add("ownerParties_Id", typeof(string));
    return dt;
}

static DataTable CreatePrevTasksTable()
{
    DataTable dt = new DataTable { TableName = "prevTasks" };
    dt.Columns.Add("data_Id", typeof(string));
    dt.Columns.Add("id", typeof(string));
    dt.Columns.Add("className", typeof(string));
    return dt;
}

static DataTable CreateNextTasksTable()
{
    DataTable dt = new DataTable { TableName = "nextTasks" };
    dt.Columns.Add("data_Id", typeof(string));
    dt.Columns.Add("id", typeof(string));
    dt.Columns.Add("className", typeof(string));
    return dt;
}
