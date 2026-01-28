DataSet SQLTasks = new DataSet(); // This is just a placeholder now
logger.Info("Load for tasks table has started (parallel)");
LoadSubDatasetParallel(SQLTasks, ConfigTable, "tasks", 2);
logger.Info("Load for tasks table has completed");



MainModule = 'proc.MainModule' threw an exception of type 'System.ComponentModel.Win32Exception'
