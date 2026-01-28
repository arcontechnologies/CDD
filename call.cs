DataSet SQLTasks = new DataSet(); // This is just a placeholder now
logger.Info("Load for tasks table has started (parallel)");
LoadSubDatasetParallel(SQLTasks, ConfigTable, "tasks", 2);
logger.Info("Load for tasks table has completed");



MainModule = 'proc.MainModule' threw an exception of type 'System.ComponentModel.Win32Exception'

string output = proc.StandardOutput.ReadToEnd();
string error = proc.StandardError.ReadToEnd();
  
task load for release table has started
task load for application table has started
task load for environment table has started
task load for tracks table has started

CustomAttributes = Method System.Reflection.MemberInfo.get_CustomAttributes cannot be called in this context.
StandardInput = 'proc.StandardInput' threw an exception of type 'System.InvalidOperationException'
