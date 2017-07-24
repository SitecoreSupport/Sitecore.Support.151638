namespace Sitecore.Support.Hooks
{
  using Sitecore.Configuration;
  using Sitecore.Data.SqlServer;
  using Sitecore.Diagnostics;
  using Sitecore.Events.Hooks;
  using Sitecore.Support.Data.Eventing;

  public class ReplaceEventQueue : IHook
  {
    public void Initialize()
    {
      foreach (var database in Factory.GetDatabases())
      {
        var connectionStringName = database.ConnectionStringName;
        if (string.IsNullOrEmpty(connectionStringName))
        {
          continue;
        }

        var connectionString = Settings.GetConnectionString(connectionStringName);
        var dataApi = new SqlServerDataApi(connectionString);
        
        Log.Info($"Replacing EventQueue of {database.Name} database - Sitecore.Support.151638", this);
        database.RemoteEvents.Queue = new SqlServerEventQueue(dataApi, database);
      }
    }
  }
}