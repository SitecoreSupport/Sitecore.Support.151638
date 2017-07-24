namespace Sitecore.Support.Data.Eventing
{
  using System.Linq;
  using Sitecore.Data;
  using Sitecore.Data.DataProviders.Sql;
  using Sitecore.Diagnostics;
  using Sitecore.Eventing;

  [UsedImplicitly]
  public class SqlServerEventQueue : Sitecore.Data.Eventing.SqlServerEventQueue
  {
    public SqlServerEventQueue(SqlDataApi api)
    : base(api)
    {
    }

    public SqlServerEventQueue(SqlDataApi api, Database database)
    : base(api, database)
    {
    }

    public override QueuedEvent GetLastEvent()
    {
      var statement = new SqlStatement
      {
        Select = "SELECT TOP(1) {0}EventType{1}, {0}InstanceType{1}, {0}InstanceData{1}, {0}InstanceName{1}, {0}UserName{1}, {0}Stamp{1}, {0}Created{1}",
        From = "FROM {0}EventQueue{1}",
        OrderBy = "ORDER BY {0}Stamp{1} DESC"
      };

      return DataApi.CreateObjectReader(statement.Sql, new object[0], CreateQueuedEvent)
        .FirstOrDefault();
    }

    public override long GetQueuedEventCount()
    {
      const string Query = "SELECT COUNT(*) FROM {0}EventQueue{1}";

      return DataApi.CreateObjectReader(Query, new object[0], r => GetLong(r, 0))
        .FirstOrDefault();
    }

    protected override SqlStatement GetSqlStatement(EventQueueQuery query)
    {
      Assert.ArgumentNotNull(query, nameof(query));

      var statement = new SqlStatement
      {
        Select = "SELECT {0}EventType{1}, {0}InstanceType{1}, {0}InstanceData{1}, {0}InstanceName{1}, {0}UserName{1}, {0}Stamp{1}, {0}Created{1}",
        From = "FROM {0}EventQueue{1}",
        OrderBy = "ORDER BY {0}Stamp{1}"
      };

      AddCriteria(statement, query);

      return statement;
    }
  }
}