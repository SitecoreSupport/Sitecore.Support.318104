namespace Sitecore.Support.Tasks
{
  using Sitecore.ContentSearch;
  using System;
  using Sitecore.Jobs;
  using System.Collections.Generic;
  using System.Linq;
  using Sitecore.ContentSearch.Azure;

  public class CloudSearchIndexReinitAgent
  {
    protected enum LogLevel
    {
      Info,
      Warn,
    }

    protected string _agentName = "CloudSearchIndexReinitAgent";

    private bool _logActivity = true;

    public bool LogActivity
    {
      get
      {
        return _logActivity;
      }
      set
      {
        _logActivity = value;
      }
    }

    public void Run()
    {
      try
      {
        EnsureIndexesInitialized();
      }
      catch (Exception ex)
      {
        var message = $"[{_agentName}] one or more errors occurred";

        Diagnostics.Log.Error(message, ex, this);
      }
    }

    protected virtual void EnsureIndexesInitialized()
    {
      LogInfo("Agent started");

      var cloudSearchIndexes = ContentSearchManager.Indexes.Where(i => (i as CloudSearchProviderIndex) != null).Select(i => i as CloudSearchProviderIndex);

      var unInitializedIndexes = cloudSearchIndexes.Where(i => i.IsInitialized == false);

      LogInfo($"found '{unInitializedIndexes.Count()}' indexes for re-initialiation.");

      if (unInitializedIndexes.Count() == 0)
      {
        LogInfo($"Agent finished");

        return;
      }

      var jobOptions = GetJobOptions(unInitializedIndexes);

      LogInfo($"queuing index re-initialization job in a separate thread.");

      JobManager.Start(jobOptions);

      LogInfo($"Agent finished");
    }

    protected virtual void InitializeIndexes(IEnumerable<CloudSearchProviderIndex> indexes)
    {
      if (indexes == null)
      {
        LogWarn("indexes list is null.");

        return;
      }

      if (indexes.Count() == 0)
      {
        LogWarn("indexes list is empty.");

        return;
      }

      if (Context.Job != null)
      {
        Context.Job.Status.Total = indexes.Count();
      }

      foreach (var index in indexes)
      {
        if (!index.IsInitialized)
        {
          LogInfo($"initializing '{index.Name}' index.");


          try
          {
            index.Initialize();
          }
          catch (Exception ex)
          {
            LogWarn($"index '{index.Name}' has not been re-initialized. \r\n{ex.Message}\r\n{ex.StackTrace}");
          }

          if (index.IsInitialized && Sitecore.Context.Job != null)
          {
            if (Context.Job != null)
            {
              Context.Job.Status.IncrementProcessed();
            }

            LogInfo($"index '{index.Name}' has been re-initialized successfully.");
          }
          else
          {
            LogWarn($"index '{index.Name}' has not been re-initialized. One or more errors occurred.");
          }
        }
        else
        {
          LogWarn($"index '{index.Name}' has been skipped. It has already been initialized.");
        }
      }
    }

    protected virtual JobOptions GetJobOptions(IEnumerable<CloudSearchProviderIndex> indexes)
    {
      var jobName = "Sitecore.Support.Tasks.CloudSearchIndexReInitAgent";
      var category = "IndexInitializing";
      var siteName = "shell";
      var methodName = "InitializeIndexes";

      var options = new JobOptions(jobName, category, siteName, this, methodName, new object[] { indexes });

      return options;
    }

    private void LogInfo(string message) { LogMessage(message, LogLevel.Info); }
    private void LogWarn(string message) { LogMessage(message, LogLevel.Warn); }

    protected virtual void LogMessage(string message, LogLevel level = LogLevel.Info)
    {
      if (!LogActivity)
      {
        return;
      }

      message = $"[{_agentName}] " + message;

      switch (level)
      {
        case LogLevel.Info:
          Diagnostics.Log.Info(message, this);
          break;
        case LogLevel.Warn:
          Diagnostics.Log.Warn(message, this);
          break;
        default:
          Diagnostics.Log.Info(message, this);
          break;
      }
    }
  }
}