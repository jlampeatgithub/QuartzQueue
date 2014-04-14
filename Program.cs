using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Configuration;
using System.Xml;
using Newtonsoft.Json;      // Serialization
using NDesk;                // Command-line parsing
using Quartz;               // Scheduling (and multithreading) 
using Quartz.Impl;
using Quartz.Job;
using Topshelf;             // Command line as a service
using log4net;              // Logging (also used by Quartz and Topshelf)
[assembly: log4net.Config.XmlConfigurator(ConfigFile = "Log4NetConfig.xml",Watch = true)]

// QuartzQueue by Jonathan Lampe and File Transfer Consulting (http://www.filetransferconsulting.com) 
// 
// LICENSE: GPL 3.0 (see "License.txt" for more information)
// 
// Revision History:
// 
// 0.9.0 - March 17, 2014 
//  - INITIAL VERSION
//
// 0.9.1 - March 20, 2014
//  - FEAT: Increased maximum MSMQ message size from 8192 to 65536
//  - FEAT: Added option ("[GUID]") to use GUIDs as message labels or filenames 
//  - FEAT: Added Name filed to Destination definition to rename message label or filename 
//  - FEAT: Added "QuartzQueueTest" project to perform NUnit tests
//  - FEAT: Added option to handle ASCII/Unicode encoding on the message queue (default = Unicode)
//  - FEAT: Added name of task to "started" message
//  - BUG:  Corrected "ToMSMQ/FromMSMQ" labels in doc and sample.
//
// 0.9.2 - April 6, 2014
//  - FEAT: Added option to MSMQ destinations to post to transactional queues
//
// 1.0.0 - April 8, 2014
//  - FEAT: Added TopShelf support to run QuartzQueue as a service
//
// 1.0.1 - April 10, 2014
//  - FEAT: Added licensing information to prepare for public release
//  - FEAT: Added command-line parameter to force use of app.config in interactive mode
//  - BUG:  Changed "no argument" behavior back to pre-TopShelf

namespace QuartzQueue
{
    public class QuartzQueue : Worker
    {
        enum InitialAction { NormalExecution, DisplayHelpWithError, DisplayHelpOK }

        static string sConfigPath = "";
        static bool bQuiet = false;
        const string sVersion = "1.0.1";
        string[] args;  // Moved into the class (from a freestanding Main) to support Topshelf

        /// <summary>
        /// New constructor to handle Topshelf and allow arguments to be passed in from the original command line
        /// </summary>
        public QuartzQueue(string[] argsFromTopshelf)
        {
            args = argsFromTopshelf;
        }

        /// <summary>
        /// Parses incoming command-line variables and decides what to do (try to run or display help, with or without an error message)
        /// </summary>
        /// <param name="args">The original args passed in from the original Main() invocation.</param>
        //static void Main(string[] args)
        protected override void DoWork()
        {
            // If there were no command-line arguments from the command-line (probably because we were running as a service)
            // then sample the "commandline" section of the application configuration.
            bool bCalledAsAService = false;
            // If we're being called by TopShelf, we'd expect these first four arguments:
            // arg #0:-displayname
            // arg #1:QuartzQueue
            // arg #2:-servicename
            // arg #3:QuartzQueue
            if (args.Length >= 4)
            {
                if (args[0].Contains("displayname") && args[2].Contains("servicename"))
                {
                    bCalledAsAService = true;
                }
            }
            // Also look for the "useappcfg" flag
            if (args.Length > 0)
            {
                for (int iArg = 0; iArg < args.Length; iArg++)
                {
                    if (args[iArg].ToLower() == "-useappcfg")
                    {
                        bCalledAsAService = true;
                    }
                }
            }
            if (bCalledAsAService)
            {
                string sAltArgs = System.Configuration.ConfigurationManager.AppSettings["commandline"];
                log.Debug("Using alternate commandline: " + sAltArgs);
                args = sAltArgs.Split(' ');
            }
            
            // Parse the incoming arguments and decide what to do
            switch (ParseArgs(args))
            {
                case InitialAction.NormalExecution:
                    DisplayAuthor(); break;
                case InitialAction.DisplayHelpWithError:
                    DisplayAuthor(); DisplayHelp(); Environment.Exit(1); break;
                case InitialAction.DisplayHelpOK:
                    DisplayAuthor(); DisplayHelp(); Environment.Exit(0); break;
            }

            //Create the scheduler factory and a factory instance
            ISchedulerFactory schedulerFactory = new StdSchedulerFactory();
            IScheduler scheduler = schedulerFactory.GetScheduler();
            //Start the scheduler so that it can start executing jobs, but pause it so nothing gets executed yet
            scheduler.Start();
            scheduler.PauseAll();

            // Parse the configuration file and schedule its contents
            bool bReadyToStart = ParseAndScheduleTasks(scheduler);

            if (bReadyToStart)
            {
                // Unpause everything
                scheduler.ResumeAll();
                // RETIRED: Wait for a key press. If we don't wait the program exits and the scheduler gets destroyed
                // NEW: Cycle until run flag is turned off (check 5 times a second)
                while (!this.stop)
                {
                    Thread.Sleep(200);
                }
            }
            else
            {
                log.Error("Error reading configuration file or scheduling tasks - execution halted!");
            }

            //A nice way to stop the scheduler, waiting for jobs that are running to finish
            scheduler.Shutdown(true);

        }

        /// <summary>
        /// Parses configured tasks and posts them to the scheduler
        /// </summary>
        private static bool ParseAndScheduleTasks(IScheduler scheduler)
        {
            // Try to load up the configuration file as an XML document
            XmlDocument doc = new XmlDocument();
            try
            {
                doc.Load(sConfigPath);
            }
            catch (Exception e)
            {
                log.Error("Could not parse config file - is it valid XML?");
                log.Debug("Error exception: " + e.ToString());
                return false;
            }

            // Get ready to parse elements and schedule them
            XmlNodeList nodes = doc.DocumentElement.SelectNodes("/Tasks/Task");
            List<XMLTask> _XMLTasks = new List<XMLTask>();
            IJobDetail job = null;
            ITrigger trigger = null;

            try
            {
                foreach (XmlNode node in nodes)
                {
                    XMLTask _XMLTask = new XMLTask();

                    _XMLTask.Name = node.Attributes["Name"].Value;
                    _XMLTask.CheckMS = node.Attributes["CheckMS"].Value;
                    _XMLTask.SourceType = node.SelectSingleNode("Source").Attributes["Type"].Value;
                    _XMLTask.SourcePath = node.SelectSingleNode("Source").Attributes["Path"].Value;
                    _XMLTask.DestinationType = node.SelectSingleNode("Destination").Attributes["Type"].Value;
                    _XMLTask.DestinationPath = node.SelectSingleNode("Destination").Attributes["Path"].Value;
                    if (node.SelectSingleNode("Destination").Attributes["NameTemplate"] != null)
                    {
                        _XMLTask.DestinationNameTemplate = node.SelectSingleNode("Destination").Attributes["NameTemplate"].Value;
                    }
                    if (node.SelectSingleNode("Source").Attributes["Encoding"] != null)
                    {
                        _XMLTask.SourceEncoding = node.SelectSingleNode("Source").Attributes["Encoding"].Value;
                    }
                    if (node.SelectSingleNode("Destination").Attributes["Transactional"] != null)
                    {
                        _XMLTask.DestinationTransactional = node.SelectSingleNode("Destination").Attributes["Transactional"].Value;
                    }
                    _XMLTask.FigureOutTaskType();
                    _XMLTask.PerformBasicValidation();
                    // _XMLTask.PopulateName(DateTime.Now,...); We don't call this here because it takes "now()" and a per-message/file iterator!
                    //log.Debug("Read in task from XML:\n" + _XMLTask.DebugDump());

                    // Prepare a scheduler with the interval specified in the configuration file
                    Quartz.SimpleScheduleBuilder SSB = Quartz.SimpleScheduleBuilder.Create();
                    SSB.WithInterval(TimeSpan.FromMilliseconds(Double.Parse(_XMLTask.CheckMS)));

                    // Schedule the appropriate kind of job and prepare the associated trigger
                    switch (_XMLTask.TaskType)
                    {
                        case "MSMQ2Folder":
                            job = JobBuilder.Create(typeof(Job_MSMQ2Folder)).WithIdentity("Job_" + _XMLTask.Name, "Job_MSMQ2Folder").UsingJobData("XMLTask", JsonConvert.SerializeObject(_XMLTask)).Build();
                            trigger = TriggerBuilder.Create().WithSchedule(SSB.RepeatForever()).StartNow().WithIdentity("Trigger_" + _XMLTask.Name, "Trigger_MSMQ2Folder").Build();
                            scheduler.ScheduleJob(job, trigger);
                            log.Info("Started task " + _XMLTask.Name + " to check every " + _XMLTask.CheckMS + "ms for messages in " + _XMLTask.SourcePath + " to write out into " + _XMLTask.DestinationPath + ".");
                            break;
                        case "Folder2MSMQ":
                            job = JobBuilder.Create(typeof(Job_Folder2MSMQ)).WithIdentity("Job_" + _XMLTask.Name, "Job_Folder2MSMQ").UsingJobData("XMLTask", JsonConvert.SerializeObject(_XMLTask)).Build();
                            trigger = TriggerBuilder.Create().WithSchedule(SSB.RepeatForever()).StartNow().WithIdentity("Trigger_" + _XMLTask.Name, "Trigger_Folder2MSMQ").Build();
                            scheduler.ScheduleJob(job, trigger);
                            log.Info("Started task " + _XMLTask.Name + " to check every " + _XMLTask.CheckMS + "ms for files in " + _XMLTask.SourcePath + " to post to " + _XMLTask.DestinationPath + ".");
                            break;
                    }

                    _XMLTasks.Add(_XMLTask);
                }
            }
            catch (Exception e)
            {
                log.Error("Could not parse tasks within config file. (Config file looks like valid XML - are the tasks configured correctly?)");
                log.Debug("Error exception: " + e.ToString());
                return false;
            }

            if (_XMLTasks.Count() == 0)
            {
                log.Error("Config file appears to be valid XML, but contains no tasks!");
                return false;
            }
            log.InfoFormat("Read in {0} tasks OK.  Starting execution momentarily.", _XMLTasks.Count());

            return true;
        }

        /// <summary>
        /// Parses incoming command-line variables and decides what to do (try to run or display help, with or without an error message)
        /// </summary>
        /// <param name="args">The original args passed in from the original Main() invocation.</param>
        /// <returns>0 = continue execution, 1 = display help and return error, 2 = display help and return OK (i.e., help was requested)</returns>
        private static InitialAction ParseArgs(string[] argsToParse)
        {
            InitialAction iaReturn = InitialAction.DisplayHelpWithError;

            // Invoke NDesk parser to parse incoming arguments
            // (Based on sample from http://stackoverflow.com/questions/491595/best-way-to-parse-command-line-arguments-in-c)
            bool bShowHelp = false;
            bool bTryQuiet = false;
            string sDebug = "";
            bool bUseAppConfig = false; // Note that this gets read BEFORE ParseArgs is called - just here for completeness
            var p = new NDesk.Options.OptionSet() {
                { "config=", "the config file",  v => sConfigPath = v },
                { "help",  "show this message and exit", v => bShowHelp = v != null },
                { "quiet",  "suppress program information and help", v => bTryQuiet = v != null },
                { "debug=", "log4net override debug level",  v => sDebug = v },
                { "useappconfig", "force use of app.config file",  v => bUseAppConfig = v != null },
            };
            List<string> extra;
            try
            {
                extra = p.Parse(argsToParse);
            }
            catch
            {
                // Parser encountered an error! 
                log.Error("Could not read command line arguments - execution halted!");
                return InitialAction.DisplayHelpWithError;
            }

            // First set the debug level
            if (sDebug.Length > 0)
            {
                switch (sDebug.ToUpper())
                {
                    case "ERROR":
                        ((log4net.Repository.Hierarchy.Hierarchy)LogManager.GetRepository()).Root.Level = log4net.Core.Level.Error;
                        ((log4net.Repository.Hierarchy.Hierarchy)LogManager.GetRepository()).RaiseConfigurationChanged(EventArgs.Empty); break;
                    case "WARN":
                        ((log4net.Repository.Hierarchy.Hierarchy)LogManager.GetRepository()).Root.Level = log4net.Core.Level.Warn;
                        ((log4net.Repository.Hierarchy.Hierarchy)LogManager.GetRepository()).RaiseConfigurationChanged(EventArgs.Empty); break;
                    case "INFO":
                        ((log4net.Repository.Hierarchy.Hierarchy)LogManager.GetRepository()).Root.Level = log4net.Core.Level.Info;
                        ((log4net.Repository.Hierarchy.Hierarchy)LogManager.GetRepository()).RaiseConfigurationChanged(EventArgs.Empty); break;
                    case "DEBUG":
                        ((log4net.Repository.Hierarchy.Hierarchy)LogManager.GetRepository()).Root.Level = log4net.Core.Level.Debug;
                        ((log4net.Repository.Hierarchy.Hierarchy)LogManager.GetRepository()).RaiseConfigurationChanged(EventArgs.Empty); break;
                    default:
                        log.WarnFormat("Ignoring invalid debug level! Log4Net value of {0} will be used instead.", ((log4net.Repository.Hierarchy.Hierarchy)LogManager.GetRepository()).Root.Level);
                        break;
                }

            }

            // Look for explicit request for help.  If there is one, ignore the quiet flag (otherwise check it appropriately)
            if (bShowHelp)
            {
                return InitialAction.DisplayHelpOK;
            }
            else
            {
                // If the user didn't explicit ask for help or run into any command line argument exceptions, we should attempt to continue
                iaReturn = InitialAction.NormalExecution;
                if (bTryQuiet)
                {
                    bQuiet = true;
                }
            }

            // Also check to see if the configuration file exists
            if (!System.IO.File.Exists(sConfigPath))
            {
                log.Error("Could not find configuration file - execution halted!");
                return InitialAction.DisplayHelpWithError;
            }

            return iaReturn;
        }

        /// <summary>
        /// Displays basic program information unless suppressed by quiet (although quiet is ignored if help explicitly requested)
        /// </summary>
        private static void DisplayAuthor()
        {
            if (!bQuiet)
            {
                Console.BackgroundColor = ConsoleColor.Black;
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("QuartzQueue {0} - MSMQ<-->folder XML file exchange", sVersion);
                Console.WriteLine("  by File Transfer Consulting (http://www.filetransferconsulting.com)");
                Console.ResetColor();
            }
        }

        /// <summary>
        /// Displays a friendly help message unless suppressed by quiet (although quiet is ignored if help explicitly requested)
        /// </summary>
        private static void DisplayHelp()
        {
            if (!bQuiet)
            {
                Console.BackgroundColor = ConsoleColor.Black;
                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine();
                Console.WriteLine("Usage: ");
                Console.WriteLine("  QuartzQueue.exe -config=(filename) [-debug=X] [-quiet]");
                Console.WriteLine();
                Console.WriteLine("    -config=... Set the task configuration file (REQUIRED).");
                Console.WriteLine("    -debug=...  Override the Log4Net debug level.");
                Console.WriteLine("                (ERROR, WARN, INFO or DEBUG)");
                Console.WriteLine("    -quiet      Suppresses program title and this help (on errors).");
                Console.WriteLine("    -useappcfg  Tells QuartzQueue to read command line parameters from");
                Console.WriteLine("                QuartzQueue.exe.config instead of the command line.");
                Console.WriteLine("    -help       Displays this help.");
                Console.WriteLine();
                Console.WriteLine("Task Configuration: ");
                Console.WriteLine("   Tasks are defined in the XML you provide with the -config argument.");
                Console.WriteLine("   Here is a quick sample configuration: ");
                Console.WriteLine("     <Tasks><Task Name=\"FromMSMQ\" CheckMS=\"200\">");
                Console.WriteLine("       <Source Type=\"MSMQ\" Path=\".\\private$\\ToAFolder\"/>");
                Console.WriteLine("       <Destination Type=\"Folder\" Path=\"D:\\FromMSMQ\"/>");
                Console.WriteLine("     </Task><Task Name=\"ToMSMQ\" CheckMS=\"200\">");
                Console.WriteLine("       <Source Type=\"Folder\" Path=\"D:\\ToMSMQ\"/>");
                Console.WriteLine("       <Destination Type=\"MSMQ\" Path=\".\\private$\\FromAFolder\"/>");
                Console.WriteLine("     </Task></Tasks>");
                Console.WriteLine();
                Console.WriteLine("   Additional options allow you to specify destination names and ");
                Console.WriteLine("   labels (with macros) and to post to transactional queues.");
                Console.WriteLine();
                Console.WriteLine("   See documentation to install, configure and run as a service.");
                Console.WriteLine("   (This is a \"Topshelf\" service with alternate command-line input.)");
                Console.WriteLine();
                Console.WriteLine("Logging and Display Configuration: ");
                Console.WriteLine("   QuartzQueue uses the popular Log4Net library and its log and display");
                Console.WriteLine("   configuration is performed in the Log4NetConfig.xml file, but the");
                Console.WriteLine("   current debug level may be overridden using the -debug option.");
                Console.WriteLine();
                Console.ResetColor();
            }
        }

        /// <summary>
        /// Recommended per-class reference to log4net (http://www.codeproject.com/Articles/140911/log4net-Tutorial)
        /// </summary>
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

    }

    // Topshelf is now invoked first
    public class Program
    {

        /// <summary>
        /// Minimal implementation of Topshelf that supports custom command-line options, based on:
        /// http://callumhibbert.blogspot.com/2012/10/building-windows-services-with-topshelf.html
        /// http://stackoverflow.com/questions/15004212/how-can-i-use-commandline-arguments-that-is-not-recognized-by-topshelf
        /// </summary>
        public static void Main(string[] args)
        {
            HostFactory.Run(hostConfigurator =>                                 
            {
                // Pass through the allowed command-line parameters
                string sCMDquiet = null;
                string sCMDconfig = null;
                string sCMDdebug = null;
                string sCMDhelp = null;
                string sCMDuseappcfg = null;
                hostConfigurator.AddCommandLineDefinition("quiet", f => { sCMDquiet = f; });
                hostConfigurator.AddCommandLineDefinition("config", f => { sCMDconfig = f; });
                hostConfigurator.AddCommandLineDefinition("debug", f => { sCMDdebug = f; });
                hostConfigurator.AddCommandLineDefinition("help", f => { sCMDhelp = f; });
                hostConfigurator.AddCommandLineDefinition("useappcfg", f => { sCMDuseappcfg = f; });
                hostConfigurator.ApplyCommandLine();

                hostConfigurator.Service<QuartzQueue>(serviceConfigurator =>               // Tell Topshelf about QuartzQueue class
                {
                    serviceConfigurator.ConstructUsing(name => new QuartzQueue(args));     // Build an QuartzQueue class instance
                    serviceConfigurator.WhenStarted(qq => qq.Start());                     // Handle service start
                    serviceConfigurator.WhenStopped(qq => qq.Stop());                      // Handle service stop
                });
                //hostConfigurator.RunAsLocalSystem();                                       // Run as LocalSystem by default
                hostConfigurator.RunAsPrompt();                                            // Ask installer for creds (better for accessing MSMQ!)

                // Configure the service
                hostConfigurator.SetDescription("QuartzQueue (MSMQ-Folder transfer) by File Transfer Consulting (http://www.filetransferconsulting.com)");
                hostConfigurator.SetDisplayName("QuartzQueue");
                hostConfigurator.SetServiceName("QuartzQueue");                            
            });                                                             
        }
    }

    // Quick little abstract class to handle service start and stop
    public abstract class Worker
    {
        private Thread thread;

        protected bool stop = true; // Changed from "private" in the example to "protected" so my implementation of DoWork could read it

        protected Worker()
        {
            this.SleepPeriod = new TimeSpan(0, 0, 0, 10);
            this.Id = Guid.NewGuid();
        }

        public TimeSpan SleepPeriod { get; set; }

        public bool IsStopped { get; private set; }

        protected Guid Id { get; private set; }

        public void Start()
        {
            string logMessage = string.Format("Starting worker of type '{0}'.", this.GetType().FullName);
            System.Diagnostics.Debug.WriteLine(logMessage);
            log.Debug(logMessage);
            this.stop = false;

            // Multiple thread instances cannot be created
            if (this.thread == null || this.thread.ThreadState == ThreadState.Stopped)
            {
                this.thread = new Thread(this.Run);
            }

            // Start thread if it's not running yet
            if (this.thread.ThreadState != ThreadState.Running)
            {
                this.thread.Start();
            }
        }

        public void Stop()
        {
            string logMessage = string.Format("Stopping worker of type '{0}'.", this.GetType().FullName);
            System.Diagnostics.Debug.WriteLine(logMessage);
            log.Debug(logMessage);
            this.stop = true;
        }

        protected abstract void DoWork();

        private void Run()
        {
            try
            {
                try
                {
                    //while (!this.stop)
                    //{
                    //    this.IsStopped = false;
                    //    this.DoWork();
                    //    Thread.Sleep(this.SleepPeriod);
                    //}
                    this.DoWork();  // Monitoring of this flag is the job of the function that implements this

                }
                catch (ThreadAbortException)
                {
                    Thread.ResetAbort();
                }
                finally
                {
                    this.thread = null;
                    this.IsStopped = true;
                    string logMessage = string.Format("Stopped worker of type '{0}'.", this.GetType().FullName);
                    System.Diagnostics.Debug.WriteLine(logMessage);
                    log.Debug(logMessage);
                }
            }
            catch (Exception e)
            {
                string exceptionMessage = string.Format("Error running the '{0}' worker.", this.GetType().FullName);
                System.Diagnostics.Debug.WriteLine(exceptionMessage, e);
                log.Warn(exceptionMessage);
                throw;
            }
        }

        /// <summary>
        /// Recommended per-class reference to log4net (http://www.codeproject.com/Articles/140911/log4net-Tutorial)
        /// </summary>
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
    
    }

}
