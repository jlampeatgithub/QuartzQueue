using Newtonsoft.Json;
using Quartz;
using System;
using System.IO;
using System.Messaging;
using System.Reflection;

namespace QuartzQueue
{
    // <summary>
    // Monitors a single folder for new files.  If any are found, it grabs the first one, tries to post it to its destination, and moves quickly on to the rest if successful
    // </summary>
    internal class Job_Folder2MSMQ : IJob
    {

        public const int MaxMessageSize = 65536;

        /// <summary>
        /// Runs an instance of this job
        /// </summary>
        public void Execute(IJobExecutionContext context)
        {
            log.Debug(MethodBase.GetCurrentMethod().DeclaringType.Name + "." + MethodBase.GetCurrentMethod().Name + "()");

            // Pull up the job context
            JobKey key = context.JobDetail.Key;
            JobDataMap dataMap = context.JobDetail.JobDataMap;
            string XMLTaskJSON = dataMap.GetString("XMLTask");
            //log.Debug("Running with raw parameters: " + XMLTaskJSON);
            XMLTask _XMLTask = JsonConvert.DeserializeObject<XMLTask>(XMLTaskJSON);
            //log.Debug("Running with parsed parameters: \n" + _XMLTask.DebugDump());

            // Make sure the directory exists
            if (!Directory.Exists(_XMLTask.SourcePath))
            {
                log.Error("Could not access " + _XMLTask.SourcePath + "!  (Does the folder exist?)");
                return;
            }

            // Make sure the queue exists 
            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(_XMLTask.DestinationPath))
            {
                messageQueue = new MessageQueue(_XMLTask.DestinationPath);
                messageQueue.Formatter = new ActiveXMessageFormatter();  // This allows the direct import of XML from files
            }
            else
            {
                // DO NOT Create the Queue - Complain!
                //MessageQueue.Create(@".\Private$\SomeTestName");
                log.Error("Could not access " + _XMLTask.DestinationPath + "!  (Does the queue exist? Do you have permissions to it?)");
                return;
            }

            // While there are any CLOSED, non-temp files in the folder, get the oldest and shove it into the MSMQ
            try
            {
                DirectoryInfo di = new DirectoryInfo(_XMLTask.SourcePath);
                FileInfo[] files;
                files = di.GetFiles();
                if (files.Length == 0)
                {
                    log.DebugFormat("Found no files in {0}", _XMLTask.SourcePath);
                    return;
                }

                // Sort and find the oldest
                // Code borrowed from https://www.informit.com/guides/content.aspx?g=dotnet&seqNum=813
                Array.Sort(files, (f1, f2) => { return f1.LastWriteTime.CompareTo(f2.LastWriteTime); });
                int i = 0;
                int iFileCount = 0;
                string sIgnoreExtension = ".tmp";
                string sLabel = "";
                byte[] bytes = new byte[MaxMessageSize];
                int iMessageLength = 0;
                DateTime dt = DateTime.Now;
                for (i = 0; i < files.Length; i++)
                {
                    string sFilePath = files[i].FullName;
                    if (files[i].Name.EndsWith(sIgnoreExtension)) {
                        log.DebugFormat("Ignoring partial file {0}...", sFilePath);
                    } else {
                        log.DebugFormat("Attempting to post and then delete file {0}...", sFilePath);
                        _XMLTask.PopulateName(dt, iFileCount, files[i].Name);
                        sLabel = _XMLTask.DestinationName;
                        // Traditional queue
                        if (_XMLTask.DestinationTransactional == "")
                        {
                            try
                            {
                                switch (_XMLTask.SourceEncoding)
                                {
                                    case "ASCII":
                                        bytes = File.ReadAllBytes(sFilePath);
                                        iMessageLength = bytes.GetLength(0);
                                        System.Text.ASCIIEncoding ascii = new System.Text.ASCIIEncoding();
                                        messageQueue.Send(ascii.GetString(bytes, 0, iMessageLength), sLabel);
                                        log.InfoFormat("Posted file {0} to {1} as {2} (with ASCII) OK.", _XMLTask.SourcePath + "\\" + sFilePath, _XMLTask.DestinationPath, _XMLTask.DestinationName);
                                        break;
                                    case "Unicode":
                                        messageQueue.Send(File.ReadAllText(sFilePath), sLabel);
                                        log.InfoFormat("Posted file {0} to {1} as {2} (with Unicode) OK.", _XMLTask.SourcePath + "\\" + sFilePath, _XMLTask.DestinationPath, _XMLTask.DestinationName);
                                        break;
                                }
                            }
                            catch (Exception ex)
                            {
                                log.ErrorFormat("Error posting file {0} to queue {2}: {1}.", sFilePath, ex.Message, messageQueue.Path);
                            }
                        }
                        else
                        {
                            // Transactional queue (new in 0.9.2)
                            try
                            {
                                var transaction = new MessageQueueTransaction();
                                switch (_XMLTask.SourceEncoding)
                                {
                                    case "ASCII":
                                        bytes = File.ReadAllBytes(sFilePath);
                                        iMessageLength = bytes.GetLength(0);
                                        System.Text.ASCIIEncoding ascii = new System.Text.ASCIIEncoding();
                                        transaction.Begin();
                                        messageQueue.Send(ascii.GetString(bytes, 0, iMessageLength), sLabel, transaction);
                                        transaction.Commit();
                                        log.InfoFormat("Posted file {0} to {1} as {2} (with ASCII) OK.", _XMLTask.SourcePath + "\\" + sFilePath, _XMLTask.DestinationPath, _XMLTask.DestinationName);
                                        break;
                                    case "Unicode":
                                        transaction.Begin();
                                        messageQueue.Send(File.ReadAllText(sFilePath), sLabel, transaction);
                                        transaction.Commit();
                                        log.InfoFormat("Posted file {0} to {1} as {2} (with Unicode) OK.", _XMLTask.SourcePath + "\\" + sFilePath, _XMLTask.DestinationPath, _XMLTask.DestinationName);
                                        break;
                                }
                            }
                            catch (Exception ex)
                            {
                                log.ErrorFormat("Error posting file {0} to transactional queue {2}: {1}.", sFilePath, ex.Message, messageQueue.Path);
                            }
                        }
                        try
                        {
                            File.Delete(sFilePath);
                            log.DebugFormat("Deleted file {0} OK.", sFilePath);
                        }
                        catch (IOException iex)
                        {
                            log.ErrorFormat("Error deleting file {0}: {1}.", sFilePath, iex.Message);
                        }
                        iFileCount++;
                    }
                }

            }
            catch (Exception e)
            {
                log.Error("Could not work with folder or queue.");
                log.Debug("Exception details: " + e.ToString());
            }

        }

        /// <summary>
        /// Recommended per-class reference to log4net (http://www.codeproject.com/Articles/140911/log4net-Tutorial)
        /// </summary>
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

    }
}