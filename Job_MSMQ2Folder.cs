using Newtonsoft.Json;
using Quartz;
using System;
using System.IO;
using System.Messaging;
using System.Reflection;

namespace QuartzQueue
{
    // <summary>
    // Monitors a single queue for new files.  If any are found, it grabs the first one, tries to post it to its destination, and moves quickly on to the rest if successful
    // </summary>
    class Job_MSMQ2Folder : IJob
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
            if (!Directory.Exists(_XMLTask.DestinationPath))
            {
                log.Error("Could not access " + _XMLTask.DestinationPath + "!  (Does the folder exist?)");
                return;
            }

            // Make sure the queue exists 
            MessageQueue messageQueue = null;
            if (MessageQueue.Exists(_XMLTask.SourcePath))
            {
                messageQueue = new MessageQueue(_XMLTask.SourcePath);
                messageQueue.Formatter = new ActiveXMessageFormatter();  // This allows the direct export of XML from files
            }
            else
            {
                // DO NOT Create the Queue - Complain!
                //MessageQueue.Create(@".\Private$\SomeTestName");
                log.Error("Could not access " + _XMLTask.SourcePath + "!  (Does the queue exist? Do you have permissions to it?)");
                return;
            }

            // While there are any entries in the queue, pull them off and shove them onto the filesystem
            try
            {
                int iMessageCount = 0;
                string sMessageFilename = "";
                string sMessagePath = "";
                int iMessageLength = 0;
                byte[] bytes = new byte[MaxMessageSize];
                DateTime dt = DateTime.Now;
                messageQueue = new MessageQueue(_XMLTask.SourcePath);
                Message[] messages = messageQueue.GetAllMessages();
                Message toss = null;  // Used to delete individual messages

                if (messages.Length == 0)
                {
                    log.DebugFormat("Found no messages in {0}", _XMLTask.SourcePath);
                    return;
                }

                foreach (Message message in messages)
                {
                    log.DebugFormat("Attempting to post and then delete message #{0}...", message.Id);
                    // Try to post the message to an XML file
                    //sMessageCount = iMessageCount.ToString().PadLeft(4, '0');
                    //sMessageFilename = "msg-" + dt.ToString("yyyyMMdd-HHmmss-fff-") + sMessageCount + ".xml";
                    _XMLTask.PopulateName(dt, iMessageCount, message.Label);
                    sMessageFilename = _XMLTask.DestinationName;
                    sMessagePath = _XMLTask.DestinationPath + "\\" + sMessageFilename;
                    // Get the content
                    // sMessageContent = message.Label;
                    iMessageLength = Int32.Parse(message.BodyStream.Length.ToString());
                    if (iMessageLength > MaxMessageSize)
                    {
                        log.WarnFormat("Ignored (and did not post) too-long message {0}.", message.Id, sMessagePath);
                    }
                    else
                    {
                        //log.InfoFormat("Message.Id={0}", message.Id);        // e.g., 11b4ce53-f956-4397-8dc6-18bd9db255ed\2082
                        //log.InfoFormat("Message.Label={0}", message.Label);  // e.g., 6af137c4-a1fa-47d4-a675-98201ea3eaf0 or whatever the Folder2MSMQ process set as the label
                        // TODO: Figure out ASCII/Unicode thing
                        message.BodyStream.Read(bytes, 0, iMessageLength);
                        switch (_XMLTask.SourceEncoding)
                        {
                            case "ASCII":
                                System.Text.ASCIIEncoding ascii = new System.Text.ASCIIEncoding();
                                File.WriteAllText(sMessagePath, ascii.GetString(bytes, 0, iMessageLength));
                                log.InfoFormat("Posted message {0} to {1} (with ASCII) OK.", _XMLTask.SourcePath + "::" + message.Id, sMessagePath);
                                break;
                            case "Unicode":
                                System.Text.UnicodeEncoding unicode = new System.Text.UnicodeEncoding();
                                File.WriteAllText(sMessagePath, unicode.GetString(bytes, 0, iMessageLength));
                                log.InfoFormat("Posted message {0} to {1} (with Unicode) OK.", _XMLTask.SourcePath + "::" + message.Id, sMessagePath);
                                break;
                        }
                    }
                    toss = messageQueue.ReceiveById(message.Id);
                    log.DebugFormat("Removed message \"{0}\" (ID:{1}) OK.", message.Label, message.Id);
                    iMessageCount++;
                }
                // after all processing, delete all the messages
                //messageQueue.Purge();
                //log.DebugFormat("Purged {0} OK.", _XMLTask.SourcePath);

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