using System;

namespace QuartzQueue
{
    // <summary>
    // Represents a single task configured in XML.
    // </summary>
    public class XMLTask
    {
        public string Name = "";                     // User-provided name, but don't get cute.
        public string CheckMS = "";                  // User-provided integer like "200".
        public string TaskType = "";                 // Derived from SourceType and DestinationType by FigureOutTaskType().  Currently just "MSMQ2Folder" or "Folder2MSMQ"
        public string SourceType = "";               // User-provided
        public string SourcePath = "";               // User-provided
        public string DestinationType = "";          // User-provided
        public string DestinationPath = "";          // User-provided
        public string DestinationName = "";          // Typically derived at runtime by PopulateName().
        public string DestinationNameTemplate = "";  // User-provided
        public string DestinationTransactional = ""; // User-provided, currently only used with MSMQ
        public string SourceEncoding = "";           // User-provided; either "Unicode" or "ASCII" and currently only available on MSMQ

        public const string Default_DestinationNameTemplate = "msg-[yyyy][MM][dd]-[HH][mm][ss]-[fff]-[n4].xml";

        /// <summary>
        /// Figures out if this is a supported type of task, and sets the task type appropriately
        /// </summary>
        public void FigureOutTaskType()
        {
            if (SourceType == "MSMQ" && DestinationType == "Folder")
            {
                TaskType = "MSMQ2Folder"; return;
            }
            if (DestinationType == "MSMQ" && SourceType == "Folder")
            {
                TaskType = "Folder2MSMQ"; return;
            }
            throw new NotSupportedException("You have requested an unsupported task type.");
        }

        /// <summary>
        /// Do some basic validation on task-wide elements (individual jobs are responsible for their own elements during runtime)
        /// </summary>
        public void PerformBasicValidation()
        {
            if (Name.Length == 0)
            {
                throw new ArgumentException("Blank task names are not allowed!");
            }
            if (CheckMS.Length == 0)
            {
                throw new ArgumentException("Blank task CheckMS values are not allowed!");
            }
            try
            {
                int iCheckMS = Int32.Parse(CheckMS);
            }
            catch (Exception)
            {
                throw new ArgumentException("CheckMS value must be a valid integer!");
            }
            if (SourcePath.Length == 0)
            {
                throw new ArgumentException("Blank task SourcePath values are not allowed!");
            }
            if (DestinationPath.Length == 0)
            {
                throw new ArgumentException("Blank task DestinationPath values are not allowed!");
            }
            if (DestinationNameTemplate.Length == 0) { 
                // Don't throw an error for a blank name template value - instead just use a default value
                DestinationNameTemplate = Default_DestinationNameTemplate;
            }
            if (SourceEncoding.Length == 0)
            {
                SourceEncoding = "Unicode";
            }
            if (SourceEncoding != "Unicode" && SourceEncoding != "ASCII")
            {
                throw new ArgumentException("SourceEncoding must be either Unicode or ASCII!");
            }
            if (DestinationTransactional != "" && DestinationTransactional != "1")
            {
                throw new ArgumentException("DestinationTransactional must be either blank or 1!");
            }
        
        }

        /// <summary>
        /// Converts the object's name template into a name using DateTime and positional arguments if necessary
        /// </summary>
        public void PopulateName(DateTime dt, int n, string sFilename)
        {
            /*
            [MM] = Month number, formatted in 2 digits
            [dd] = Day of month, formatted in 2 digits
            [HH] = Hours, formatted in 2 digits
            [mm] = Minutes, formatted in 2 digits
            [ss] = Seconds, formatted in 2 digits
            [fff] = Milliseconds, formatted in 3 digits
            [Task.Name] = Name of the task
            [Source.Name] = Original message label or file name picked up from the source
            [GUID] = GUID, e.g., 3f2504e0-4f89-41d3-9a0c-0305e82c3301 
            [n1..8] = Number of message/file in this run, formatted in 1 to 8 digits
            */
            String sName = DestinationNameTemplate;
            // Now figure out the supported elements
            // This is slow (i.e., should be replaced in a high-performance setting) but effective
            // We support a subset of http://msdn.microsoft.com/en-us/library/8kb3ddd4%28v=vs.110%29.aspx 
            sName = sName.Replace("[yyyy]", dt.ToString("yyyy"));
            sName = sName.Replace("[MM]", dt.ToString("MM"));
            sName = sName.Replace("[dd]", dt.ToString("dd"));
            sName = sName.Replace("[HH]", dt.ToString("HH"));
            sName = sName.Replace("[mm]", dt.ToString("mm"));
            sName = sName.Replace("[ss]", dt.ToString("ss"));
            sName = sName.Replace("[fff]", dt.ToString("fff"));
            // Task name
            sName = sName.Replace("[Task.Name]", Name);
            // File name
            sName = sName.Replace("[Source.Name]", sFilename);
            // GUIDs
            sName = sName.Replace("[GUID]", Guid.NewGuid().ToString());
            // Positional IDs up to 8 digits long
            // (there's a more elegant way of doing this, but this does what I want for now with little chance of mistake)
            sName = sName.Replace("[n]", n.ToString());
            sName = sName.Replace("[n1]", n.ToString().PadLeft(1, '0')); 
            sName = sName.Replace("[n2]", n.ToString().PadLeft(2, '0')); 
            sName = sName.Replace("[n3]", n.ToString().PadLeft(3, '0')); 
            sName = sName.Replace("[n4]", n.ToString().PadLeft(4, '0')); 
            sName = sName.Replace("[n5]", n.ToString().PadLeft(5, '0')); 
            sName = sName.Replace("[n6]", n.ToString().PadLeft(6, '0')); 
            sName = sName.Replace("[n7]", n.ToString().PadLeft(7, '0'));
            sName = sName.Replace("[n8]", n.ToString().PadLeft(8, '0'));
            DestinationName = sName;
        }

        /// <summary>
        /// Dump the contents of the object as a nicely formatted string
        /// </summary>
        /// <returns>Returns a string ideal for dumping into a human-readable debug file or console.</returns>
        public string DebugDump()
        {
            return DebugDump("  ");
        }
        public string DebugDump(string p)
        {
            return 
                p + "Name=" + Name + "\n" +
                p + "CheckMS=" + CheckMS + "\n" +
                p + "TaskType=" + TaskType + "\n" +
                p + "SourceType=" + SourceType + "\n" +
                p + "SourcePath=" + SourcePath + "\n" +
                p + "SourceEncoding=" + SourceEncoding + "\n" +
                p + "DestinationType=" + DestinationType + "\n" +
                p + "DestinationPath=" + DestinationPath + "\n" +
                p + "DestinationNameTemplate=" + DestinationNameTemplate + "\n" +
                p + "DestinationName=" + DestinationName + "\n" +
                p + "DestinationTransactional=" + DestinationTransactional + "";
            // Last line should not have a closing newline
        }
    }
}
