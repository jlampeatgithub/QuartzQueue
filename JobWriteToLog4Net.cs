using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Quartz;
using log4net;

namespace QuartzQueue
{
    class JobWriteToLog4Net : IJob
    {
        /// <summary>
        /// Runs an instance of this job
        /// </summary>
        public void Execute(IJobExecutionContext context)
        {
            int iSleep = 300;  // Time to sleep in milliseconds
            log.Fatal("log4net Fatal");
            System.Threading.Thread.Sleep(iSleep);
            log.Error("log4net Error");
            System.Threading.Thread.Sleep(iSleep);
            log.Warn("log4net Warn");
            System.Threading.Thread.Sleep(iSleep);
            log.Info("log4net Info");
            System.Threading.Thread.Sleep(iSleep);
            log.Debug("log4net Debug");
        }

        /// <summary>
        /// Recommended per-class reference to log4net (http://www.codeproject.com/Articles/140911/log4net-Tutorial)
        /// </summary>
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

    }
}
