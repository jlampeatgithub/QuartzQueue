QuartzQueue uses the Quartz.NET scheduling engine to power a folder-to-MSMQ file exchange service. 

QuartzQueue handles several Unicode/ASCII and transaction issues and includes macros for custom file names and message labels. 

QuartzQueue is configured in XML, runs as a command-line app or a Windows Service (thanks to Topshelf), logs to console, file, database and more (thanks to Log4Net).

QuartzQueue is written in C# and was originally a custom application for a File Transfer Consulting (http://www.filetransferconsulting.com) client looking for a quick and easy way to tie two software packages together (one could communicate through a transactional MSMQ queue; the other knew how to import/export XML files from disk).