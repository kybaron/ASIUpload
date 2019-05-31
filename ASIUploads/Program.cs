using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using WinSCP;
using System.Net.Mail;

// C:\ASIUploads\Files\

namespace ASIUploads
{
    class Program
    {
        const string CSV_DEMOGRAPHIC_FILE = @"C:\ASIUploads\Files\UCWV_DEMOGRAPHICS_DATA.txt";  // 1st row will be column header names, and we need "|" as delimiter.
        const string CSV_SCHEDULE_FILE = @"C:\ASIUploads\Files\UCWV_SCHEDULES_DATA.txt";
        const string CONNECTION_STRING = @"Data Source=172.16.237.5;Initial Catalog=ODS_02prod;Integrated Security=False;User Id=ODS_02;Password=*%#^jcas@safasfHFFGYjh23r32r3;MultipleActiveResultSets=True";
        const string DEST_EXPORT_FILE_LOC = @"/ucwv/prod/in/";

        
        const string DEST_EXPORT_IMAGE_LOC = @"/ucwv/prod/in/studentphotos/";
        const string IMAGE_FILE_SOURCE = @"C:\CS Images\*.jpg";

        public static string strSourceName = null;

        public static int Transfer(string strSource, string strDestination)
        {


            try
            {
                // Setup session options
                SessionOptions sessionOptions = new SessionOptions
                {
                    Protocol = Protocol.Sftp,
                    HostName = "data-03.datacenter.adirondacksolutions.com",
                    UserName = "ucwv",
                    Password = "3hU42yEz9XBX",
                    SshHostKeyFingerprint = "ssh-rsa 2048 7e:ed:6d:e7:76:f9:d0:f8:96:7e:01:d8:f0:73:29:7a"
                };

                using (Session session = new Session())
                {

                    // Will continuously report progress of transfer
                    session.FileTransferProgress += SessionFileTransferProgress;


                    // Connect
                    session.Open(sessionOptions);

                    // Upload files
                    TransferOptions transferOptions = new TransferOptions();
                    transferOptions.TransferMode = TransferMode.Binary;

                    TransferOperationResult transferResult;
                    transferResult = session.PutFiles(@strSource, strDestination, false, transferOptions);

                    // Throw on any error
                    transferResult.Check();

                }



               strSourceName = strSource.Split('\\').Last();

               if (strSourceName == "*.jpg")
                    { 
                    strSourceName = "CS Images"; 
                    }


                SendErrNotification("SUCCESS - ASI " + strSourceName + " upload have completed", "Success!! " + strSourceName + " transferred to ASI");
                return 0;

            }
            catch (Exception e)
            {

                SendErrNotification("ERRORS - Uploading " + strSourceName + " to ASI.", e.Message);

                return 1;
            }
      
            
        
        }



        private static void SessionFileTransferProgress(object sender, FileTransferProgressEventArgs e)
        {

            Console.Write("\r{0} ({1:P0})                  ", e.FileName, e.FileProgress);
        }




        public static void SendErrNotification(string strNotificationSubject, string strNotificationBody)
        {

            MailMessage mm = new MailMessage();

            mm.To.Add("alanclark@ucwv.edu");
           // mm.To.Add("brianbaum@ucwv.edu");
           // mm.To.Add("ryanwhite@ucwv.edu");

            mm.From = new MailAddress("uccredentials@ucwv.edu", "UC Credentials");
            mm.Subject = strNotificationSubject;
            mm.IsBodyHtml = true;
            mm.Body = strNotificationBody;

            SmtpClient smtpclnt = new SmtpClient("pod51038.outlook.com");
            smtpclnt.Credentials = new System.Net.NetworkCredential("uccredentials@ucwv.edu", "!23qweASDzxc");
            smtpclnt.Port = 587;
            smtpclnt.EnableSsl = true;
            smtpclnt.DeliveryMethod = SmtpDeliveryMethod.Network;
            smtpclnt.Timeout = 20000;
            smtpclnt.Send(mm);

        }


            
        static void ProcessSQL(string strStoredProc, string strFilename) 
                       
        {

            SqlConnection sqlConnection1 = new SqlConnection(CONNECTION_STRING);
            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader = null;

            cmd.CommandText = strStoredProc;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Connection = sqlConnection1;
            cmd.CommandTimeout = 300;

            try
            {

                // Delete the file if it exists.
                if (File.Exists(strFilename))
                {
                    // Note that no lock is put on the
                    // file and the possibility exists
                    // that another process could do
                    // something with it between
                    // the calls to Exists and Delete.
                    File.Delete(strFilename);
                }

                sqlConnection1.Open();

                // Data is accessible through the DataReader object.

                reader = cmd.ExecuteReader();

                using (reader)
                {
                    

                    using (StreamWriter csvFile = new StreamWriter(File.Create(strFilename)))
                    {
                        StringBuilder outputLine = new StringBuilder();
                        DataTable schema = reader.GetSchemaTable();
                        List<int> ordinals = new List<int>();

                        foreach (DataRow row in schema.Rows)
                        {
                            // Append column name to outputLine
                            outputLine.AppendFormat("{0}|", row["ColumnName"]);

                            // Add this column's ordinal to the List<int> of ordinals
                            ordinals.Add((int)row["ColumnOrdinal"]);

                        }


                        // Write header row to CSV
                      
                        csvFile.WriteLine(outputLine.ToString().TrimEnd('|'));


                        // Read each record in the SqlDataReader
                        while (reader.Read())
                        {
                            // Clear the outputLine StringBuilder, start fresh for each record
                            outputLine.Length = 0;

                            // Loop through the list of column numbers,
                            // getting the value of each column number for this record
                            foreach (int ordinal in ordinals)
                            {
                                // Append the value to outputLine enclosed in quotes
                                outputLine.AppendFormat("{0}|", reader[ordinal]);
                            }

                          
                            csvFile.WriteLine(outputLine.ToString().TrimEnd('|'));



                        }





                    }


                }


            } // end try


            catch (Exception ex)
            {
                Console.Write(ex.Message);
            }




            finally
            {
                if (reader != null)
                    reader.Close();
                    sqlConnection1.Close();
            }
        
        
      }




        static void Main(string[] args)
        {
            // Process & transfer via SFTP Demographic File
            ProcessSQL("ASI_DEMOGRAPHICS_UPLOAD", CSV_DEMOGRAPHIC_FILE);
            Transfer(CSV_DEMOGRAPHIC_FILE, DEST_EXPORT_FILE_LOC);

            // Process & transfer via SFTP Schedule File
            ProcessSQL("ASI_SCHEDULES_UPLOAD", CSV_SCHEDULE_FILE);
            Transfer(CSV_SCHEDULE_FILE, DEST_EXPORT_FILE_LOC);

            // Transfer all Image Files
            Transfer(IMAGE_FILE_SOURCE, DEST_EXPORT_IMAGE_LOC);
        }



        }
    }

