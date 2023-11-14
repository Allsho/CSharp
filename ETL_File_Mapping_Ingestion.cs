using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Security;

namespace FileIngestion
{
    class DataMapping
    {
        public string IncomingColumnName { get; set; }
        public string StagingTableName { get; set; }
        public string ExpectedDelimiter { get; set; }
        public bool IsRequired { get; set; }
    }

    class Program
    {
        static string connectionString = Environment.GetEnvironmentVariable("DB_CONNECTION_STRING");
        static string emailSender = Environment.GetEnvironmentVariable("EMAIL_SENDER");
        static string emailRecipient = Environment.GetEnvironmentVariable("EMAIL_RECIPIENT");
        static SecureString emailPassword = ConvertToSecureString(Environment.GetEnvironmentVariable("EMAIL_PASSWORD"));
        static string folderPath = "your_folder_path"; // Update this path as needed

        static void Main(string[] args)
        {
            string[] fileEntries = Directory.GetFiles(folderPath, "*.csv").Union(Directory.GetFiles(folderPath, "*.txt")).ToArray();

            List<DataMapping> dataMappings = LoadDataMappings();

            foreach (string filePath in fileEntries)
            {
                ProcessFile(filePath, dataMappings);
            }
        }

        static void ProcessFile(string filePath, List<DataMapping> dataMappings)
        {
            string fileName = Path.GetFileNameWithoutExtension(filePath);
            string fileDate = fileName.Substring(fileName.Length - 8); // Assuming MMDDYYYY format

            List<string> unmatchedColumns = new List<string>();

            try
            {
                using (StreamReader reader = new StreamReader(filePath))
                {
                    string headerLine = reader.ReadLine();
                    if (headerLine == null)
                    {
                        // Handle empty file
                        return;
                    }

                    string[] headerColumns = headerLine.Split(',');

                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        connection.Open();

                        Dictionary<string, string> columnMapping = MapColumns(headerColumns, dataMappings, unmatchedColumns);

                        while (!reader.EndOfStream)
                        {
                            string dataLine = reader.ReadLine();
                            string[] dataColumns = dataLine.Split(',');

                            InsertDataIntoStagingTable(connection, columnMapping, headerColumns, dataColumns);
                        }
                    }
                }
            }
            catch (IOException ioEx)
            {
                LogError($"File Error: {ioEx.Message}");
            }
            catch (Exception ex)
            {
                LogError($"Error: {ex.Message}");
            }

            if (unmatchedColumns.Count > 0)
            {
                SendEmailWithUnmatchedColumns(unmatchedColumns);
            }
        }

        static Dictionary<string, string> MapColumns(string[] headerColumns, List<DataMapping> dataMappings, List<string> unmatchedColumns)
        {
            Dictionary<string, string> columnMapping = new Dictionary<string, string>();

            foreach (var dataMapping in dataMappings)
            {
                string incomingColumnName = dataMapping.IncomingColumnName;
                string stagingTableName = dataMapping.StagingTableName;
                bool isRequired = dataMapping.IsRequired;

                if (headerColumns.Contains(incomingColumnName, StringComparer.OrdinalIgnoreCase))
                {
                    // Map incoming column to staging column
                    columnMapping.Add(incomingColumnName, stagingTableName);
                }
                else if (isRequired)
                {
                    // Handle a required column missing in the incoming data
                    unmatchedColumns.Add(incomingColumnName);
                }
                // If the column is not required and missing, continue with the processing
            }

            return columnMapping;
        }

        static void InsertDataIntoStagingTable(SqlConnection connection, Dictionary<string, string> columnMapping, string[] headerColumns, string[] dataColumns)
        {
            using (SqlCommand insertCommand = new SqlCommand())
            {
                insertCommand.Connection = connection;
                foreach (var kvp in columnMapping)
                {
                    string incomingColumn = kvp.Key;
                    string stagingColumn = kvp.Value;

                    if (headerColumns.Contains(incomingColumn, StringComparer.OrdinalIgnoreCase))
                    {
                        int columnIndex = Array.IndexOf(headerColumns, incomingColumn);
                        if (columnIndex >= 0 && columnIndex < dataColumns.Length)
                        {
                            string columnValue = dataColumns[columnIndex];
                            insertCommand.Parameters.AddWithValue($"@{stagingColumn}", columnValue);
                        }
                    }
                }

                insertCommand.CommandText = $"INSERT INTO {string.Join(", ", columnMapping.Values)} " +
                                        $"VALUES ({string.Join(", ", columnMapping.Keys.Select(key => $"@{key}"))})";
                insertCommand.ExecuteNonQuery();
                insertCommand.Parameters.Clear();
            }
        }

        static List<DataMapping> LoadDataMappings()
        {
            List<DataMapping> dataMappings = new List<DataMapping>();

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                using (SqlCommand command = new SqlCommand("SELECT IncomingColumnName, StagingTableName, ExpectedDelimiter, IsRequired FROM Data_Mapping", connection))
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        DataMapping dataMapping = new DataMapping
                        {
                            IncomingColumnName = reader.GetString(0),
                            StagingTableName = reader.GetString(1),
                            ExpectedDelimiter = reader.GetString(2),
                            IsRequired = reader.GetBoolean(3)
                        };

                        dataMappings.Add(dataMapping);
                    }
                }
            }

            return dataMappings;
        }

        static void SendEmailWithUnmatchedColumns(List<string> unmatchedColumns)
        {
            try
            {
                using (SmtpClient smtpClient = new SmtpClient("smtp.gmail.com"))
                {
                    smtpClient.UseDefaultCredentials = false;
                    smtpClient.Credentials = new NetworkCredential(emailSender, emailPassword);
                    smtpClient.EnableSsl = true;
                    smtpClient.Port = 587;

                    using (MailMessage mailMessage = new MailMessage(emailSender, emailRecipient))
                    {
                        mailMessage.Subject = "Unmatched Columns in Incoming File";
                        mailMessage.Body = "The following columns were not found in the data mapping table:\n\n";
                        foreach (var column in unmatchedColumns)
                        {
                            mailMessage.Body += $"{column}\n";
                        }

                        smtpClient.Send(mailMessage);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"Error sending email: {ex.Message}");
            }
        }

        static void LogError(string errorMessage)
        {
            // You can implement a logging mechanism here, e.g., write to a log file.
            Console.WriteLine(errorMessage);
        }
    }
}
