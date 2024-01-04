using System;
using System.Data;
using System.Data.SqlClient;
using ClosedXML.Excel;

class Program
{
    static void Main()
    {
        // Update the connection string with your SQL Server details
        string connectionString = "Data Source=YourServer;Initial Catalog=YourDatabase;Integrated Security=True";

        // Excel file path
        string excelFilePath = "path\\to\\your\\file.xlsx";

        // SQL Server staging table name
        string stagingTableName = "YourStagingTable";

        try
        {
            // Read data from Excel file
            DataTable excelData = ReadExcelFile(excelFilePath);

            // Insert data into SQL Server staging table
            InsertIntoSqlStagingTable(connectionString, excelData, stagingTableName);

            Console.WriteLine("Data successfully loaded into the staging table.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
        }
    }

    static DataTable ReadExcelFile(string filePath)
    {
        using (var workbook = new XLWorkbook(filePath))
        {
            var ws = workbook.Worksheet(1); // Assuming data is on the first worksheet

            // Extract data into a DataTable
            var dataTable = new DataTable();

            // Assuming the first row contains column headers
            foreach (var cell in ws.FirstRow().CellsUsed())
            {
                dataTable.Columns.Add(cell.Value.ToString());
            }

            // Populate data rows
            foreach (var row in ws.RowsUsed().Skip(1)) // Skipping header row
            {
                dataTable.Rows.Add(row.Cells().Select(cell => cell.Value.ToString()).ToArray());
            }

            return dataTable;
        }
    }

    static void InsertIntoSqlStagingTable(string connectionString, DataTable data, string tableName)
    {
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            // Create a SQL Server staging table if it doesn't exist
            using (var command = new SqlCommand($"CREATE TABLE IF NOT EXISTS {tableName} (" +
                                                string.Join(", ", data.Columns.Cast<DataColumn>().Select(c => $"{c.ColumnName} NVARCHAR(MAX)")) +
                                                ")", connection))
            {
                command.ExecuteNonQuery();
            }

            // Bulk insert data into the staging table
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = tableName;
                foreach (DataColumn column in data.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }
                bulkCopy.WriteToServer(data);
            }
        }
    }
}
