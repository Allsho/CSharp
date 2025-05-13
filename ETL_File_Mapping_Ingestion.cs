using System;
using System.Data;
using System.IO;
using System.Linq;
using Microsoft.Data.SqlClient;
using OfficeOpenXml;
using System.Collections.Generic;
using System.Reflection.Metadata.Ecma335;
using Microsoft.Extensions.FileSystemGlobbing;

class Program
{
    static string connectionString = "Server=;Database=ClaimsStage;Integrated Security=True;";

    static void Main()
    {
        try
        {
            Log("ETL Process Started");
            List<TableMapping> mappings = GetTableMappings();

            foreach (var mapping in mappings)
            {
                List<ColumnMapping> columnMappings = GetColumnMappings(mapping.TargetTable);
                ProcessFiles(mapping, columnMappings);
            }

            Log("ETL Process Compeleted");
        }
        catch (Exception ex)
        {
            LogError("General Error", ex.Message);
        }
    }

    static List<TableMapping> GetTableMappings()
    {
        List<TableMapping> mappings = new List<TableMapping>();
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            conn.Open();
            string query = @"
                SELECT TargetTable, FilePattern, SheetName, FileType, SourcePath, ArchivePath, Delimiter
                FROM ETL.Table_Mapping";
            using (SqlCommand cmd = new SqlCommand(query, conn))
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    mappings.Add(new TableMapping
                    {
                        TargetTable = reader["TargetTable"].ToString(),
                        FilePattern = reader["FilePattern"].ToString(),
                        SheetName = reader["SheetName"].ToString(),
                        FileType = reader["FileType"].ToString(),
                        SourcePath = reader["SourcePath"].ToString(),
                        ArchivePath = reader["ArchivePath"].ToString(),
                        Delimiter = reader["Delimiter"].ToString()
                    });
                }
            }
        }
        return mappings;
    }

    static List<ColumnMapping> GetColumnMappings(string targetTable)
    {

        List<ColumnMapping> columnMappings = new List<ColumnMapping>();
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            conn.Open();
            string query = @"
                SELECT cdm.IncomingColumnName AS IncomingColumn, cdm.StandardizedColumnName AS TargetColumn
                FROM ETL.Claim_Data_Mapping cdm
                JOIN ETL.Table_Mapping tm ON cdm.PayorName = tm.PayorName AND cdm.IncomingColumnName IS NOT NULL
                WHERE tm.TargetTable = @TargetTable";
            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@TargetTable", targetTable);
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        columnMappings.Add(new ColumnMapping
                        {
                            IncomingColumn = reader["IncomingColumn"].ToString(),
                            TargetColumn = reader["TargetColumn"].ToString()
                        });
                    }
                }
            }
        }
        return columnMappings;
    }

    static void ProcessFiles(TableMapping mapping, List<ColumnMapping> columnMappings)
    {
        string[] files = Directory.GetFiles(mapping.SourcePath, mapping.FilePattern);
        foreach (var file in files)
        {
            try
            {
                Log($"Processing: {file}");
                DataTable data = mapping.FileType.ToLower() == "excel" ? ReadExcel(file, mapping.SheetName) : ReadCsv(file, mapping.Delimiter);
                MapColumns(data, columnMappings);
                BulkInsert(data, mapping.TargetTable);
                ArchiveFile(file, mapping.ArchivePath);
            }
            catch (Exception ex)
            {
                LogError($"Error processing {file}", ex.Message);
            }
        }
    }

    static void MapColumns(DataTable data, List<ColumnMapping> columnMappings)
    {
        foreach (var mapping in columnMappings)
        {
            if (data.Columns.Contains(mapping.IncomingColumn))
            {
                data.Columns[mapping.IncomingColumn].ColumnName = mapping.TargetColumn;
            }
        }
    }

    static DataTable ReadCsv(string filePath, string delimiter)
    {
        DataTable dt = new DataTable();
        using (StreamReader sr = new StreamReader(filePath))
        {
            string[] headers = sr.ReadLine().Split(delimiter.ToCharArray());
            foreach (string header in headers)
                dt.Columns.Add(header);

        while (!sr.EndOfStream)
            {
                dt.Rows.Add(sr.ReadLine().Split(delimiter.ToCharArray()));
            }
        }
        return dt;
    }

    static DataTable ReadExcel(string filePath, string sheetName)
    {
        DataTable dt = new DataTable();
        using (ExcelPackage package = new ExcelPackage(new FileInfo(filePath)))
        {
            ExcelWorksheet worksheet = package.Workbook.Worksheets.FirstOrDefault(ws => ws.Name == sheetName);
            if (worksheet != null)
                throw new Exception($"Sheet '{sheetName}' not found in file {filePath}");

            for (int col = 1; col <= worksheet.Dimension.Columns; col++)
                dt.Columns.Add(worksheet.Cells[1, col].Value.ToString());

            for (int row = 2; row <= worksheet.Dimension.Rows; row++)
            {
                DataRow dr = dt.NewRow();
                for (int col = 1; col <= worksheet.Dimension.Columns; col++)
                    dr[col - 1] = worksheet.Cells[row, col].Value?.ToString() ?? "";
                dt.Rows.Add(dr);
            }
        }
        return dt;
    }

    static void BulkInsert(DataTable data, string tableName)
    {
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            conn.Open();
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.WriteToServer(data);
            }
        }
    }

    static void ArchiveFile(string filePath, string archivePath)
    {
        string destPath = Path.Combine(archivePath, Path.GetFileName(filePath));
        File.Move(filePath, destPath);
        Log($"Archived: {filePath} -> {destPath}");
    }

    static void Log(string message)
    {
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            conn.Open();
            using (SqlCommand cmd = new SqlCommand("INSERT INTO ETL_Log (Timestamp, Message) VALUES (@Timestamp, @Message)", conn))
            {
                cmd.Parameters.AddWithValue("@Timestamp", DateTime.Now);
                cmd.Parameters.AddWithValue("@Message", message);
                cmd.ExecuteNonQuery();
            }
        }
    }

    static void LogError(string errorType, string message)
    {
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            conn.Open();
            using (SqlCommand cmd = new SqlCommand("INSERT INTO ETL_ErrorLog (Timestamp, ErrorType, Message) VALUES (@Timestamp, @ErrorType, @Message)", conn))
            {
                cmd.Parameters.AddWithValue("@Timestamp", DateTime.Now);
                cmd.Parameters.AddWithValue("@ErrorType", errorType);
                cmd.Parameters.AddWithValue("@Message", message);
                cmd.ExecuteNonQuery();
            }
        }
    }
}

class TableMapping
{
    public string? FilePattern { get; set; }
    
    public string? TargetTable { get; set; }

    public string? SheetName { get; set; }

    public string? FileType { get; set; }

    public string? SourcePath { get; set; }

    public string? ArchivePath { get; set; }

    public string? Delimiter { get; set; }
}

class ColumnMapping
{
    public string? IncomingColumn { get; set; }
    
    public string? TargetColumn { get; set; }
}
