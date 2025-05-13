using System;
using System.Data;
using System.IO;
using System.Linq;
using Microsoft.Data.SqlClient;
using OfficeOpenXml;
using System.Collections.Generic;
using Microsoft.SqlServer.Dts.Runtime;

public void Main()
{
    string connectionString = "Server=;Database=ClaimsStage;Integrated Security=True;";

    try
    {
        Log("ETL Process Started", connectionString);
        List<TableMapping> mappings = GetTableMappings(connectionString);

        foreach (var mapping in mappings)
        {
            List<ColumnMapping> columnMappings = GetColumnMappings(mapping.TargetTable, connectionString);
            ProcessFiles(mapping, columnMappings, connectionString);
        }

        Log("ETL Process Completed", connectionString);
        Dts.TaskResult = (int)ScriptResults.Success;
    }
    catch (Exception ex)
    {
        LogError("General Error", ex.Message, connectionString);
        Dts.TaskResult = (int)ScriptResults.Failure;
    }
}

public class TableMapping
{
    public string FilePattern { get; set; }
    public string TargetTable { get; set; }
    public string SheetName { get; set; }
    public string FileType { get; set; }
    public string SourcePath { get; set; }
    public string ArchivePath { get; set; }
    public string Delimiter { get; set; }
}

public class ColumnMapping
{
    public string IncomingColumn { get; set; }
    public string TargetColumn { get; set; }
}

private List<TableMapping> GetTableMappings(string connectionString)
{
    var mappings = new List<TableMapping>();
    using (SqlConnection conn = new SqlConnection(connectionString))
    {
        conn.Open();
        string query = @"SELECT TargetTable, FilePattern, SheetName, FileType, SourcePath, ArchivePath, Delimiter FROM ETL.Table_Mapping";
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

private List<ColumnMapping> GetColumnMappings(string targetTable, string connectionString)
{
    var columnMappings = new List<ColumnMapping>();
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

private void ProcessFiles(TableMapping mapping, List<ColumnMapping> columnMappings, string connectionString)
{
    string[] files = Directory.GetFiles(mapping.SourcePath, mapping.FilePattern);
    foreach (var file in files)
    {
        try
        {
            Log($"Processing: {file}", connectionString);
            DataTable data = mapping.FileType.ToLower() == "excel" ? ReadExcel(file, mapping.SheetName) : ReadCsv(file, mapping.Delimiter);
            MapColumns(data, columnMappings);
            BulkInsert(data, mapping.TargetTable, connectionString);
            ArchiveFile(file, mapping.ArchivePath, connectionString);
        }
        catch (Exception ex)
        {
            LogError($"Error processing {file}", ex.Message, connectionString);
        }
    }
}

private void MapColumns(DataTable data, List<ColumnMapping> columnMappings)
{
    foreach (var mapping in columnMappings)
    {
        if (data.Columns.Contains(mapping.IncomingColumn))
        {
            data.Columns[mapping.IncomingColumn].ColumnName = mapping.TargetColumn;
        }
    }
}

private DataTable ReadCsv(string filePath, string delimiter)
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

private DataTable ReadExcel(string filePath, string sheetName)
{
    DataTable dt = new DataTable();
    ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
    using (ExcelPackage package = new ExcelPackage(new FileInfo(filePath)))
    {
        ExcelWorksheet worksheet = package.Workbook.Worksheets.FirstOrDefault(ws => ws.Name == sheetName);
        if (worksheet == null)
            throw new Exception($"Sheet '{sheetName}' not found in file {filePath}");

        for (int col = 1; col <= worksheet.Dimension.Columns; col++)
            dt.Columns.Add(worksheet.Cells[1, col].Value?.ToString() ?? $"Column{col}");

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

private void BulkInsert(DataTable data, string tableName, string connectionString)
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

private void ArchiveFile(string filePath, string archivePath, string connectionString)
{
    string destPath = Path.Combine(archivePath, Path.GetFileName(filePath));
    File.Move(filePath, destPath);
    Log($"Archived: {filePath} -> {destPath}", connectionString);
}

private void Log(string message, string connectionString)
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

private void LogError(string errorType, string message, string connectionString)
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

enum ScriptResults
{
    Success = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success,
    Failure = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure
}
