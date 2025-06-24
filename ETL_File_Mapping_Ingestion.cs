public void Main()
{
    string connStr = Dts.Variables["User::CM_OLEDB_ClaimsStage"].Value.ToString();

    try
    {
        Log(connStr, "ETL Process Started");
        List<TableMapping> mappings = GetTableMappings(connStr);

        foreach (var mapping in mappings)
        {
            List<ColumnMapping> columnMappings = GetColumnMappings(connStr, mapping.TargetTable);
            ProcessFiles(connStr, mapping, columnMappings);
        }

        Log(connStr, "ETL Process Completed");
    }
    catch (Exception ex)
    {
        LogError(connStr, "General Error", ex.Message);
        Dts.TaskResult = (int)ScriptResults.Failure;
    }

    Dts.TaskResult = (int)ScriptResults.Success;
}

public class TableMapping
{
    public string TargetTable;
    public string FilePattern;
    public string FileType;
    public string SourcePath;
    public string ArchivePath;
    public string Delimiter;
}

public class ColumnMapping
{
    public string IncomingColumn;
    public string TargetColumn;
}

public List<TableMapping> GetTableMappings(string connStr)
{
    var mappings = new List<TableMapping>();

    string idList = Dts.Variables["User::FilteredMappingIDs"].Value.ToString(); 

    using (SqlConnection conn = new SqlConnection(connStr))
    {
        conn.Open();
        using (SqlCommand cmd = new SqlCommand("ETL.usp_Get_Table_Mappings_ByIds", conn))
        {
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@MappingIds", idList);

            using (SqlDataReader rdr = cmd.ExecuteReader())
            {
                while (rdr.Read())
                {
                    mappings.Add(new TableMapping
                    {
                        TargetTable = rdr["TargetTable"].ToString(),
                        FilePattern = rdr["FilePattern"].ToString(),
                        FileType = rdr["FileType"].ToString(),
                        SourcePath = rdr["SourcePath"].ToString(),
                        ArchivePath = rdr["ArchivePath"].ToString(),
                        Delimiter = rdr["Delimiter"].ToString()
                    });
                }
            }
        }
    }

    return mappings;
}

public List<ColumnMapping> GetColumnMappings(string connStr, string targetTable)
{
    List<ColumnMapping> list = new List<ColumnMapping>();

    using (SqlConnection conn = new SqlConnection(connStr))
    {
        conn.Open();
        string sql = @"SELECT cdm.IncomingColumnName AS IncomingColumn, cdm.StandardizedColumnName AS TargetColumn
               FROM ETL.Claim_Data_Mapping cdm
               JOIN ETL.Table_Mapping tm ON cdm.PayorName = tm.PayorName AND cdm.IncomingColumnName IS NOT NULL
               WHERE tm.TargetTable = @TargetTable";
        using (SqlCommand cmd = new SqlCommand(sql, conn))
        {
            cmd.Parameters.AddWithValue("@TargetTable", targetTable);
            using (SqlDataReader rdr = cmd.ExecuteReader())
            {
                while (rdr.Read())
                {
                    list.Add(new ColumnMapping
                    {
                        IncomingColumn = rdr["IncomingColumn"].ToString(),
                        TargetColumn = rdr["TargetColumn"].ToString()
                    });
                }
            }
        }
    }

    return list;
}

public void ProcessFiles(string connStr, TableMapping mapping, List<ColumnMapping> colMappings)
{
    string[] files = Directory.GetFiles(mapping.SourcePath, mapping.FilePattern);
    foreach (string file in files)
    {
        try
        {
            Log(connStr, $"Processing file: {file}");
            DataTable data = ReadCsv(file, mapping.Delimiter);

            // Add SourceFileName column if not exists
            if (!data.Columns.Contains("SourceFileName"))
                data.Columns.Add("SourceFileName", typeof(string));

            // Fill that column for all rows
            foreach (DataRow row in data.Rows)
            {
                row["SourceFileName"] = Path.GetFileName(file);
            }

            MapColumns(data, colMappings);
            TruncateTargetTable(mapping.TargetTable, connStr);
            BulkInsert(data, mapping.TargetTable, connStr);
            ArchiveFile(file, mapping.ArchivePath);
            Log(connStr, $"Processed and archived: {file}");
        }
        catch (Exception ex)
        {
            LogError(connStr, $"Processing error: {file}", ex.Message);
        }
    }
}

private DataTable ReadCsv(string filePath, string delimiter)
{
    DataTable dt = new DataTable();
    string sourceFileName = Path.GetFileName(filePath);

    using (StreamReader sr = new StreamReader(filePath))
    {
        string headerLine = sr.ReadLine();
        if (headerLine == null)
            throw new Exception("CSV file is empty.");

        // Check if this is a single-column quoted file (no delimiter present)
        bool isQuotedSingleColumn = !headerLine.Contains(delimiter) && headerLine.StartsWith("\"") && headerLine.EndsWith("\"");

        if (isQuotedSingleColumn)
        {
            string header = headerLine.Trim().Trim('"');
            dt.Columns.Add(header);
            dt.Columns.Add("SourceFileName");

            while (!sr.EndOfStream)
            {
                string line = sr.ReadLine()?.Trim().Trim('"');
                if (!string.IsNullOrWhiteSpace(line))
                {
                    DataRow row = dt.NewRow();
                    row[header] = line;
                    row["SourceFileName"] = sourceFileName;
                    dt.Rows.Add(row);
                }
            }
        }
        else
        {
            // Normal delimited CSV
            string[] headers = headerLine.Split(delimiter.ToCharArray());
            foreach (string header in headers)
                dt.Columns.Add(header.Trim('"'));

            dt.Columns.Add("SourceFileName");

            while (!sr.EndOfStream)
            {
                string[] values = sr.ReadLine()?.Split(delimiter.ToCharArray());

                if (values != null && values.Length > 0)
                {
                    DataRow row = dt.NewRow();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        row[i] = values[i].Trim('"');
                    }
                    row["SourceFileName"] = sourceFileName;
                    dt.Rows.Add(row);
                }
            }
        }
    }

    return dt;
}

public void MapColumns(DataTable dt, List<ColumnMapping> mappings)
{
    foreach (var map in mappings)
    {
        if (dt.Columns.Contains(map.IncomingColumn))
            dt.Columns[map.IncomingColumn].ColumnName = map.TargetColumn;
    }
}

private void TruncateTargetTable(string tableName, string connStr)
{
    using (SqlConnection conn = new SqlConnection(connStr))
    {
        conn.Open();
        using (SqlCommand cmd = new SqlCommand($"TRUNCATE TABLE {tableName}", conn))
        {
            cmd.ExecuteNonQuery();
            Log(connStr, $"Truncated table: {tableName}");
        }
    }
}

public void BulkInsert(DataTable dt, string tableName, string connStr)
{
    using (SqlConnection conn = new SqlConnection(connStr))
    {
        conn.Open();
        using (SqlBulkCopy bulk = new SqlBulkCopy(conn))
        {
            bulk.DestinationTableName = tableName;

            // Explicit column mapping to avoid ordinal mismatch
            foreach (DataColumn col in dt.Columns)
            {
                bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                Log(connStr, $"DataTable Column: {col.ColumnName}");
            }

            bulk.WriteToServer(dt);
        }
    }
}

public void ArchiveFile(string filePath, string archivePath)
{
    string destPath = Path.Combine(archivePath, Path.GetFileName(filePath));
    File.Move(filePath, destPath);
}

public void Log(string connStr, string message)
{
    using (SqlConnection conn = new SqlConnection(connStr))
    {
        conn.Open();
        using (SqlCommand cmd = new SqlCommand("INSERT INTO ETL.Claims_Log (LogTimestamp, Message) VALUES (@ts, @msg)", conn))
        {
            cmd.Parameters.AddWithValue("@ts", DateTime.Now);
            cmd.Parameters.AddWithValue("@msg", message);
            cmd.ExecuteNonQuery();
        }
    }
}

public void LogError(string connStr, string errorType, string message)
{
    using (SqlConnection conn = new SqlConnection(connStr))
    {
        conn.Open();
        using (SqlCommand cmd = new SqlCommand("INSERT INTO ETL.Claims_Error_Log (LogTimestamp, Message) VALUES (@ts, @msg)", conn))
        {
            cmd.Parameters.AddWithValue("@ts", DateTime.Now);
            cmd.Parameters.AddWithValue("@msg", message);
            cmd.ExecuteNonQuery();
        }
    }
}
