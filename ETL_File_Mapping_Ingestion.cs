public void Main()
{
    string connStr = Dts.Variables["User::CM_OLEDB_ClaimsStage"].Value.ToString();

    try
    {
        Log(connStr, "ETL Process Started");
        List<TableMapping> mappings = GetTableMappings(connStr);

        using (SqlConnection conn = new SqlConnection(connStr))
        {
            conn.Open();
            foreach (var mapping in mappings)
            {
                List<ColumnMapping> columnMappings = GetColumnMappings(conn, mapping.TargetTable);
                ProcessFiles(conn, mapping, columnMappings);
            }
        }

        Log(connStr, "ETL Process Completed");
    }
    catch (Exception ex)
    {
        LogError(connStr, "General Error", ex.Message);
        Dts.TaskResult = (int)ScriptResults.Failure;
        return;
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
    using (SqlCommand cmd = new SqlCommand("ETL.usp_Get_Table_Mappings_ByIds", conn))
    {
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.Parameters.AddWithValue("@MappingIds", idList);
        conn.Open();

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

    return mappings;
}

public List<ColumnMapping> GetColumnMappings(SqlConnection conn, string targetTable)
{
    List<ColumnMapping> list = new List<ColumnMapping>();

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

    return list;
}

public void ProcessFiles(SqlConnection conn, TableMapping mapping, List<ColumnMapping> colMappings)
{
    string[] files = Directory.GetFiles(mapping.SourcePath, mapping.FilePattern);
    foreach (string file in files)
    {
        try
        {
            DataTable data = ReadCsv(file, mapping.Delimiter);

            if (!data.Columns.Contains("SourceFileName"))
                data.Columns.Add("SourceFileName", typeof(string));

            foreach (DataRow row in data.Rows)
                row["SourceFileName"] = Path.GetFileName(file);

            MapColumns(data, colMappings);
            TruncateTargetTable(mapping.TargetTable, conn);
            BulkInsert(data, mapping.TargetTable, conn);
            ArchiveFile(file, mapping.ArchivePath);

            Log(conn.ConnectionString, $"? Processed and archived: {file}");
        }
        catch (Exception ex)
        {
            LogError(conn.ConnectionString, $"? Processing error: {file}", ex.Message);
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
        if (headerLine == null) throw new Exception("CSV file is empty.");

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
                        row[i] = (i < values.Length) ? values[i].Trim('"') : "";

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
    var dtColumns = new HashSet<string>(dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName), StringComparer.OrdinalIgnoreCase);
    var mappedIncoming = new HashSet<string>(mappings.Select(m => m.IncomingColumn), StringComparer.OrdinalIgnoreCase);

    foreach (var map in mappings)
    {
        if (dt.Columns.Contains(map.IncomingColumn))
            dt.Columns[map.IncomingColumn].ColumnName = map.TargetColumn;
    }
}

private void TruncateTargetTable(string tableName, SqlConnection conn)
{
    using (SqlCommand cmd = new SqlCommand($"TRUNCATE TABLE {tableName}", conn))
        cmd.ExecuteNonQuery();
}

public void LogSchemaDifferences(DataTable dt, string tableName, string sourceFileName, SqlConnection conn)
{
    var destColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    using (SqlCommand cmd = new SqlCommand($"SELECT TOP 0 * FROM {tableName}", conn))
    using (SqlDataReader reader = cmd.ExecuteReader())
        for (int i = 0; i < reader.FieldCount; i++)
            destColumns.Add(reader.GetName(i));

    foreach (DataColumn col in dt.Columns)
    {
        bool existsInDb = destColumns.Contains(col.ColumnName);
        string alterStmt = existsInDb ? null : $"ALTER TABLE {tableName} ADD [{col.ColumnName}] NVARCHAR(255);";

        using (SqlCommand logCmd = new SqlCommand("ETL.usp_Log_Claim_File_Schema_History", conn))
        {
            logCmd.CommandType = CommandType.StoredProcedure;
            logCmd.Parameters.AddWithValue("@TableName", tableName);
            logCmd.Parameters.AddWithValue("@ColumnName", col.ColumnName);
            logCmd.Parameters.AddWithValue("@ColumnDetected", 1);
            logCmd.Parameters.AddWithValue("@ColumnMapped", existsInDb ? 1 : 0);
            logCmd.Parameters.AddWithValue("@SuggestedAlterStatement", (object)alterStmt ?? DBNull.Value);
            logCmd.Parameters.AddWithValue("@SourceFileName", sourceFileName);
            logCmd.ExecuteNonQuery();
        }
    }
}

public void BulkInsert(DataTable dt, string tableName, SqlConnection conn)
{
    LogSchemaDifferences(dt, tableName, dt.Rows[0]["SourceFileName"].ToString(), conn);

    var destColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    using (SqlCommand cmd = new SqlCommand($"SELECT TOP 0 * FROM {tableName}", conn))
    using (SqlDataReader reader = cmd.ExecuteReader())
        for (int i = 0; i < reader.FieldCount; i++)
            destColumns.Add(reader.GetName(i));

    using (SqlBulkCopy bulk = new SqlBulkCopy(conn))
    {
        bulk.BulkCopyTimeout = 300; // Seconds
        bulk.DestinationTableName = tableName;
        foreach (DataColumn col in dt.Columns)
        {
            if (destColumns.Contains(col.ColumnName))
                bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
        }
        bulk.WriteToServer(dt);
    }
}

public void ArchiveFile(string filePath, string archivePath)
{
    string yearMonth = DateTime.Now.ToString("yyyyMM");
    string subfolder = Path.Combine(archivePath, yearMonth);

    if (!Directory.Exists(subfolder))
        Directory.CreateDirectory(subfolder);

    string destPath = Path.Combine(subfolder, Path.GetFileName(filePath));
    File.Move(filePath, destPath);
}

public void Log(string connStr, string message)
{
    using (SqlConnection conn = new SqlConnection(connStr))
    using (SqlCommand cmd = new SqlCommand("INSERT INTO ETL.Claims_Log (LogTimestamp, Message) VALUES (@ts, @msg)", conn))
    {
        cmd.Parameters.AddWithValue("@ts", DateTime.Now);
        cmd.Parameters.AddWithValue("@msg", message);
        conn.Open();
        cmd.ExecuteNonQuery();
    }
}

public void LogError(string connStr, string errorType, string message)
{
    using (SqlConnection conn = new SqlConnection(connStr))
    using (SqlCommand cmd = new SqlCommand("INSERT INTO ETL.Claims_Error_Log (LogTimestamp, Message) VALUES (@ts, @msg)", conn))
    {
        cmd.Parameters.AddWithValue("@ts", DateTime.Now);
        cmd.Parameters.AddWithValue("@msg", message);
        conn.Open();
        cmd.ExecuteNonQuery();
    }
}
