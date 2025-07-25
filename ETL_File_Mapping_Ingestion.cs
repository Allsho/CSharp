public void Main()
{
    string connStr = Dts.Variables["User::CM_OLEDB_ClaimsStage"].Value.ToString();
    string basePath = Dts.Variables["User::prmRootPath"].Value.ToString();

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
                ProcessFiles(conn, mapping, columnMappings, basePath);
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
    //public string PostLoadProcedure;
    //Add Excel Info
    public string SheetName;
    public int HeaderRowIndex = 1; // Default to 1 (Excel rows are 1-based)
}

public class ColumnMapping
{
    public string IncomingColumn;
    public string TargetColumn;
    public bool IsRequired;
}

public List<TableMapping> GetTableMappings(string connStr)
{
    var mappings = new List<TableMapping>();

    try
    {
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
                        Delimiter = rdr["Delimiter"].ToString(),
                        //PostLoadProcedure = rdr["PostLoadProcedure"].ToString(),
                        SheetName = rdr["SheetName"].ToString(),
                        HeaderRowIndex = rdr["HeaderRowIndex"] != DBNull.Value ? Convert.ToInt32(rdr["HeaderRowIndex"]) : 1
                    });
                }
            }
        }
    }
    catch (Exception ex)
    {
        LogError(connStr, "Error in GetTableMappings", ex.Message);
    }

    return mappings;
}

public List<ColumnMapping> GetColumnMappings(SqlConnection conn, string targetTable)
{
    List<ColumnMapping> list = new List<ColumnMapping>();

    string sql = @"SELECT cdm.IncomingColumnName AS IncomingColumn, cdm.StandardizedColumnName AS TargetColumn, ISNULL(cdm.IsRequired, 0) AS IsRequired
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
                    TargetColumn = rdr["TargetColumn"].ToString(),
                    IsRequired = Convert.ToBoolean(rdr["IsRequired"])
                });
            }
        }
    }

    return list;
}

public void ProcessFiles(SqlConnection conn, TableMapping mapping, List<ColumnMapping> colMappings, string basePath)
{
    string fullSourcePath = Path.Combine(basePath, mapping.SourcePath);
    string[] files = Directory.GetFiles(fullSourcePath, mapping.FilePattern);
    foreach (string file in files)
    {
        try
        {
            DataTable data;

            if (mapping.FileType.ToLower().Contains("excel"))
            {
                data = ReadExcel(file, mapping, colMappings, conn.ConnectionString);
            }
            else
            {
                data = ReadCsv(file, mapping.Delimiter, colMappings, conn.ConnectionString);
            }

            MapColumns(data, colMappings, conn.ConnectionString);

            LogTruncationIssues(data, mapping.TargetTable, conn);
            TruncateTargetTable(mapping.TargetTable, conn);
            BulkInsert(data, mapping.TargetTable, conn);
            //RunPostLoadProcedure(conn, mapping.PostLoadProcedure);
            
            string fullArchivePath = Path.Combine(basePath, mapping.ArchivePath);
            //ArchiveFile(file, fullArchivePath);

            //Log(conn.ConnectionString, $"? Processed and archived: {file}");
        }
        catch (Exception ex)
        {
            LogError(conn.ConnectionString, $"? Processing error: {file}", ex.Message);
        }
    }
}

private DataTable ReadCsv(string filePath, string delimiter, List<ColumnMapping> colMappings, string connStr)
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

            // Check for required columns from mapping (commented out for now)
            
            var requiredCols = colMappings
                .Where(m => m.IsRequired)
                .Select(m => m.IncomingColumn)
                .ToList();

            foreach (var col in requiredCols)
            {
                bool found = dt.Columns.Cast<DataColumn>()
                                .Any(c => string.Equals(c.ColumnName, col, StringComparison.OrdinalIgnoreCase));
                if (!found)
                    throw new Exception($"Missing required column in file: {col}");
            }
            

            while (!sr.EndOfStream)
            {
                string[] values = sr.ReadLine()?.Split(delimiter.ToCharArray());
                if (values != null && values.Any(v => !string.IsNullOrWhiteSpace(v)))
                {
                    if (values.Length > headers.Length)
                    {
                        LogError(connStr, "CSV Format Warning", $"Too many values in line of {sourceFileName}, Truncating extras.");
                    }
                    DataRow row = dt.NewRow();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        if (i < values.Length && !string.IsNullOrWhiteSpace(values[i]))
                            row[i] = values[i].Trim('"');
                        else
                            row[i] = DBNull.Value;
                    }

                    // Add SourceFileName
                    row["SourceFileName"] = sourceFileName;

                    //Remove empty rows
                    bool isAllEmpty = row.ItemArray.All(val => val == DBNull.Value || string.IsNullOrWhiteSpace(val?.ToString()));
                    if (!isAllEmpty)
                        dt.Rows.Add(row);
                }
            }
        }
    }

    return dt;
}

private DataTable ReadExcel(string filePath, TableMapping mapping, List<ColumnMapping> colMappings, string connStr)
{
    DataTable dt = new DataTable();
    string ext = Path.GetExtension(filePath);
    string hdr = "No";
    string sheet = mapping.SheetName?.TrimEnd('$') + "$";
    string hdrRowIndex = mapping.HeaderRowIndex.ToString();

    string connStrExcel = $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={filePath};" +
                          $"Extended Properties=\"Excel 12.0 Xml;HDR={hdr};IMEX=1;TypeGuessRows=0;ImportMixedTypes=Text\"";

    try
    {
        using (OleDbConnection excelConn = new OleDbConnection(connStrExcel))
        {
            excelConn.Open();

            // Optional: Validate sheet exists
            DataTable sheets = excelConn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
            foreach (DataRow row in sheets.Rows)
            {
                string sheetName = row["TABLE_NAME"].ToString();
                Log(connStr, $"Found sheet: {sheetName}");
            }
            
            string expectedSheet = mapping.SheetName.TrimEnd('$') + "$";

            // Try to match ignoring quotes and casing
            string matchedSheetName = sheets.AsEnumerable()
                .Select(r => r["TABLE_NAME"].ToString().Trim('\''))
                .FirstOrDefault(name => name.Equals(expectedSheet, StringComparison.OrdinalIgnoreCase));

            if (matchedSheetName == null)
            {
                LogError(connStr, "Excel Sheet Error", $"Sheet '{expectedSheet}' not found in file: {Path.GetFileName(filePath)}. Available sheets: " +
                    string.Join(", ", sheets.AsEnumerable().Select(r => r["TABLE_NAME"].ToString())));
                throw new Exception($"Sheet '{expectedSheet}' not found.");
            }

            string query = $"SELECT * FROM [{matchedSheetName}]";

            using (OleDbDataAdapter da = new OleDbDataAdapter(query, excelConn))
            {
                da.Fill(dt);
            }

            DataTable schemaTable = excelConn.GetOleDbSchemaTable(OleDbSchemaGuid.Columns,
            new object[] { null, null, matchedSheetName, null });

            foreach (DataRow row in schemaTable.Rows)
            {
                string colName = row["COLUMN_NAME"].ToString();
                int ordinal = (int)row["ORDINAL_POSITION"];
                Log(connStr, $"Schema Column {ordinal}: {colName}");
            }

            // Promote the header row
            int headerIndex = mapping.HeaderRowIndex - 1;
            if (dt.Rows.Count <= headerIndex)
                throw new Exception($"HeaderRowIndex {mapping.HeaderRowIndex} exceeds total rows in Excel sheet.");

            DataRow headerRow = dt.Rows[headerIndex];
            for (int i = 0; i < dt.Columns.Count; i++)
            {
                string rawHeader = headerRow[i]?.ToString()?.Trim();

                // Replace odd characters
                if (!string.IsNullOrWhiteSpace(rawHeader))
                {
                    rawHeader = rawHeader
                        .Replace("\u00A0", "") // non-breaking space
                        .Replace("“", "").Replace("”", "")//smart quotes
                        .Replace("\"", "") // normal quotes
                        .Trim();
                }

                if (string.IsNullOrWhiteSpace(rawHeader))
                    rawHeader = $"Column{i + 1}"; // fallback name

                dt.Columns[i].ColumnName = rawHeader;
                Log(connStr, $"Parsed header: '{rawHeader}'");
            }

            // Remove header row only (don't delete it twice)
            dt.Rows.RemoveAt(headerIndex);

            // If you need to remove rows before header (rarely needed, be careful) - BRING IT BACK 
            if (headerIndex > 0)
            {
                for (int i = 0; i < headerIndex; i++)
                {
                    if (dt.Rows.Count > 0)
                        dt.Rows.RemoveAt(0);
                }
            }
            dt.AcceptChanges();

            if (dt.Rows.Count > 0)
            {
                dt = dt.AsEnumerable()
                        .Where(row => row.ItemArray.Any(cell =>
                            cell != null && !string.IsNullOrWhiteSpace(cell.ToString())))
                        .CopyToDataTable();
            }


            // Add SourceFileName
            if (!dt.Columns.Contains("SourceFileName"))
                dt.Columns.Add("SourceFileName", typeof(string));
            foreach (DataRow row in dt.Rows)
                row["SourceFileName"] = Path.GetFileName(filePath);

            // Validate required columns (optional)
            var requiredCols = colMappings.Where(m => m.IsRequired).Select(m => m.IncomingColumn).ToList();
            foreach (var col in requiredCols)
            {
                bool found = dt.Columns.Cast<DataColumn>().Any(c => string.Equals(c.ColumnName, col, StringComparison.OrdinalIgnoreCase));
                if (!found)
                    throw new Exception($"Missing required column in Excel file: {col}");
            }
        }
    }
    catch (Exception ex)
    {
        LogError(connStr, "Excel Read Error", $"Failed to load Excel file: {filePath}. {ex.Message}");
        throw;
    }

    return dt;
}

public void MapColumns(DataTable dt, List<ColumnMapping> mappings, string connStr)
{
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

public void LogTruncationIssues(DataTable dt, string tableName, SqlConnection conn)
{
    // Get destination column limits from database
    var colLengths = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
    using (SqlCommand cmd = new SqlCommand($@"
        SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = @TableName AND CHARACTER_MAXIMUM_LENGTH IS NOT NULL", conn))
    {
        cmd.Parameters.AddWithValue("@TableName", tableName);
        using (SqlDataReader reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                string colName = reader.GetString(0);
                int maxLength = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
                colLengths[colName] = maxLength;
            }
        }
    }

    // Check each row
    foreach (DataRow row in dt.Rows)
    {
        foreach (DataColumn col in dt.Columns)
        {
            if (col.DataType != typeof(string)) continue;

            if (!colLengths.TryGetValue(col.ColumnName, out int maxAllowed)) continue;

            string value = row[col]?.ToString();
            if (string.IsNullOrEmpty(value)) continue;

            if (value.Length > maxAllowed)
            {
                // Create a row identifier if needed
                string rowId = row.Table.Columns.Contains("SourceFileName") ? row["SourceFileName"].ToString() : "UnknownFile";

                using (SqlCommand logCmd = new SqlCommand(@"
                    INSERT INTO ETL.Truncation_Log 
                    (TableName, ColumnName, RowIdentifier, ActualLength, MaxAllowedLength, ValueTruncated) 
                    VALUES (@TableName, @ColumnName, @RowIdentifier, @ActualLength, @MaxAllowedLength, @ValueTruncated)", conn))
                {
                    logCmd.Parameters.AddWithValue("@TableName", tableName);
                    logCmd.Parameters.AddWithValue("@ColumnName", col.ColumnName);
                    logCmd.Parameters.AddWithValue("@RowIdentifier", rowId);
                    logCmd.Parameters.AddWithValue("@ActualLength", value.Length);
                    logCmd.Parameters.AddWithValue("@MaxAllowedLength", maxAllowed);
                    logCmd.Parameters.AddWithValue("@ValueTruncated", value.Substring(0, Math.Min(255, value.Length))); // Just a preview

                    logCmd.ExecuteNonQuery();
                }
            }
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

public void RunPostLoadProcedure(SqlConnection conn, string procedureName)
{
    if (string.IsNullOrWhiteSpace(procedureName))
        return;

    try
    {
        using (SqlCommand cmd = new SqlCommand(procedureName, conn))
        {
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.ExecuteNonQuery();
        }

        Log(conn.ConnectionString, $"Post-load procedure executed: {procedureName}");
    }
    catch (Exception ex)
    {
        LogError(conn.ConnectionString, $"Failed to execute post-load proc: {procedureName}", ex.Message);
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
        cmd.Parameters.AddWithValue("@msg", $"{errorType}: {message}");
        conn.Open();
        cmd.ExecuteNonQuery();
    }
}
