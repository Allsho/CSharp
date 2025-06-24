public void LogSchemaDifferences(DataTable dt, string tableName, SqlConnection conn)
{
    // Get destination table schema
    HashSet<string> destColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    using (SqlCommand cmd = new SqlCommand($"SELECT TOP 0 * FROM {tableName}", conn))
    using (SqlDataReader reader = cmd.ExecuteReader())
    {
        for (int i = 0; i < reader.FieldCount; i++)
            destColumns.Add(reader.GetName(i));
    }

    // Create logging table if it doesn't exist (optional)
    using (SqlCommand cmd = new SqlCommand(@"
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'File_Schema_History')
        BEGIN
            CREATE TABLE ETL.File_Schema_History (
                TableName NVARCHAR(255),
                ColumnName NVARCHAR(255),
                ColumnDetected BIT,
                ColumnMapped BIT,
                SuggestedAlterStatement NVARCHAR(MAX),
                ProcessedOn DATETIME DEFAULT GETDATE()
            );
        END", conn))
    {
        cmd.ExecuteNonQuery();
    }

    // Log each column in the file
    foreach (DataColumn col in dt.Columns)
    {
        bool existsInDb = destColumns.Contains(col.ColumnName);
        string alterStmt = existsInDb
            ? null
            : $"ALTER TABLE {tableName} ADD [{col.ColumnName}] NVARCHAR(255);";

        using (SqlCommand logCmd = new SqlCommand(@"
            INSERT INTO ETL.File_Schema_History (TableName, ColumnName, ColumnDetected, ColumnMapped, SuggestedAlterStatement)
            VALUES (@TableName, @ColumnName, 1, @ColumnMapped, @AlterStatement)", conn))
        {
            logCmd.Parameters.AddWithValue("@TableName", tableName);
            logCmd.Parameters.AddWithValue("@ColumnName", col.ColumnName);
            logCmd.Parameters.AddWithValue("@ColumnMapped", existsInDb ? 1 : 0);
            logCmd.Parameters.AddWithValue("@AlterStatement", (object)alterStmt ?? DBNull.Value);
            logCmd.ExecuteNonQuery();
        }
    }
}

CREATE TABLE ETL.File_Schema_History (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    FileName VARCHAR(255),
    TargetTable VARCHAR(255),
    ColumnName VARCHAR(255),
    ColumnDetected BIT,
    ColumnMapped BIT,
    AlterStatement NVARCHAR(MAX),
    ProcessedOn DATETIME DEFAULT GETDATE()
);
