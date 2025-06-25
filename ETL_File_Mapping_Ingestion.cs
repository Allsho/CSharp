CREATE PROCEDURE ETL.usp_Log_Claim_File_Schema_History
    @TableName NVARCHAR(255),
    @ColumnName NVARCHAR(255),
    @ColumnDetected BIT,
    @ColumnMapped BIT,
    @SuggestedAlterStatement NVARCHAR(MAX)
AS
BEGIN
    INSERT INTO ETL.Claim_File_Schema_History
    (TableName, ColumnName, ColumnDetected, ColumnMapped, SuggestedAlterStatement, ProcessedOn)
    VALUES
    (@TableName, @ColumnName, @ColumnDetected, @ColumnMapped, @SuggestedAlterStatement, GETDATE());
END
*/

// In C#:
public void LogSchemaDifferences(DataTable dt, string tableName, SqlConnection conn)
{
    HashSet<string> destColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    using (SqlCommand cmd = new SqlCommand($"SELECT TOP 0 * FROM {tableName}", conn))
    using (SqlDataReader reader = cmd.ExecuteReader())
    {
        for (int i = 0; i < reader.FieldCount; i++)
            destColumns.Add(reader.GetName(i));
    }

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
            logCmd.ExecuteNonQuery();
        }
    }
}

// In ReadCsv(): Safe row filling
for (int i = 0; i < headers.Length; i++)
{
    row[i] = i < values.Length ? values[i].Trim('"') : DBNull.Value;
}
