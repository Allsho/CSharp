IF NOT EXISTS (
    SELECT * 
    FROM sys.tables t 
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'Claim_File_Schema_History' AND s.name = 'ETL'
)
BEGIN
    CREATE TABLE ETL.Claim_File_Schema_History
    (
        HistoryID INT IDENTITY(1,1) PRIMARY KEY,
        TableName NVARCHAR(255) NOT NULL,
        ColumnName NVARCHAR(255) NOT NULL,
        ColumnDetected BIT NOT NULL,         -- Was this column found in the file?
        ColumnMapped BIT NOT NULL,           -- Was this column mapped to the DB?
        SuggestedAlterStatement NVARCHAR(MAX), -- Suggest ALTER if needed
        SourceFileName NVARCHAR(255) NULL,   -- Optional: Which file triggered this?
        ProcessedOn DATETIME NOT NULL DEFAULT GETDATE()
    );
END
