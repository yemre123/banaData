using Microsoft.Data.SqlClient;

namespace banaData.Models;

public sealed record SqlConnectionSettings(
    string Server,
    string Database,
    bool UseIntegratedSecurity,
    string UserName,
    string Password,
    bool TrustServerCertificate,
    bool EncryptConnection = true)
{
    public string BuildConnectionString()
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = Server.Trim(),
            InitialCatalog = Database.Trim(),
            Encrypt = EncryptConnection,
            TrustServerCertificate = TrustServerCertificate,
            ConnectTimeout = 15,
            PersistSecurityInfo = false
        };

        if (UseIntegratedSecurity)
        {
            builder.IntegratedSecurity = true;
        }
        else
        {
            builder.UserID = UserName.Trim();
            builder.Password = Password;
        }

        return builder.ConnectionString;
    }
}

public sealed record DatabaseObject(string Schema, string Name)
{
    public string DisplayName => $"{Schema}.{Name}";
}

public sealed record ColumnMetadata(
    string Name,
    string SqlType,
    int Ordinal,
    bool IsNullable,
    bool IsIdentity,
    bool IsComputed,
    bool IsPrimaryKey,
    bool IsUnique);

public enum TransferRecordLimit
{
    Last1000,
    Last5000,
    All
}

public enum SortDirection
{
    Ascending,
    Descending
}

public sealed record TransferOptions(
    SqlConnectionSettings SourceConnection,
    SqlConnectionSettings TargetConnection,
    string SourceSchema,
    string TargetSchema,
    string TableName,
    TransferRecordLimit RecordLimit,
    bool UseOrderBy,
    string? OrderByColumn,
    SortDirection SortDirection = SortDirection.Descending);

public sealed record TransferProgress(
    string Message,
    int? RowsRead = null,
    int? RowsInserted = null,
    bool IsError = false,
    string? Detail = null);

public sealed record TransferResult(bool IsSuccess, int RowsRead, int RowsInserted, string Message, string? Detail = null)
{
    public static TransferResult Success(int rowsRead, int rowsInserted)
        => new(true, rowsRead, rowsInserted, "Transfer tamamlandı.", null);

    public static TransferResult Failure(int rowsRead, int rowsInserted, string message, string? detail = null)
        => new(false, rowsRead, rowsInserted, message, detail);
}
