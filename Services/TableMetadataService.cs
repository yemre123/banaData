using banaData.Models;
using Microsoft.Data.SqlClient;

namespace banaData.Services;

public sealed class TableMetadataService
{
    public async Task<IReadOnlyList<string>> GetSchemasAsync(
        SqlConnectionSettings connectionSettings,
        CancellationToken cancellationToken = default)
    {
        const string sql = """
            SELECT name
            FROM sys.schemas
            WHERE name NOT IN ('sys', 'INFORMATION_SCHEMA')
            ORDER BY name;
            """;

        var schemas = new List<string>();
        await using var connection = new SqlConnection(connectionSettings.BuildConnectionString());
        await using var command = new SqlCommand(sql, connection);

        await connection.OpenAsync(cancellationToken);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            schemas.Add(reader.GetString(0));
        }

        return schemas;
    }

    public async Task<IReadOnlyList<DatabaseObject>> GetTablesAsync(
        SqlConnectionSettings connectionSettings,
        string schema,
        CancellationToken cancellationToken = default)
    {
        const string sql = """
            SELECT s.name AS SchemaName, t.name AS TableName
            FROM sys.tables AS t
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE t.is_ms_shipped = 0
              AND s.name = @schema
            ORDER BY s.name, t.name;
            """;

        var tables = new List<DatabaseObject>();
        await using var connection = new SqlConnection(connectionSettings.BuildConnectionString());
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@schema", schema);

        await connection.OpenAsync(cancellationToken);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            tables.Add(new DatabaseObject(reader.GetString(0), reader.GetString(1)));
        }

        return tables;
    }

    public async Task<IReadOnlyList<ColumnMetadata>> GetColumnsAsync(
        SqlConnectionSettings connectionSettings,
        string schema,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        const string sql = """
            SELECT
                c.name AS ColumnName,
                TYPE_NAME(c.user_type_id) AS SqlType,
                c.column_id AS Ordinal,
                CONVERT(bit, c.is_nullable) AS IsNullable,
                CONVERT(bit, c.is_identity) AS IsIdentity,
                CONVERT(bit, c.is_computed) AS IsComputed,
                CONVERT(bit, CASE WHEN EXISTS (
                    SELECT 1
                    FROM sys.indexes AS i
                    INNER JOIN sys.index_columns AS ic
                        ON ic.object_id = i.object_id
                       AND ic.index_id = i.index_id
                    WHERE i.object_id = c.object_id
                      AND ic.column_id = c.column_id
                      AND i.is_primary_key = 1
                ) THEN 1 ELSE 0 END) AS IsPrimaryKey,
                CONVERT(bit, CASE WHEN EXISTS (
                    SELECT 1
                    FROM sys.indexes AS i
                    INNER JOIN sys.index_columns AS ic
                        ON ic.object_id = i.object_id
                       AND ic.index_id = i.index_id
                    WHERE i.object_id = c.object_id
                      AND ic.column_id = c.column_id
                      AND i.is_unique = 1
                ) THEN 1 ELSE 0 END) AS IsUnique
            FROM sys.columns AS c
            INNER JOIN sys.tables AS t ON t.object_id = c.object_id
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = @schema
              AND t.name = @table
            ORDER BY c.column_id;
            """;

        var columns = new List<ColumnMetadata>();
        await using var connection = new SqlConnection(connectionSettings.BuildConnectionString());
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@schema", schema);
        command.Parameters.AddWithValue("@table", tableName);

        await connection.OpenAsync(cancellationToken);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(new ColumnMetadata(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetInt32(2),
                reader.GetBoolean(3),
                reader.GetBoolean(4),
                reader.GetBoolean(5),
                reader.GetBoolean(6),
                reader.GetBoolean(7)));
        }

        return columns;
    }

    public async Task<long> GetRowCountAsync(
        SqlConnectionSettings connectionSettings,
        string schema,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        var quoted = SqlIdentifier.QuoteFullName(schema, tableName);
        var sql = $"SELECT COUNT_BIG(*) FROM {quoted};";

        await using var connection = new SqlConnection(connectionSettings.BuildConnectionString());
        await using var command = new SqlCommand(sql, connection)
        {
            CommandTimeout = 30
        };

        await connection.OpenAsync(cancellationToken);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is null or DBNull ? 0L : Convert.ToInt64(result);
    }

    public async Task<bool> TableExistsAsync(
        SqlConnectionSettings connectionSettings,
        string schema,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        const string sql = """
            SELECT COUNT(1)
            FROM sys.tables AS t
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = @schema
              AND t.name = @table;
            """;

        await using var connection = new SqlConnection(connectionSettings.BuildConnectionString());
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@schema", schema);
        command.Parameters.AddWithValue("@table", tableName);

        await connection.OpenAsync(cancellationToken);
        var result = await command.ExecuteScalarAsync(cancellationToken);

        return Convert.ToInt32(result) > 0;
    }
}
