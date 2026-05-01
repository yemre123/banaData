using System.Data;
using System.Globalization;
using banaData.Models;
using Microsoft.Data.SqlClient;

namespace banaData.Services;

public sealed class DataTransferService
{
    private const int BatchSize = 1000;
    private readonly TableMetadataService _metadataService;

    public DataTransferService(TableMetadataService metadataService)
    {
        _metadataService = metadataService;
    }

    public async Task<TransferResult> TransferAsync(
        TransferOptions options,
        IProgress<TransferProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        var rowsRead = 0;
        var rowsInserted = 0;

        try
        {
            progress?.Report(new TransferProgress("Transfer başladı."));

            var validation = await ValidateTransferAsync(options, cancellationToken);
            if (!validation.IsValid)
            {
                progress?.Report(new TransferProgress(validation.Message, IsError: true));
                return TransferResult.Failure(rowsRead, rowsInserted, validation.Message);
            }

            progress?.Report(new TransferProgress(
                "Uyarı: Bu işlem sadece insert yapar. Hedef tabloda aynı kayıtlar varsa primary key veya unique constraint hatası oluşabilir."));

            var insertColumns = validation.InsertColumns;
            var selectSql = BuildSelectSql(options, insertColumns);
            var targetTable = SqlIdentifier.QuoteFullName(options.TargetSchema, options.TableName);

            await using var sourceConnection = new SqlConnection(options.SourceConnection.BuildConnectionString());
            await using var targetConnection = new SqlConnection(options.TargetConnection.BuildConnectionString());
            await sourceConnection.OpenAsync(cancellationToken);
            await targetConnection.OpenAsync(cancellationToken);

            await using var sourceCommand = new SqlCommand(selectSql, sourceConnection)
            {
                CommandTimeout = 0
            };

            if (options.RecordLimit is not TransferRecordLimit.All)
            {
                sourceCommand.Parameters.AddWithValue("@take", GetRecordLimit(options.RecordLimit));
            }

            await using var reader = await sourceCommand.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);
            var batch = CreateBatchTable(reader, insertColumns);

            while (await reader.ReadAsync(cancellationToken))
            {
                AddReaderRow(batch, reader, insertColumns.Count);
                rowsRead++;

                if (batch.Rows.Count >= BatchSize)
                {
                    rowsInserted += await WriteBatchAsync(targetConnection, targetTable, insertColumns, batch, validation.HasTargetIdentity, cancellationToken);
                    progress?.Report(new TransferProgress("Transfer devam ediyor.", rowsRead, rowsInserted));
                    batch.Clear();
                }
            }

            if (batch.Rows.Count > 0)
            {
                rowsInserted += await WriteBatchAsync(targetConnection, targetTable, insertColumns, batch, validation.HasTargetIdentity, cancellationToken);
            }

            progress?.Report(new TransferProgress("İşlem tamamlandı.", rowsRead, rowsInserted));
            return TransferResult.Success(rowsRead, rowsInserted);
        }
        catch (SqlException ex)
        {
            var message = GetFriendlySqlMessage(ex);
            progress?.Report(new TransferProgress(message, rowsRead, rowsInserted, true));
            return TransferResult.Failure(rowsRead, rowsInserted, message);
        }
        catch (OperationCanceledException)
        {
            const string message = "Transfer işlemi iptal edildi.";
            progress?.Report(new TransferProgress(message, rowsRead, rowsInserted, true));
            return TransferResult.Failure(rowsRead, rowsInserted, message);
        }
        catch (Exception ex)
        {
            var message = $"Transfer sırasında beklenmeyen bir hata oluştu: {ex.Message}";
            progress?.Report(new TransferProgress(message, rowsRead, rowsInserted, true));
            return TransferResult.Failure(rowsRead, rowsInserted, message);
        }
    }

    private async Task<ValidationResult> ValidateTransferAsync(TransferOptions options, CancellationToken cancellationToken)
    {
        if (!await _metadataService.TableExistsAsync(options.TargetConnection, options.TargetSchema, options.TableName, cancellationToken))
        {
            return ValidationResult.Invalid($"Hedef veritabanında {options.TargetSchema}.{options.TableName} tablosu bulunamadı.");
        }

        var sourceColumns = await _metadataService.GetColumnsAsync(options.SourceConnection, options.SourceSchema, options.TableName, cancellationToken);
        var targetColumns = await _metadataService.GetColumnsAsync(options.TargetConnection, options.TargetSchema, options.TableName, cancellationToken);

        var columnValidationMessage = ValidateColumns(sourceColumns, targetColumns);
        if (columnValidationMessage is not null)
        {
            return ValidationResult.Invalid(columnValidationMessage);
        }

        if (options.RecordLimit is not TransferRecordLimit.All)
        {
            if (string.IsNullOrWhiteSpace(options.OrderByColumn))
            {
                return ValidationResult.Invalid("Son kayıtları seçmek için sıralama kolonu seçilmelidir.");
            }

            var orderColumn = sourceColumns.FirstOrDefault(column => column.Name == options.OrderByColumn);
            if (orderColumn is null || orderColumn.IsComputed)
            {
                return ValidationResult.Invalid("Seçilen sıralama kolonu source tabloda bulunamadı veya kullanılamıyor.");
            }
        }

        var insertColumns = sourceColumns
            .Where(source => !source.IsComputed)
            .OrderBy(source => source.Ordinal)
            .ToArray();

        if (insertColumns.Length == 0)
        {
            return ValidationResult.Invalid("Aktarılabilecek kolon bulunamadı. Computed kolonlar insert işleminde kullanılamaz.");
        }

        var hasTargetIdentity = targetColumns.Any(column => column.IsIdentity && insertColumns.Any(insertColumn => insertColumn.Name == column.Name));
        return ValidationResult.Valid(insertColumns, hasTargetIdentity);
    }

    private static string? ValidateColumns(IReadOnlyList<ColumnMetadata> sourceColumns, IReadOnlyList<ColumnMetadata> targetColumns)
    {
        if (sourceColumns.Count == 0)
        {
            return "Source tabloda kolon bulunamadı.";
        }

        var targetByName = targetColumns.ToDictionary(column => column.Name, StringComparer.OrdinalIgnoreCase);
        var sourceByName = sourceColumns.ToDictionary(column => column.Name, StringComparer.OrdinalIgnoreCase);

        var missingInTarget = sourceColumns
            .Where(column => !targetByName.ContainsKey(column.Name))
            .Select(column => column.Name)
            .ToArray();

        var extraInTarget = targetColumns
            .Where(column => !sourceByName.ContainsKey(column.Name))
            .Select(column => column.Name)
            .ToArray();

        if (missingInTarget.Length > 0 || extraInTarget.Length > 0)
        {
            return $"Kolonlar birebir uyuşmuyor. Target tarafında eksik: {FormatList(missingInTarget)}. Target tarafında fazla: {FormatList(extraInTarget)}.";
        }

        var typeMismatches = sourceColumns
            .Select(source => new { Source = source, Target = targetByName[source.Name] })
            .Where(pair =>
                !string.Equals(pair.Source.SqlType, pair.Target.SqlType, StringComparison.OrdinalIgnoreCase) ||
                pair.Source.IsComputed != pair.Target.IsComputed)
            .Select(pair => $"{pair.Source.Name} (source: {pair.Source.SqlType}, target: {pair.Target.SqlType})")
            .ToArray();

        if (typeMismatches.Length > 0)
        {
            return $"Kolon tipleri veya computed kolon durumu uyuşmuyor: {FormatList(typeMismatches)}.";
        }

        return null;
    }

    private static string BuildSelectSql(TransferOptions options, IReadOnlyList<ColumnMetadata> insertColumns)
    {
        var columns = string.Join(", ", insertColumns.Select(column => SqlIdentifier.Quote(column.Name)));
        var sourceTable = SqlIdentifier.QuoteFullName(options.SourceSchema, options.TableName);

        if (options.RecordLimit is TransferRecordLimit.All)
        {
            return $"SELECT {columns} FROM {sourceTable};";
        }

        return $"SELECT TOP (@take) {columns} FROM {sourceTable} ORDER BY {SqlIdentifier.Quote(options.OrderByColumn!)} DESC;";
    }

    private static DataTable CreateBatchTable(SqlDataReader reader, IReadOnlyList<ColumnMetadata> insertColumns)
    {
        var table = new DataTable
        {
            Locale = CultureInfo.InvariantCulture
        };

        for (var index = 0; index < insertColumns.Count; index++)
        {
            table.Columns.Add(insertColumns[index].Name, reader.GetFieldType(index));
        }

        return table;
    }

    private static void AddReaderRow(DataTable batch, SqlDataReader reader, int columnCount)
    {
        var row = batch.NewRow();

        for (var index = 0; index < columnCount; index++)
        {
            row[index] = reader.IsDBNull(index) ? DBNull.Value : reader.GetValue(index);
        }

        batch.Rows.Add(row);
    }

    private static async Task<int> WriteBatchAsync(
        SqlConnection targetConnection,
        string targetTable,
        IReadOnlyList<ColumnMetadata> insertColumns,
        DataTable batch,
        bool keepIdentity,
        CancellationToken cancellationToken)
    {
        var options = keepIdentity ? SqlBulkCopyOptions.KeepIdentity : SqlBulkCopyOptions.Default;

        using var bulkCopy = new SqlBulkCopy(targetConnection, options, null)
        {
            DestinationTableName = targetTable,
            BatchSize = BatchSize,
            BulkCopyTimeout = 0
        };

        foreach (var column in insertColumns)
        {
            bulkCopy.ColumnMappings.Add(column.Name, column.Name);
        }

        await bulkCopy.WriteToServerAsync(batch, cancellationToken);
        return batch.Rows.Count;
    }

    private static int GetRecordLimit(TransferRecordLimit recordLimit)
    {
        return recordLimit switch
        {
            TransferRecordLimit.Last1000 => 1000,
            TransferRecordLimit.Last5000 => 5000,
            _ => throw new InvalidOperationException("Bu kayıt limiti için sayı değeri yok.")
        };
    }

    private static string FormatList(IReadOnlyList<string> values)
        => values.Count == 0 ? "yok" : string.Join(", ", values);

    private static string GetFriendlySqlMessage(SqlException exception)
    {
        return exception.Number switch
        {
            208 => "Seçilen tablo bulunamadı. Source ve target tablo adlarını kontrol edin.",
            2601 or 2627 => "Duplicate kayıt hatası oluştu. Hedef tabloda primary key veya unique constraint aynı değere sahip bir kayıtla çakışıyor.",
            544 => "Identity kolonuna veri yazılamadı. Hedef tabloda identity ayarlarını ve yetkileri kontrol edin.",
            515 => "Hedef tabloda boş geçilemeyen bir kolona değer gelmedi.",
            547 => "Hedef tablodaki constraint kuralı nedeniyle kayıt eklenemedi.",
            _ => $"SQL işlemi başarısız oldu: {exception.Message}"
        };
    }

    private sealed record ValidationResult(bool IsValid, string Message, IReadOnlyList<ColumnMetadata> InsertColumns, bool HasTargetIdentity)
    {
        public static ValidationResult Valid(IReadOnlyList<ColumnMetadata> insertColumns, bool hasTargetIdentity)
            => new(true, string.Empty, insertColumns, hasTargetIdentity);

        public static ValidationResult Invalid(string message)
            => new(false, message, Array.Empty<ColumnMetadata>(), false);
    }
}
