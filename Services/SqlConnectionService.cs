using banaData.Models;
using Microsoft.Data.SqlClient;

namespace banaData.Services;

public sealed class SqlConnectionService
{
    public async Task<(bool IsSuccess, string Message)> TestConnectionAsync(
        SqlConnectionSettings settings,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = new SqlConnection(settings.BuildConnectionString());
            await connection.OpenAsync(cancellationToken);

            return (true, "Bağlantı başarılı.");
        }
        catch (SqlException ex)
        {
            return (false, GetFriendlyConnectionMessage(ex));
        }
        catch (InvalidOperationException ex)
        {
            return (false, $"Bağlantı bilgileri geçerli değil: {ex.Message}");
        }
    }

    private static string GetFriendlyConnectionMessage(SqlException exception)
    {
        if (exception.Message.Contains("certificate chain", StringComparison.OrdinalIgnoreCase) ||
            exception.Message.Contains("trust relationship", StringComparison.OrdinalIgnoreCase))
        {
            return "SSL sertifika doğrulaması başarısız. Yerel SQL Server için 'Trust Server Certificate' seçeneğini açık bırakın.";
        }

        return exception.Number switch
        {
            53 or 11001 => "SQL Server bulunamadı. Sunucu adını ve ağ bağlantısını kontrol edin.",
            18456 => "Giriş başarısız. Kullanıcı adı, şifre veya yetki bilgilerini kontrol edin.",
            4060 => "Veritabanı açılamadı. Veritabanı adını ve erişim yetkisini kontrol edin.",
            -2 => "Bağlantı zaman aşımına uğradı. Sunucu erişimini kontrol edin.",
            _ => $"Bağlantı kurulamadı: {exception.Message}"
        };
    }
}
