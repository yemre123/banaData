using System.Text.Json;

namespace banaData.Services;

public sealed class ConnectionPreferencesService
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true
    };

    private readonly string _filePath;

    public ConnectionPreferencesService()
    {
        var folderPath = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "banaData");

        Directory.CreateDirectory(folderPath);
        _filePath = Path.Combine(folderPath, "connection-preferences.json");
    }

    public async Task<ConnectionPreferences?> LoadAsync(CancellationToken cancellationToken = default)
    {
        if (!File.Exists(_filePath))
        {
            return null;
        }

        try
        {
            await using var stream = File.OpenRead(_filePath);
            return await JsonSerializer.DeserializeAsync<ConnectionPreferences>(stream, SerializerOptions, cancellationToken);
        }
        catch
        {
            return null;
        }
    }

    public async Task SaveAsync(ConnectionPreferences preferences, CancellationToken cancellationToken = default)
    {
        await using var stream = File.Create(_filePath);
        await JsonSerializer.SerializeAsync(stream, preferences, SerializerOptions, cancellationToken);
    }

    public void Save(ConnectionPreferences preferences)
    {
        var json = JsonSerializer.Serialize(preferences, SerializerOptions);
        File.WriteAllText(_filePath, json);
    }
}

public sealed record ConnectionPreferences(
    SavedConnection Source,
    SavedConnection Target);

public sealed record SavedConnection(
    string Server,
    string Database,
    bool UseIntegratedSecurity,
    string UserName,
    bool TrustServerCertificate);
