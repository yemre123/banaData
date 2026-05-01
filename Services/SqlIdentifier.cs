namespace banaData.Services;

internal static class SqlIdentifier
{
    public static string Quote(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new ArgumentException("SQL identifier cannot be empty.", nameof(identifier));
        }

        return $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}]";
    }

    public static string QuoteFullName(string schema, string name)
        => $"{Quote(schema)}.{Quote(name)}";
}
