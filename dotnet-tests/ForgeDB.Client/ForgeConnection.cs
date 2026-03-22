using System.Data;
using System.Data.Common;
using System.Net.Sockets;
using System.Text;

namespace ForgeDB.Client;

/// <summary>
/// ForgeWire protocol constants.
/// </summary>
internal static class Wire
{
    // Client → Server
    public const byte Query = 0x01;
    public const byte Prepare = 0x02;
    public const byte Execute = 0x03;
    public const byte CloseStmt = 0x04;
    public const byte Ping = 0x05;
    public const byte Disconnect = 0xFF;

    // Server → Client
    public const byte RowHeader = 0x10;
    public const byte Row = 0x11;
    public const byte Done = 0x12;
    public const byte Error = 0x13;
    public const byte PrepareOk = 0x14;
    public const byte Pong = 0x15;

    // Value tags
    public const byte Null = 0x00;
    public const byte Int32 = 0x01;
    public const byte Int64 = 0x02;
    public const byte Float64 = 0x03;
    public const byte Bool = 0x04;
    public const byte Str = 0x05;
    public const byte Blob = 0x06;
}

/// <summary>
/// ADO.NET connection to ForgeDB via ForgeWire binary protocol.
/// </summary>
public sealed class ForgeConnection : DbConnection
{
    private string _connectionString = "";
    private string _host = "127.0.0.1";
    private int _port = 5433;
    private TcpClient? _tcp;
    private NetworkStream? _stream;
    private ConnectionState _state = ConnectionState.Closed;

    public ForgeConnection() { }
    public ForgeConnection(string connectionString) { ConnectionString = connectionString; }

    public override string ConnectionString
    {
        get => _connectionString;
        set
        {
            _connectionString = value ?? "";
            ParseConnectionString(_connectionString);
        }
    }

    public override string Database => "forgedb";
    public override string DataSource => $"{_host}:{_port}";
    public override string ServerVersion => "1.0";
    public override ConnectionState State => _state;

    private void ParseConnectionString(string cs)
    {
        foreach (var part in cs.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var kv = part.Split('=', 2);
            if (kv.Length != 2) continue;
            var key = kv[0].Trim().ToLowerInvariant();
            var val = kv[1].Trim();
            switch (key)
            {
                case "host": case "server": _host = val; break;
                case "port": if (int.TryParse(val, out var p)) _port = p; break;
            }
        }
    }

    public override void Open()
    {
        if (_state == ConnectionState.Open) return;
        _tcp = new TcpClient();
        _tcp.NoDelay = true;
        _tcp.Connect(_host, _port);
        _stream = _tcp.GetStream();
        _state = ConnectionState.Open;
    }

    public override void Close()
    {
        if (_state == ConnectionState.Closed) return;
        try
        {
            SendMessage(Wire.Disconnect, ReadOnlySpan<byte>.Empty);
        }
        catch { }
        _stream?.Dispose();
        _tcp?.Dispose();
        _stream = null;
        _tcp = null;
        _state = ConnectionState.Closed;
    }

    protected override void Dispose(bool disposing) { if (disposing) Close(); base.Dispose(disposing); }

    // -- Wire I/O --

    internal void SendMessage(byte msgType, ReadOnlySpan<byte> payload)
    {
        var s = _stream ?? throw new InvalidOperationException("Not connected");
        Span<byte> hdr = stackalloc byte[5];
        hdr[0] = msgType;
        BitConverter.TryWriteBytes(hdr[1..], (uint)payload.Length);
        s.Write(hdr);
        if (payload.Length > 0) s.Write(payload);
    }

    internal (byte msgType, byte[] payload) ReadMessage()
    {
        var s = _stream ?? throw new InvalidOperationException("Not connected");
        Span<byte> hdr = stackalloc byte[5];
        s.ReadExactly(hdr);
        var msgType = hdr[0];
        var length = BitConverter.ToUInt32(hdr[1..]);
        var payload = new byte[length];
        if (length > 0) s.ReadExactly(payload);
        return (msgType, payload);
    }

    // -- Batch INSERT --

    /// <summary>
    /// Insert multiple rows in a single TCP round-trip.
    /// Uses MSG_BATCH_INSERT (0x06) protocol message.
    /// </summary>
    public int BatchInsert(string tableName, object?[][] rows)
    {
        if (rows.Length == 0) return 0;
        int colCount = rows[0].Length;

        // Build payload: table_name_len(u16) + table_name + col_count(u16) + row_count(u32) + values
        var buf = new MemoryStream();
        var w = new BinaryWriter(buf);

        var nameBytes = Encoding.UTF8.GetBytes(tableName);
        w.Write((ushort)nameBytes.Length);
        w.Write(nameBytes);
        w.Write((ushort)colCount);
        w.Write((uint)rows.Length);

        foreach (var row in rows)
        {
            foreach (var val in row)
            {
                switch (val)
                {
                    case null: w.Write(Wire.Null); break;
                    case int n: w.Write(Wire.Int32); w.Write(n); break;
                    case long n: w.Write(Wire.Int64); w.Write(n); break;
                    case double d: w.Write(Wire.Float64); w.Write(d); break;
                    case float f: w.Write(Wire.Float64); w.Write((double)f); break;
                    case bool b: w.Write(Wire.Bool); w.Write(b ? (byte)1 : (byte)0); break;
                    case string s:
                        var sb = Encoding.UTF8.GetBytes(s);
                        w.Write(Wire.Str);
                        w.Write((uint)sb.Length);
                        w.Write(sb);
                        break;
                    default:
                        var ds = val.ToString() ?? "";
                        var db = Encoding.UTF8.GetBytes(ds);
                        w.Write(Wire.Str);
                        w.Write((uint)db.Length);
                        w.Write(db);
                        break;
                }
            }
        }

        w.Flush();
        var payload = buf.ToArray();
        SendMessage(0x06, payload);

        // Read response
        var (msgType, respPayload) = ReadMessage();
        if (msgType == Wire.Error)
        {
            var errLen = BitConverter.ToUInt16(respPayload);
            throw new ForgeException(Encoding.UTF8.GetString(respPayload, 2, errLen));
        }
        return (int)BitConverter.ToUInt64(respPayload);
    }

    // -- ADO.NET plumbing --

    public override void ChangeDatabase(string databaseName) { }
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException("Use SQL BEGIN/COMMIT");
    protected override DbCommand CreateDbCommand() => new ForgeCommand { Connection = this };
    public new ForgeCommand CreateCommand() => new ForgeCommand { Connection = this };
}

/// <summary>
/// ADO.NET command for ForgeDB via ForgeWire.
/// </summary>
public sealed class ForgeCommand : DbCommand
{
    private string _sql = "";
    private ForgeConnection? _conn;
    private readonly ForgeParameterCollection _params = new();

    public override string CommandText { get => _sql; set => _sql = value ?? ""; }
    public override int CommandTimeout { get; set; } = 30;
    public override CommandType CommandType { get; set; } = CommandType.Text;
    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }
    protected override DbConnection? DbConnection { get => _conn; set => _conn = value as ForgeConnection; }
    protected override DbParameterCollection DbParameterCollection => _params;
    protected override DbTransaction? DbTransaction { get; set; }

    public new ForgeConnection? Connection { get => _conn; set => _conn = value; }

    public override void Cancel() { }
    public override void Prepare() { }

    public override int ExecuteNonQuery()
    {
        var conn = _conn ?? throw new InvalidOperationException("No connection");
        var sql = SubstituteParams();
        conn.SendMessage(Wire.Query, Encoding.UTF8.GetBytes(sql));

        // Read response
        while (true)
        {
            var (msgType, payload) = conn.ReadMessage();
            switch (msgType)
            {
                case Wire.Done:
                    return (int)BitConverter.ToUInt64(payload);
                case Wire.Error:
                    var errLen = BitConverter.ToUInt16(payload);
                    throw new ForgeException(Encoding.UTF8.GetString(payload, 2, errLen));
                case Wire.RowHeader:
                case Wire.Row:
                    continue; // drain result set
            }
        }
    }

    public override object? ExecuteScalar()
    {
        using var reader = ExecuteReader();
        if (reader.Read() && reader.FieldCount > 0)
            return reader.GetValue(0);
        return null;
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var conn = _conn ?? throw new InvalidOperationException("No connection");
        var sql = SubstituteParams();
        conn.SendMessage(Wire.Query, Encoding.UTF8.GetBytes(sql));
        return new ForgeDataReader(conn);
    }

    public new ForgeDataReader ExecuteReader() => (ForgeDataReader)ExecuteDbDataReader(CommandBehavior.Default);

    private string SubstituteParams()
    {
        if (_params.Count == 0) return _sql;
        var sql = _sql;
        for (int i = _params.Count - 1; i >= 0; i--)
        {
            var p = _params[i];
            var name = p.ParameterName;
            if (string.IsNullOrEmpty(name)) name = $"@p{i}";
            var literal = p.Value switch
            {
                null or DBNull => "NULL",
                int n => n.ToString(),
                long n => n.ToString(),
                double d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
                float f => f.ToString(System.Globalization.CultureInfo.InvariantCulture),
                bool b => b ? "1" : "0",
                string s => $"'{s.Replace("'", "''")}'",
                _ => $"'{p.Value}'"
            };
            sql = sql.Replace(name, literal);
        }
        return sql;
    }

    protected override DbParameter CreateDbParameter() => new ForgeParameter();
}

/// <summary>
/// ForgeWire data reader — streams rows from server.
/// </summary>
public sealed class ForgeDataReader : DbDataReader
{
    private readonly ForgeConnection _conn;
    private string[] _colNames = Array.Empty<string>();
    private byte[] _colTypes = Array.Empty<byte>();
    private object?[] _currentRow = Array.Empty<object?>();
    private bool _hasRows;
    private bool _closed;
    private bool _headerRead;
    private long _rowsAffected;

    internal ForgeDataReader(ForgeConnection conn) { _conn = conn; }

    public override int FieldCount => _colNames.Length;
    public override bool HasRows => _hasRows;
    public override bool IsClosed => _closed;
    public override int RecordsAffected => (int)_rowsAffected;
    public override int Depth => 0;

    public override bool Read()
    {
        if (_closed) return false;

        while (true)
        {
            var (msgType, payload) = _conn.ReadMessage();
            switch (msgType)
            {
                case Wire.RowHeader:
                    ReadHeader(payload);
                    _headerRead = true;
                    continue;

                case Wire.Row:
                    ReadRow(payload);
                    _hasRows = true;
                    return true;

                case Wire.Done:
                    _rowsAffected = (long)BitConverter.ToUInt64(payload);
                    _closed = true;
                    return false;

                case Wire.Error:
                    var errLen = BitConverter.ToUInt16(payload);
                    _closed = true;
                    throw new ForgeException(Encoding.UTF8.GetString(payload, 2, errLen));
            }
        }
    }

    private void ReadHeader(byte[] data)
    {
        int pos = 0;
        int colCount = BitConverter.ToUInt16(data, pos); pos += 2;
        _colNames = new string[colCount];
        _colTypes = new byte[colCount];
        _currentRow = new object?[colCount];
        for (int i = 0; i < colCount; i++)
        {
            int nameLen = BitConverter.ToUInt16(data, pos); pos += 2;
            _colNames[i] = Encoding.UTF8.GetString(data, pos, nameLen); pos += nameLen;
            _colTypes[i] = data[pos++];
        }
    }

    private void ReadRow(byte[] data)
    {
        int pos = 0;
        for (int i = 0; i < _colNames.Length && pos < data.Length; i++)
        {
            byte tag = data[pos++];
            switch (tag)
            {
                case Wire.Null: _currentRow[i] = DBNull.Value; break;
                case Wire.Int32: _currentRow[i] = BitConverter.ToInt32(data, pos); pos += 4; break;
                case Wire.Int64: _currentRow[i] = BitConverter.ToInt64(data, pos); pos += 8; break;
                case Wire.Float64: _currentRow[i] = BitConverter.ToDouble(data, pos); pos += 8; break;
                case Wire.Bool: _currentRow[i] = data[pos++] != 0; break;
                case Wire.Str:
                    int sLen = (int)BitConverter.ToUInt32(data, pos); pos += 4;
                    _currentRow[i] = Encoding.UTF8.GetString(data, pos, sLen); pos += sLen;
                    break;
                default: _currentRow[i] = DBNull.Value; break;
            }
        }
    }

    // -- DbDataReader interface --

    public override object GetValue(int ordinal) => _currentRow[ordinal] ?? DBNull.Value;
    public override string GetName(int ordinal) => _colNames[ordinal];
    public override int GetOrdinal(string name) => Array.IndexOf(_colNames, name);
    public override Type GetFieldType(int ordinal) => _currentRow[ordinal]?.GetType() ?? typeof(object);
    public override string GetDataTypeName(int ordinal) => _colTypes[ordinal] switch { Wire.Int32 => "int", Wire.Int64 => "bigint", Wire.Float64 => "float", Wire.Bool => "bit", Wire.Str => "varchar", _ => "unknown" };
    public override bool IsDBNull(int ordinal) => _currentRow[ordinal] is null or DBNull;
    public override int GetInt32(int ordinal) => Convert.ToInt32(_currentRow[ordinal]);
    public override long GetInt64(int ordinal) => Convert.ToInt64(_currentRow[ordinal]);
    public override double GetDouble(int ordinal) => Convert.ToDouble(_currentRow[ordinal]);
    public override bool GetBoolean(int ordinal) => Convert.ToBoolean(_currentRow[ordinal]);
    public override string GetString(int ordinal) => _currentRow[ordinal]?.ToString() ?? "";
    public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(_currentRow[ordinal]);
    public override float GetFloat(int ordinal) => Convert.ToSingle(_currentRow[ordinal]);
    public override short GetInt16(int ordinal) => Convert.ToInt16(_currentRow[ordinal]);
    public override byte GetByte(int ordinal) => Convert.ToByte(_currentRow[ordinal]);
    public override char GetChar(int ordinal) => Convert.ToChar(_currentRow[ordinal]);
    public override DateTime GetDateTime(int ordinal) => Convert.ToDateTime(_currentRow[ordinal]);
    public override Guid GetGuid(int ordinal) => Guid.Parse(_currentRow[ordinal]?.ToString() ?? "");
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;
    public override int GetValues(object[] values) { Array.Copy(_currentRow!, values, Math.Min(_currentRow.Length, values.Length)); return Math.Min(_currentRow.Length, values.Length); }
    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));
    public override bool NextResult() => false;
    public override System.Collections.IEnumerator GetEnumerator() => throw new NotSupportedException();
    public override void Close() { _closed = true; }
}

/// <summary>
/// ForgeDB parameter.
/// </summary>
public sealed class ForgeParameter : DbParameter
{
    public override string ParameterName { get; set; } = "";
    public override object? Value { get; set; }
    public override DbType DbType { get; set; }
    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    public override bool IsNullable { get; set; }
    public override int Size { get; set; }
    public override string SourceColumn { get; set; } = "";
    public override bool SourceColumnNullMapping { get; set; }
    public override void ResetDbType() { DbType = DbType.String; }
}

/// <summary>
/// ForgeDB parameter collection.
/// </summary>
public sealed class ForgeParameterCollection : DbParameterCollection
{
    private readonly List<ForgeParameter> _params = new();
    public override int Count => _params.Count;
    public override object SyncRoot => _params;
    public ForgeParameter this[int i] => _params[i];

    public ForgeParameter Add(string name, object? value) { var p = new ForgeParameter { ParameterName = name, Value = value }; _params.Add(p); return p; }
    public override int Add(object value) { _params.Add((ForgeParameter)value); return _params.Count - 1; }
    public override void Clear() => _params.Clear();
    public override bool Contains(object value) => _params.Contains((ForgeParameter)value);
    public override bool Contains(string value) => _params.Any(p => p.ParameterName == value);
    public override int IndexOf(object value) => _params.IndexOf((ForgeParameter)value);
    public override int IndexOf(string parameterName) => _params.FindIndex(p => p.ParameterName == parameterName);
    public override void Insert(int index, object value) => _params.Insert(index, (ForgeParameter)value);
    public override void Remove(object value) => _params.Remove((ForgeParameter)value);
    public override void RemoveAt(int index) => _params.RemoveAt(index);
    public override void RemoveAt(string parameterName) => _params.RemoveAll(p => p.ParameterName == parameterName);
    public override void CopyTo(Array array, int index) => ((System.Collections.IList)_params).CopyTo(array, index);
    public override System.Collections.IEnumerator GetEnumerator() => _params.GetEnumerator();
    protected override DbParameter GetParameter(int index) => _params[index];
    protected override DbParameter GetParameter(string parameterName) => _params.First(p => p.ParameterName == parameterName);
    protected override void SetParameter(int index, DbParameter value) => _params[index] = (ForgeParameter)value;
    protected override void SetParameter(string parameterName, DbParameter value) { var i = IndexOf(parameterName); if (i >= 0) _params[i] = (ForgeParameter)value; }
    public override void AddRange(Array values) { foreach (ForgeParameter p in values) _params.Add(p); }
}

/// <summary>
/// ForgeDB exception.
/// </summary>
public sealed class ForgeException : DbException
{
    public ForgeException(string message) : base(message) { }
}
