using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using System.Data;

namespace OpinionesClientesETL.Services;

public class IdMappingService
{
    private readonly ILogger<IdMappingService> _logger;
    private readonly string _connectionString;
    private readonly Dictionary<string, int> _fuenteIds = new();
    private readonly Dictionary<string, int> _productoIds = new();
    private readonly Dictionary<string, int> _clienteIds = new();

    public IdMappingService(ILogger<IdMappingService> logger, IConfiguration configuration)
    {
        _logger = logger;
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("DefaultConnection string not found");
    }

    public async Task LoadFuenteIdsAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var query = "SELECT FuenteID, Nombre FROM Fuentes";
        using var command = new SqlCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync();

        _fuenteIds.Clear();
        while (await reader.ReadAsync())
        {
            var id = reader.GetInt32("FuenteID");
            var nombre = reader.GetString("Nombre");
            _fuenteIds[nombre] = id;
        }

        _logger.LogInformation($"Cargados {_fuenteIds.Count} IDs de Fuentes");
    }

    public async Task LoadProductoIdsAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var query = "SELECT ProductoID, Codigo FROM Productos";
        using var command = new SqlCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync();

        _productoIds.Clear();
        while (await reader.ReadAsync())
        {
            var id = reader.GetInt32("ProductoID");
            var codigo = reader.GetString("Codigo");
            _productoIds[codigo] = id;
        }

        _logger.LogInformation($"Cargados {_productoIds.Count} IDs de Productos");
    }

    public async Task LoadClienteIdsAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var query = "SELECT ClienteID, Codigo FROM Clientes";
        using var command = new SqlCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync();

        _clienteIds.Clear();
        while (await reader.ReadAsync())
        {
            var id = reader.GetInt32("ClienteID");
            var codigo = reader.GetString("Codigo");
            _clienteIds[codigo] = id;
        }

        _logger.LogInformation($"Cargados {_clienteIds.Count} IDs de Clientes");
    }

    public int GetFuenteId(string fuenteNombre)
    {
        return _fuenteIds.TryGetValue(fuenteNombre, out var id) ? id : _fuenteIds.Values.FirstOrDefault(1);
    }

    public int GetProductoId(string productoCodigo)
    {
        return _productoIds.TryGetValue(productoCodigo, out var id) ? id : _productoIds.Values.FirstOrDefault(1);
    }

    public int GetClienteId(string clienteCodigo)
    {
        return _clienteIds.TryGetValue(clienteCodigo, out var id) ? id : _clienteIds.Values.FirstOrDefault(1);
    }

    public int GetRandomFuenteId()
    {
        return _fuenteIds.Values.Count > 0 ? _fuenteIds.Values.ElementAt(Random.Shared.Next(_fuenteIds.Count)) : 1;
    }

    public int GetRandomProductoId()
    {
        return _productoIds.Values.Count > 0 ? _productoIds.Values.ElementAt(Random.Shared.Next(_productoIds.Count)) : 1;
    }

    public int GetRandomClienteId()
    {
        return _clienteIds.Values.Count > 0 ? _clienteIds.Values.ElementAt(Random.Shared.Next(_clienteIds.Count)) : 1;
    }
}