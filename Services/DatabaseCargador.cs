using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using OpinionesClientesETL.Interfaces;
using OpinionesClientesETL.Models;
using System.Data;

namespace OpinionesClientesETL.Services;

public class DatabaseCargador<T> : ICargador<T> where T : class
{
    private readonly ILogger<DatabaseCargador<T>> _logger;
    private readonly string _connectionString;

    public DatabaseCargador(ILogger<DatabaseCargador<T>> logger, IConfiguration configuration)
    {
        _logger = logger;
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new ArgumentNullException(nameof(configuration), "Connection string not found");
    }

    public async Task<int> LoadAsync(IEnumerable<T> datos)
    {
        return await LoadBatchAsync(datos, 1000);
    }

    public async Task<int> LoadBatchAsync(IEnumerable<T> datos, int batchSize)
    {
        try
        {
            _logger.LogInformation($"Iniciando carga de {datos.Count()} registros con batch size {batchSize}");

            if (!await TestConnectionAsync())
            {
                throw new InvalidOperationException("No se puede conectar a la base de datos");
            }

            int totalLoaded = 0;
            var batches = datos.Chunk(batchSize);

            foreach (var batch in batches)
            {
                int batchLoaded = await ProcessBatchAsync(batch);
                totalLoaded += batchLoaded;
                _logger.LogDebug($"Batch procesado: {batchLoaded} registros cargados");
            }

            _logger.LogInformation($"Carga completada exitosamente: {totalLoaded} registros cargados");
            return totalLoaded;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error durante la carga de datos");
            throw;
        }
    }

    public async Task<bool> TestConnectionAsync()
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            _logger.LogDebug("Conexión a base de datos establecida correctamente");
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error al conectar con la base de datos");
            return false;
        }
    }

    private async Task<int> ProcessBatchAsync(IEnumerable<T> batch)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var transaction = connection.BeginTransaction();

        try
        {
            int loaded = 0;

            foreach (var item in batch)
            {
                if (await InsertEntityAsync(item, connection, transaction))
                {
                    loaded++;
                }
            }

            await transaction.CommitAsync();
            return loaded;
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            _logger.LogError(ex, "Error en batch, realizando rollback");
            throw;
        }
    }

    private async Task<bool> InsertEntityAsync(T entity, SqlConnection connection, SqlTransaction transaction)
    {
        try
        {
            string insertQuery = GetInsertQuery(entity);
            using var command = new SqlCommand(insertQuery, connection, transaction);

            AddParameters(command, entity);

            int result = await command.ExecuteNonQueryAsync();
            return result > 0;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, $"Error al insertar entidad: {ex.Message}");
            return false;
        }
    }

    private string GetInsertQuery(T entity)
    {
        return entity switch
        {
            Fuente => @"
                INSERT INTO Fuentes (Nombre, TipoFuente, URL, Descripcion, Activa, FechaCreacion)
                VALUES (@Nombre, @TipoFuente, @URL, @Descripcion, @Activa, @FechaCreacion)",

            Producto => @"
                INSERT INTO Productos (Codigo, Nombre, Categoria, Subcategoria, Precio, Descripcion, Marca, Estado, FechaCreacion, FechaActualizacion)
                VALUES (@Codigo, @Nombre, @Categoria, @Subcategoria, @Precio, @Descripcion, @Marca, @Estado, @FechaCreacion, @FechaActualizacion)",

            Cliente => @"
                INSERT INTO Clientes (Codigo, Nombre, Apellido, Email, Telefono, FechaNacimiento, Genero, Ciudad, Pais, SegmentoCliente, Estado, FechaRegistro, FechaActualizacion)
                VALUES (@Codigo, @Nombre, @Apellido, @Email, @Telefono, @FechaNacimiento, @Genero, @Ciudad, @Pais, @SegmentoCliente, @Estado, @FechaRegistro, @FechaActualizacion)",

            Encuesta => @"
                INSERT INTO Encuestas (ClienteID, ProductoID, FuenteID, TituloEncuesta, PreguntaPrincipal, CalificacionGeneral, CalificacionCalidad, CalificacionServicio, CalificacionPrecio, Comentario, SentimientoAnalizado, ConfianzaSentimiento, FechaEncuesta, FechaCreacion)
                VALUES (@ClienteID, @ProductoID, @FuenteID, @TituloEncuesta, @PreguntaPrincipal, @CalificacionGeneral, @CalificacionCalidad, @CalificacionServicio, @CalificacionPrecio, @Comentario, @SentimientoAnalizado, @ConfianzaSentimiento, @FechaEncuesta, @FechaCreacion)",

            ComentarioSocial => @"
                INSERT INTO ComentariosSociales (ClienteID, ProductoID, FuenteID, PlataformaSocial, UsuarioSocial, TextoComentario, NumLikes, NumCompartidos, NumRespuestas, HashtagsPrincipales, SentimientoAnalizado, ConfianzaSentimiento, FechaPublicacion, FechaExtraccion)
                VALUES (@ClienteID, @ProductoID, @FuenteID, @PlataformaSocial, @UsuarioSocial, @TextoComentario, @NumLikes, @NumCompartidos, @NumRespuestas, @HashtagsPrincipales, @SentimientoAnalizado, @ConfianzaSentimiento, @FechaPublicacion, @FechaExtraccion)",

            ReseñaWeb => @"
                INSERT INTO ReseñasWeb (ClienteID, ProductoID, FuenteID, SitioWeb, TituloReseña, TextoReseña, CalificacionNumerica, CalificacionEstrellas, UsuarioReseñador, CompraVerificada, VotosUtiles, VotosTotal, SentimientoAnalizado, ConfianzaSentimiento, FechaReseña, FechaExtraccion)
                VALUES (@ClienteID, @ProductoID, @FuenteID, @SitioWeb, @TituloReseña, @TextoReseña, @CalificacionNumerica, @CalificacionEstrellas, @UsuarioReseñador, @CompraVerificada, @VotosUtiles, @VotosTotal, @SentimientoAnalizado, @ConfianzaSentimiento, @FechaReseña, @FechaExtraccion)",

            _ => throw new NotSupportedException($"Tipo de entidad no soportado: {typeof(T).Name}")
        };
    }

    private void AddParameters(SqlCommand command, T entity)
    {
        switch (entity)
        {
            case Fuente fuente:
                command.Parameters.AddWithValue("@Nombre", fuente.Nombre);
                command.Parameters.AddWithValue("@TipoFuente", fuente.TipoFuente);
                command.Parameters.AddWithValue("@URL", (object?)fuente.URL ?? DBNull.Value);
                command.Parameters.AddWithValue("@Descripcion", (object?)fuente.Descripcion ?? DBNull.Value);
                command.Parameters.AddWithValue("@Activa", fuente.Activa);
                command.Parameters.AddWithValue("@FechaCreacion", fuente.FechaCreacion);
                break;

            case Producto producto:
                command.Parameters.AddWithValue("@Codigo", producto.Codigo);
                command.Parameters.AddWithValue("@Nombre", producto.Nombre);
                command.Parameters.AddWithValue("@Categoria", (object?)producto.Categoria ?? DBNull.Value);
                command.Parameters.AddWithValue("@Subcategoria", (object?)producto.Subcategoria ?? DBNull.Value);
                command.Parameters.AddWithValue("@Precio", (object?)producto.Precio ?? DBNull.Value);
                command.Parameters.AddWithValue("@Descripcion", (object?)producto.Descripcion ?? DBNull.Value);
                command.Parameters.AddWithValue("@Marca", (object?)producto.Marca ?? DBNull.Value);
                command.Parameters.AddWithValue("@Estado", producto.Estado);
                command.Parameters.AddWithValue("@FechaCreacion", producto.FechaCreacion);
                command.Parameters.AddWithValue("@FechaActualizacion", producto.FechaActualizacion);
                break;

            case Cliente cliente:
                command.Parameters.AddWithValue("@Codigo", cliente.Codigo);
                command.Parameters.AddWithValue("@Nombre", cliente.Nombre);
                command.Parameters.AddWithValue("@Apellido", cliente.Apellido);
                command.Parameters.AddWithValue("@Email", (object?)cliente.Email ?? DBNull.Value);
                command.Parameters.AddWithValue("@Telefono", (object?)cliente.Telefono ?? DBNull.Value);
                command.Parameters.AddWithValue("@FechaNacimiento", (object?)cliente.FechaNacimiento ?? DBNull.Value);
                command.Parameters.AddWithValue("@Genero", (object?)cliente.Genero ?? DBNull.Value);
                command.Parameters.AddWithValue("@Ciudad", (object?)cliente.Ciudad ?? DBNull.Value);
                command.Parameters.AddWithValue("@Pais", (object?)cliente.Pais ?? DBNull.Value);
                command.Parameters.AddWithValue("@SegmentoCliente", cliente.SegmentoCliente);
                command.Parameters.AddWithValue("@Estado", cliente.Estado);
                command.Parameters.AddWithValue("@FechaRegistro", cliente.FechaRegistro);
                command.Parameters.AddWithValue("@FechaActualizacion", cliente.FechaActualizacion);
                break;

            case Encuesta encuesta:
                command.Parameters.AddWithValue("@ClienteID", encuesta.ClienteID);
                command.Parameters.AddWithValue("@ProductoID", encuesta.ProductoID);
                command.Parameters.AddWithValue("@FuenteID", encuesta.FuenteID);
                command.Parameters.AddWithValue("@TituloEncuesta", encuesta.TituloEncuesta);
                command.Parameters.AddWithValue("@PreguntaPrincipal", (object?)encuesta.PreguntaPrincipal ?? DBNull.Value);
                command.Parameters.AddWithValue("@CalificacionGeneral", (object?)encuesta.CalificacionGeneral ?? DBNull.Value);
                command.Parameters.AddWithValue("@CalificacionCalidad", (object?)encuesta.CalificacionCalidad ?? DBNull.Value);
                command.Parameters.AddWithValue("@CalificacionServicio", (object?)encuesta.CalificacionServicio ?? DBNull.Value);
                command.Parameters.AddWithValue("@CalificacionPrecio", (object?)encuesta.CalificacionPrecio ?? DBNull.Value);
                command.Parameters.AddWithValue("@Comentario", (object?)encuesta.Comentario ?? DBNull.Value);
                command.Parameters.AddWithValue("@SentimientoAnalizado", (object?)encuesta.SentimientoAnalizado ?? DBNull.Value);
                command.Parameters.AddWithValue("@ConfianzaSentimiento", (object?)encuesta.ConfianzaSentimiento ?? DBNull.Value);
                command.Parameters.AddWithValue("@FechaEncuesta", encuesta.FechaEncuesta);
                command.Parameters.AddWithValue("@FechaCreacion", encuesta.FechaCreacion);
                break;

            case ComentarioSocial comentario:
                command.Parameters.AddWithValue("@ClienteID", (object?)comentario.ClienteID ?? DBNull.Value);
                command.Parameters.AddWithValue("@ProductoID", comentario.ProductoID);
                command.Parameters.AddWithValue("@FuenteID", comentario.FuenteID);
                command.Parameters.AddWithValue("@PlataformaSocial", comentario.PlataformaSocial);
                command.Parameters.AddWithValue("@UsuarioSocial", (object?)comentario.UsuarioSocial ?? DBNull.Value);
                command.Parameters.AddWithValue("@TextoComentario", comentario.TextoComentario);
                command.Parameters.AddWithValue("@NumLikes", comentario.NumLikes);
                command.Parameters.AddWithValue("@NumCompartidos", comentario.NumCompartidos);
                command.Parameters.AddWithValue("@NumRespuestas", comentario.NumRespuestas);
                command.Parameters.AddWithValue("@HashtagsPrincipales", (object?)comentario.HashtagsPrincipales ?? DBNull.Value);
                command.Parameters.AddWithValue("@SentimientoAnalizado", (object?)comentario.SentimientoAnalizado ?? DBNull.Value);
                command.Parameters.AddWithValue("@ConfianzaSentimiento", (object?)comentario.ConfianzaSentimiento ?? DBNull.Value);
                command.Parameters.AddWithValue("@FechaPublicacion", comentario.FechaPublicacion);
                command.Parameters.AddWithValue("@FechaExtraccion", comentario.FechaExtraccion);
                break;

            case ReseñaWeb reseña:
                command.Parameters.AddWithValue("@ClienteID", (object?)reseña.ClienteID ?? DBNull.Value);
                command.Parameters.AddWithValue("@ProductoID", reseña.ProductoID);
                command.Parameters.AddWithValue("@FuenteID", reseña.FuenteID);
                command.Parameters.AddWithValue("@SitioWeb", reseña.SitioWeb);
                command.Parameters.AddWithValue("@TituloReseña", (object?)reseña.TituloReseña ?? DBNull.Value);
                command.Parameters.AddWithValue("@TextoReseña", reseña.TextoReseña);
                command.Parameters.AddWithValue("@CalificacionNumerica", (object?)reseña.CalificacionNumerica ?? DBNull.Value);
                command.Parameters.AddWithValue("@CalificacionEstrellas", (object?)reseña.CalificacionEstrellas ?? DBNull.Value);
                command.Parameters.AddWithValue("@UsuarioReseñador", (object?)reseña.UsuarioReseñador ?? DBNull.Value);
                command.Parameters.AddWithValue("@CompraVerificada", reseña.CompraVerificada);
                command.Parameters.AddWithValue("@VotosUtiles", reseña.VotosUtiles);
                command.Parameters.AddWithValue("@VotosTotal", reseña.VotosTotal);
                command.Parameters.AddWithValue("@SentimientoAnalizado", (object?)reseña.SentimientoAnalizado ?? DBNull.Value);
                command.Parameters.AddWithValue("@ConfianzaSentimiento", (object?)reseña.ConfianzaSentimiento ?? DBNull.Value);
                command.Parameters.AddWithValue("@FechaReseña", reseña.FechaReseña);
                command.Parameters.AddWithValue("@FechaExtraccion", reseña.FechaExtraccion);
                break;

            default:
                throw new NotSupportedException($"Tipo de entidad no soportado: {typeof(T).Name}");
        }
    }
}