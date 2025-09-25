using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Logging;
using OpinionesClientesETL.Interfaces;
using OpinionesClientesETL.Models;
using System.Globalization;
using System.Text;

namespace OpinionesClientesETL.Services;

public class CsvExtractor<T> : IExtractor<T> where T : class
{
    private readonly ILogger<CsvExtractor<T>> _logger;
    private readonly CsvConfiguration _csvConfig;
    private readonly IdMappingService? _idMappingService;

    public CsvExtractor(ILogger<CsvExtractor<T>> logger, IdMappingService? idMappingService = null)
    {
        _logger = logger;
        _idMappingService = idMappingService;
        _csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            MissingFieldFound = null,
            BadDataFound = null,
            HeaderValidated = null,
            Encoding = Encoding.UTF8
        };
    }

    public async Task<IEnumerable<T>> ExtractFromCsvAsync(string filePath)
    {
        try
        {
            _logger.LogInformation($"Iniciando extracción de datos desde: {filePath}");

            if (!await ValidateFileAsync(filePath))
            {
                throw new FileNotFoundException($"El archivo no existe o no es válido: {filePath}");
            }

            var records = new List<T>();

            using var reader = new StringReader(await File.ReadAllTextAsync(filePath, Encoding.UTF8));
            using var csv = new CsvReader(reader, _csvConfig);

            // Registrar mapeos personalizados
            RegisterClassMaps(csv.Context);

            await foreach (var record in csv.GetRecordsAsync<T>())
            {
                if (record != null)
                {
                    records.Add(record);
                }
            }

            _logger.LogInformation($"Extracción completada. {records.Count} registros extraídos de {filePath}");
            return records;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error durante la extracción de datos desde {filePath}");
            throw;
        }
    }

    public async Task<bool> ValidateFileAsync(string filePath)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(filePath))
            {
                _logger.LogWarning("La ruta del archivo está vacía");
                return false;
            }

            if (!File.Exists(filePath))
            {
                _logger.LogWarning($"El archivo no existe: {filePath}");
                return false;
            }

            var fileInfo = new FileInfo(filePath);
            if (fileInfo.Length == 0)
            {
                _logger.LogWarning($"El archivo está vacío: {filePath}");
                return false;
            }

            if (!filePath.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning($"El archivo no tiene extensión .csv: {filePath}");
                return false;
            }

            var firstLine = await File.ReadAllLinesAsync(filePath).ConfigureAwait(false);
            if (firstLine.Length == 0)
            {
                _logger.LogWarning($"El archivo no tiene contenido: {filePath}");
                return false;
            }

            _logger.LogDebug($"Archivo validado correctamente: {filePath}");
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error durante la validación del archivo: {filePath}");
            return false;
        }
    }

    private void RegisterClassMaps(CsvContext context)
    {
        var entityType = typeof(T);

        if (entityType == typeof(Fuente))
        {
            context.RegisterClassMap<FuenteMap>();
        }
        else if (entityType == typeof(Producto))
        {
            context.RegisterClassMap<ProductoMap>();
        }
        else if (entityType == typeof(Cliente))
        {
            context.RegisterClassMap<ClienteMap>();
        }
        else if (entityType == typeof(Encuesta))
        {
            context.RegisterClassMap(new EncuestaMap(_idMappingService));
        }
        else if (entityType == typeof(ComentarioSocial))
        {
            context.RegisterClassMap(new ComentarioSocialMap(_idMappingService));
        }
        else if (entityType == typeof(ReseñaWeb))
        {
            context.RegisterClassMap(new ReseñaWebMap(_idMappingService));
        }
    }
}

// Mapeos de CSV a entidades
public sealed class FuenteMap : ClassMap<Fuente>
{
    public FuenteMap()
    {
        Map(m => m.FuenteID).Ignore(); // Auto-increment in database
        Map(m => m.Nombre).Name("IdFuente").Convert(args => $"Fuente_{args.Row.GetField("IdFuente")}");
        Map(m => m.TipoFuente).Name("TipoFuente");
        Map(m => m.URL).Optional();
        Map(m => m.Descripcion).Optional();
        Map(m => m.Activa).Optional().Default(true);
        Map(m => m.FechaCreacion).Name("FechaCarga").Convert(args =>
        {
            if (DateTime.TryParse(args.Row.GetField("FechaCarga"), out var fecha))
                return fecha;
            return DateTime.Now;
        });
    }
}

public sealed class ProductoMap : ClassMap<Producto>
{
    public ProductoMap()
    {
        Map(m => m.ProductoID).Name("IdProducto").Optional();
        Map(m => m.Codigo).Name("IdProducto").Convert(args => $"PROD_{args.Row.GetField("IdProducto")}");
        Map(m => m.Nombre).Name("Nombre");
        Map(m => m.Categoria).Name("Categoría");
        Map(m => m.Subcategoria).Optional();
        Map(m => m.Precio).Optional();
        Map(m => m.Descripcion).Optional();
        Map(m => m.Marca).Optional();
        Map(m => m.Estado).Optional().Default("Activo");
        Map(m => m.FechaCreacion).Optional().Default(DateTime.Now);
        Map(m => m.FechaActualizacion).Optional().Default(DateTime.Now);
    }
}

public sealed class ClienteMap : ClassMap<Cliente>
{
    public ClienteMap()
    {
        Map(m => m.ClienteID).Name("IdCliente").Optional();
        Map(m => m.Codigo).Name("IdCliente").Convert(args => $"CLI_{args.Row.GetField("IdCliente")}");
        Map(m => m.Nombre).Name("Nombre");
        Map(m => m.Apellido).Convert((ConvertFromString<string>)(args => "Apellido_Generado"));
        Map(m => m.Email).Name("Email");
        Map(m => m.Telefono).Optional();
        Map(m => m.FechaNacimiento).Optional();
        Map(m => m.Genero).Optional();
        Map(m => m.Ciudad).Optional();
        Map(m => m.Pais).Optional();
        Map(m => m.SegmentoCliente).Optional().Default("Regular");
        Map(m => m.Estado).Optional().Default("Activo");
        Map(m => m.FechaRegistro).Optional().Default(DateTime.Now);
        Map(m => m.FechaActualizacion).Optional().Default(DateTime.Now);
    }
}

public sealed class EncuestaMap : ClassMap<Encuesta>
{
    public EncuestaMap(IdMappingService? idMappingService)
    {
        Map(m => m.ClienteID).Convert(args => idMappingService?.GetRandomClienteId() ?? 1);
        Map(m => m.ProductoID).Convert(args => idMappingService?.GetRandomProductoId() ?? 1);
        Map(m => m.FuenteID).Convert(args => idMappingService?.GetRandomFuenteId() ?? 1);
        Map(m => m.TituloEncuesta).Optional().Default("Encuesta Importada");
        Map(m => m.PreguntaPrincipal).Optional();
        Map(m => m.CalificacionGeneral).Optional();
        Map(m => m.Comentario).Optional();
        Map(m => m.SentimientoAnalizado).Optional().Default("Neutral");
        Map(m => m.FechaEncuesta).Optional().Default(DateTime.Now);
    }
}

public sealed class ComentarioSocialMap : ClassMap<ComentarioSocial>
{
    public ComentarioSocialMap(IdMappingService? idMappingService)
    {
        Map(m => m.ProductoID).Convert(args => idMappingService?.GetRandomProductoId() ?? 1);
        Map(m => m.FuenteID).Convert(args => idMappingService?.GetRandomFuenteId() ?? 1);
        Map(m => m.PlataformaSocial).Optional().Default("Facebook");
        Map(m => m.TextoComentario).Optional().Default("Comentario importado");
        Map(m => m.SentimientoAnalizado).Optional().Default("Neutral");
        Map(m => m.FechaPublicacion).Optional().Default(DateTime.Now);
    }
}

public sealed class ReseñaWebMap : ClassMap<ReseñaWeb>
{
    public ReseñaWebMap(IdMappingService? idMappingService)
    {
        Map(m => m.ProductoID).Convert(args => idMappingService?.GetRandomProductoId() ?? 1);
        Map(m => m.FuenteID).Convert(args => idMappingService?.GetRandomFuenteId() ?? 1);
        Map(m => m.SitioWeb).Optional().Default("Amazon");
        Map(m => m.TextoReseña).Optional().Default("Reseña importada");
        Map(m => m.SentimientoAnalizado).Optional().Default("Neutral");
        Map(m => m.FechaReseña).Optional().Default(DateTime.Now);
    }
}