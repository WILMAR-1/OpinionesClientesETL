using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using OpinionesClientesETL.Interfaces;
using OpinionesClientesETL.Models;

namespace OpinionesClientesETL.Services;

public class ETLPipeline
{
    private readonly ILogger<ETLPipeline> _logger;
    private readonly IConfiguration _configuration;
    private readonly string _csvFilesPath;
    private IdMappingService? _idMappingService;

    public ETLPipeline(ILogger<ETLPipeline> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        var configPath = _configuration["ETLSettings:CsvFilesPath"] ?? "Data";

        // Si no es una ruta absoluta, usar el directorio del proyecto en lugar del directorio de salida
        if (!Path.IsPathRooted(configPath))
        {
            // Buscar el directorio del proyecto (donde está el .csproj)
            var baseDir = AppContext.BaseDirectory;
            var projectDir = Directory.GetParent(baseDir)?.Parent?.Parent?.Parent?.FullName ?? baseDir;
            _csvFilesPath = Path.Combine(projectDir, configPath);
        }
        else
        {
            _csvFilesPath = configPath;
        }
        _logger.LogInformation($"Ruta de archivos CSV configurada: {_csvFilesPath}");
    }

    public async Task<bool> ExecuteFullPipelineAsync()
    {
        try
        {
            _logger.LogInformation("Iniciando Pipeline ETL completo - Sistema de Análisis de Opiniones de Clientes");
            var startTime = DateTime.Now;

            // Fase 1: Procesar Fuentes (primer paso para establecer relaciones)
            await ProcessEntityAsync<Fuente>("fuente_datos.csv", "Fuentes");

            // Fase 2: Procesar Productos
            await ProcessEntityAsync<Producto>("products.csv", "Productos");

            // Fase 3: Procesar Clientes
            await ProcessEntityAsync<Cliente>("clients.csv", "Clientes");

            // Fase 4: Cargar mapeos de IDs para FK
            await LoadIdMappingsAsync();

            // Fase 5: Procesar Encuestas (requiere Clientes y Productos)
            await ProcessEntityAsync<Encuesta>("surveys_part1.csv", "Encuestas");

            // Fase 6: Procesar Comentarios Sociales
            await ProcessEntityAsync<ComentarioSocial>("social_comments.csv", "Comentarios Sociales");

            // Fase 7: Procesar Reseñas Web
            await ProcessEntityAsync<ReseñaWeb>("web_reviews.csv", "Reseñas Web");

            var endTime = DateTime.Now;
            var duration = endTime - startTime;

            _logger.LogInformation($"Pipeline ETL completado exitosamente en {duration.TotalMinutes:F2} minutos");
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error crítico durante la ejecución del pipeline ETL");
            return false;
        }
    }

    public async Task<bool> ProcessEntityAsync<T>(string csvFileName, string entityName) where T : class
    {
        try
        {
            _logger.LogInformation($"Procesando {entityName}...");
            var startTime = DateTime.Now;

            // Crear servicios específicos para el tipo T
            var extractor = CreateExtractor<T>();
            var transformador = CreateTransformador<T>();
            var cargador = CreateCargador<T>();

            // EXTRACCIÓN
            _logger.LogInformation($"Extrayendo datos de {csvFileName}");
            var csvFilePath = Path.Combine(_csvFilesPath, csvFileName);
            var datosExtraidos = await extractor.ExtractFromCsvAsync(csvFilePath);
            _logger.LogInformation($"Extraídos {datosExtraidos.Count()} registros de {entityName}");

            if (!datosExtraidos.Any())
            {
                _logger.LogWarning($"No se encontraron datos en {csvFileName}");
                return true;
            }

            // TRANSFORMACIÓN
            _logger.LogInformation($"Transformando datos de {entityName}");
            var datosTransformados = await transformador.TransformAsync(datosExtraidos);
            _logger.LogInformation($"Transformados {datosTransformados.Count()} registros válidos de {entityName}");

            if (!datosTransformados.Any())
            {
                _logger.LogWarning($"No hay datos válidos para cargar en {entityName}");
                return true;
            }

            // CARGA
            _logger.LogInformation($"Cargando datos de {entityName} a la base de datos");
            var batchSize = _configuration.GetValue<int>("ETLSettings:BatchSize", 1000);
            var registrosCargados = await cargador.LoadBatchAsync(datosTransformados, batchSize);

            var endTime = DateTime.Now;
            var duration = endTime - startTime;

            _logger.LogInformation($"{entityName} procesado completamente: {registrosCargados} registros cargados en {duration.TotalSeconds:F2} segundos");
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error procesando {entityName}: {ex.Message}");
            return false;
        }
    }

    public async Task<Dictionary<string, int>> GetDatabaseStatsAsync()
    {
        var stats = new Dictionary<string, int>();

        try
        {
            var cargador = CreateCargador<Fuente>();

            if (!await cargador.TestConnectionAsync())
            {
                _logger.LogError("No se puede conectar a la base de datos para obtener estadísticas");
                return stats;
            }

            // Aquí podrías implementar consultas para obtener conteos
            // Por simplicidad, devolvemos un diccionario vacío
            _logger.LogInformation("Estadísticas de base de datos obtenidas");

            return stats;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error obteniendo estadísticas de la base de datos");
            return stats;
        }
    }

    private async Task LoadIdMappingsAsync()
    {
        try
        {
            _logger.LogInformation("Cargando mapeos de IDs para referencias FK...");

            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var logger = loggerFactory.CreateLogger<IdMappingService>();
            _idMappingService = new IdMappingService(logger, _configuration);

            await _idMappingService.LoadFuenteIdsAsync();
            await _idMappingService.LoadProductoIdsAsync();
            await _idMappingService.LoadClienteIdsAsync();

            _logger.LogInformation("Mapeos de IDs cargados exitosamente");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error cargando mapeos de IDs");
            throw;
        }
    }

    private IExtractor<T> CreateExtractor<T>() where T : class
    {
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        var logger = loggerFactory.CreateLogger<CsvExtractor<T>>();
        return new CsvExtractor<T>(logger, _idMappingService);
    }

    private ITransformador<T> CreateTransformador<T>() where T : class
    {
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        var logger = loggerFactory.CreateLogger<DataTransformador<T>>();
        return new DataTransformador<T>(logger);
    }

    private ICargador<T> CreateCargador<T>() where T : class
    {
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        var logger = loggerFactory.CreateLogger<DatabaseCargador<T>>();
        return new DatabaseCargador<T>(logger, _configuration);
    }
}