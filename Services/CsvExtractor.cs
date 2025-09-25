using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Logging;
using OpinionesClientesETL.Interfaces;
using System.Globalization;
using System.Text;

namespace OpinionesClientesETL.Services;

public class CsvExtractor<T> : IExtractor<T> where T : class
{
    private readonly ILogger<CsvExtractor<T>> _logger;
    private readonly CsvConfiguration _csvConfig;

    public CsvExtractor(ILogger<CsvExtractor<T>> logger)
    {
        _logger = logger;
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
}