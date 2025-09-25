using Microsoft.Extensions.Logging;
using OpinionesClientesETL.Interfaces;
using OpinionesClientesETL.Models;
using System.ComponentModel.DataAnnotations;
using System.Text.RegularExpressions;

namespace OpinionesClientesETL.Services;

public class DataTransformador<T> : ITransformador<T> where T : class
{
    private readonly ILogger<DataTransformador<T>> _logger;

    public DataTransformador(ILogger<DataTransformador<T>> logger)
    {
        _logger = logger;
    }

    public async Task<IEnumerable<T>> TransformAsync(IEnumerable<T> datos)
    {
        try
        {
            _logger.LogInformation($"Iniciando transformación de {datos.Count()} registros");

            var datosProcesados = new List<T>();
            var datosUnicos = RemoveDuplicates(datos);

            foreach (var dato in datosUnicos)
            {
                try
                {
                    var datoLimpio = await ValidateAndCleanAsync(dato);
                    if (datoLimpio != null && await ValidateIntegrityAsync(datoLimpio))
                    {
                        datosProcesados.Add(datoLimpio);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, $"Error al procesar registro: {ex.Message}");
                }
            }

            _logger.LogInformation($"Transformación completada. {datosProcesados.Count} de {datos.Count()} registros procesados exitosamente");
            return datosProcesados;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error durante la transformación de datos");
            throw;
        }
    }

    public async Task<T> ValidateAndCleanAsync(T entidad)
    {
        if (entidad == null) return null!;

        await Task.Run(() =>
        {
            switch (entidad)
            {
                case Cliente cliente:
                    CleanClienteData(cliente);
                    break;
                case Producto producto:
                    CleanProductoData(producto);
                    break;
                case Encuesta encuesta:
                    CleanEncuestaData(encuesta);
                    break;
                case ComentarioSocial comentario:
                    CleanComentarioSocialData(comentario);
                    break;
                case ReseñaWeb reseña:
                    CleanReseñaWebData(reseña);
                    break;
                case Fuente fuente:
                    CleanFuenteData(fuente);
                    break;
            }
        });

        return entidad;
    }

    public async Task<bool> ValidateIntegrityAsync(T entidad)
    {
        if (entidad == null) return false;

        var validationResults = new List<ValidationResult>();
        var validationContext = new ValidationContext(entidad);

        bool isValid = await Task.Run(() =>
            Validator.TryValidateObject(entidad, validationContext, validationResults, true));

        if (!isValid)
        {
            foreach (var error in validationResults)
            {
                _logger.LogWarning($"Error de validación: {error.ErrorMessage}");
            }
        }

        return isValid;
    }

    private IEnumerable<T> RemoveDuplicates(IEnumerable<T> datos)
    {
        var uniqueData = new HashSet<string>();
        var result = new List<T>();

        foreach (var dato in datos)
        {
            var key = GenerateUniqueKey(dato);
            if (!string.IsNullOrEmpty(key) && uniqueData.Add(key))
            {
                result.Add(dato);
            }
        }

        _logger.LogInformation($"Duplicados eliminados: {datos.Count() - result.Count} registros");
        return result;
    }

    private string GenerateUniqueKey(T entidad)
    {
        return entidad switch
        {
            Cliente c => $"CLI_{c.Codigo}_{c.Email}",
            Producto p => $"PRO_{p.Codigo}",
            Encuesta e => $"ENC_{e.ClienteID}_{e.ProductoID}_{e.FechaEncuesta:yyyyMMdd}",
            ComentarioSocial cs => $"COM_{cs.UsuarioSocial}_{cs.ProductoID}_{cs.FechaPublicacion:yyyyMMddHHmm}",
            ReseñaWeb rw => $"RES_{rw.UsuarioReseñador}_{rw.ProductoID}_{rw.FechaReseña:yyyyMMdd}",
            Fuente f => $"FUE_{f.Nombre}_{f.TipoFuente}",
            _ => Guid.NewGuid().ToString()
        };
    }

    private void CleanClienteData(Cliente cliente)
    {
        cliente.Nombre = CleanText(cliente.Nombre);
        cliente.Apellido = CleanText(cliente.Apellido);
        cliente.Email = CleanEmail(cliente.Email);
        cliente.Telefono = CleanPhone(cliente.Telefono);
        cliente.Ciudad = CleanText(cliente.Ciudad);
        cliente.Pais = CleanText(cliente.Pais);

        if (cliente.Genero != null)
        {
            cliente.Genero = cliente.Genero.ToUpper();
            if (!new[] { "M", "F", "OTRO" }.Contains(cliente.Genero))
                cliente.Genero = "Otro";
        }
    }

    private void CleanProductoData(Producto producto)
    {
        producto.Codigo = CleanText(producto.Codigo);
        producto.Nombre = CleanText(producto.Nombre);
        producto.Categoria = CleanText(producto.Categoria);
        producto.Subcategoria = CleanText(producto.Subcategoria);
        producto.Marca = CleanText(producto.Marca);
        producto.Descripcion = CleanLongText(producto.Descripcion);

        if (producto.Precio.HasValue && producto.Precio <= 0)
            producto.Precio = null;
    }

    private void CleanEncuestaData(Encuesta encuesta)
    {
        encuesta.TituloEncuesta = CleanText(encuesta.TituloEncuesta);
        encuesta.PreguntaPrincipal = CleanText(encuesta.PreguntaPrincipal);
        encuesta.Comentario = CleanLongText(encuesta.Comentario);

        encuesta.CalificacionGeneral = ValidateRating(encuesta.CalificacionGeneral, 1, 10);
        encuesta.CalificacionCalidad = ValidateRating(encuesta.CalificacionCalidad, 1, 5);
        encuesta.CalificacionServicio = ValidateRating(encuesta.CalificacionServicio, 1, 5);
        encuesta.CalificacionPrecio = ValidateRating(encuesta.CalificacionPrecio, 1, 5);

        encuesta.SentimientoAnalizado = NormalizeSentiment(encuesta.SentimientoAnalizado);
    }

    private void CleanComentarioSocialData(ComentarioSocial comentario)
    {
        comentario.PlataformaSocial = CleanText(comentario.PlataformaSocial);
        comentario.UsuarioSocial = CleanText(comentario.UsuarioSocial);
        comentario.TextoComentario = CleanLongText(comentario.TextoComentario);
        comentario.HashtagsPrincipales = CleanText(comentario.HashtagsPrincipales);
        comentario.SentimientoAnalizado = NormalizeSentiment(comentario.SentimientoAnalizado);

        if (comentario.NumLikes < 0) comentario.NumLikes = 0;
        if (comentario.NumCompartidos < 0) comentario.NumCompartidos = 0;
        if (comentario.NumRespuestas < 0) comentario.NumRespuestas = 0;
    }

    private void CleanReseñaWebData(ReseñaWeb reseña)
    {
        reseña.SitioWeb = CleanText(reseña.SitioWeb);
        reseña.TituloReseña = CleanText(reseña.TituloReseña);
        reseña.TextoReseña = CleanLongText(reseña.TextoReseña);
        reseña.UsuarioReseñador = CleanText(reseña.UsuarioReseñador);
        reseña.SentimientoAnalizado = NormalizeSentiment(reseña.SentimientoAnalizado);

        if (reseña.CalificacionNumerica.HasValue && (reseña.CalificacionNumerica < 0 || reseña.CalificacionNumerica > 5))
            reseña.CalificacionNumerica = null;

        reseña.CalificacionEstrellas = ValidateRating(reseña.CalificacionEstrellas, 1, 5);

        if (reseña.VotosUtiles < 0) reseña.VotosUtiles = 0;
        if (reseña.VotosTotal < 0) reseña.VotosTotal = 0;
    }

    private void CleanFuenteData(Fuente fuente)
    {
        fuente.Nombre = CleanText(fuente.Nombre);
        fuente.TipoFuente = CleanText(fuente.TipoFuente);
        fuente.Descripcion = CleanLongText(fuente.Descripcion);

        if (!string.IsNullOrEmpty(fuente.URL) && !IsValidUrl(fuente.URL))
            fuente.URL = null;
    }

    private string CleanText(string? input)
    {
        if (string.IsNullOrWhiteSpace(input)) return string.Empty;

        return Regex.Replace(input.Trim(), @"\s+", " ");
    }

    private string? CleanLongText(string? input)
    {
        if (string.IsNullOrWhiteSpace(input)) return null;

        var cleaned = Regex.Replace(input.Trim(), @"\s+", " ");
        return string.IsNullOrEmpty(cleaned) ? null : cleaned;
    }

    private string? CleanEmail(string? email)
    {
        if (string.IsNullOrWhiteSpace(email)) return null;

        email = email.Trim().ToLowerInvariant();
        return IsValidEmail(email) ? email : null;
    }

    private string? CleanPhone(string? phone)
    {
        if (string.IsNullOrWhiteSpace(phone)) return null;

        var cleanPhone = Regex.Replace(phone, @"[^\d+\-\(\)\s]", "");
        return string.IsNullOrEmpty(cleanPhone) ? null : cleanPhone.Trim();
    }

    private int? ValidateRating(int? rating, int min, int max)
    {
        if (!rating.HasValue) return null;
        return rating >= min && rating <= max ? rating : null;
    }

    private string? NormalizeSentiment(string? sentiment)
    {
        if (string.IsNullOrWhiteSpace(sentiment)) return null;

        var normalizedSentiment = sentiment.Trim().ToLowerInvariant();
        return normalizedSentiment switch
        {
            "positivo" or "positive" or "bueno" or "good" => "Positivo",
            "negativo" or "negative" or "malo" or "bad" => "Negativo",
            "neutral" or "neutro" => "Neutral",
            _ => "Neutral"
        };
    }

    private bool IsValidEmail(string email)
    {
        try
        {
            var emailRegex = new Regex(@"^[^@\s]+@[^@\s]+\.[^@\s]+$");
            return emailRegex.IsMatch(email);
        }
        catch
        {
            return false;
        }
    }

    private bool IsValidUrl(string url)
    {
        try
        {
            return Uri.TryCreate(url, UriKind.Absolute, out _);
        }
        catch
        {
            return false;
        }
    }
}