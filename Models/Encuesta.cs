using System.ComponentModel.DataAnnotations;

namespace OpinionesClientesETL.Models;

public class Encuesta
{
    public int EncuestaID { get; set; }

    public int ClienteID { get; set; }

    public int ProductoID { get; set; }

    public int FuenteID { get; set; }

    [Required]
    [MaxLength(200)]
    public string TituloEncuesta { get; set; } = string.Empty;

    [MaxLength(500)]
    public string? PreguntaPrincipal { get; set; }

    public int? CalificacionGeneral { get; set; }

    public int? CalificacionCalidad { get; set; }

    public int? CalificacionServicio { get; set; }

    public int? CalificacionPrecio { get; set; }

    [MaxLength(2000)]
    public string? Comentario { get; set; }

    [MaxLength(20)]
    public string? SentimientoAnalizado { get; set; }

    public decimal? ConfianzaSentimiento { get; set; }

    public DateTime FechaEncuesta { get; set; }

    public DateTime FechaCreacion { get; set; } = DateTime.Now;

    public Cliente? Cliente { get; set; }
    public Producto? Producto { get; set; }
    public Fuente? Fuente { get; set; }
}