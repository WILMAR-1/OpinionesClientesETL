using System.ComponentModel.DataAnnotations;

namespace OpinionesClientesETL.Models;

public class ReseñaWeb
{
    public int ReseñaID { get; set; }

    public int? ClienteID { get; set; }

    public int ProductoID { get; set; }

    public int FuenteID { get; set; }

    [Required]
    [MaxLength(100)]
    public string SitioWeb { get; set; } = string.Empty;

    [MaxLength(300)]
    public string? TituloReseña { get; set; }

    [Required]
    [MaxLength(4000)]
    public string TextoReseña { get; set; } = string.Empty;

    public decimal? CalificacionNumerica { get; set; }

    public int? CalificacionEstrellas { get; set; }

    [MaxLength(100)]
    public string? UsuarioReseñador { get; set; }

    public bool CompraVerificada { get; set; } = false;

    public int VotosUtiles { get; set; } = 0;

    public int VotosTotal { get; set; } = 0;

    [MaxLength(20)]
    public string? SentimientoAnalizado { get; set; }

    public decimal? ConfianzaSentimiento { get; set; }

    public DateTime FechaReseña { get; set; }

    public DateTime FechaExtraccion { get; set; } = DateTime.Now;

    public Cliente? Cliente { get; set; }
    public Producto? Producto { get; set; }
    public Fuente? Fuente { get; set; }
}