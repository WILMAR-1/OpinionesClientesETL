using System.ComponentModel.DataAnnotations;

namespace OpinionesClientesETL.Models;

public class ComentarioSocial
{
    public int ComentarioID { get; set; }

    public int? ClienteID { get; set; }

    public int ProductoID { get; set; }

    public int FuenteID { get; set; }

    [Required]
    [MaxLength(50)]
    public string PlataformaSocial { get; set; } = string.Empty;

    [MaxLength(100)]
    public string? UsuarioSocial { get; set; }

    [Required]
    [MaxLength(4000)]
    public string TextoComentario { get; set; } = string.Empty;

    public int NumLikes { get; set; } = 0;

    public int NumCompartidos { get; set; } = 0;

    public int NumRespuestas { get; set; } = 0;

    [MaxLength(500)]
    public string? HashtagsPrincipales { get; set; }

    [MaxLength(20)]
    public string? SentimientoAnalizado { get; set; }

    public decimal? ConfianzaSentimiento { get; set; }

    public DateTime FechaPublicacion { get; set; }

    public DateTime FechaExtraccion { get; set; } = DateTime.Now;

    public Cliente? Cliente { get; set; }
    public Producto? Producto { get; set; }
    public Fuente? Fuente { get; set; }
}