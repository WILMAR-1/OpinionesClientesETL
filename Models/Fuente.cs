using System.ComponentModel.DataAnnotations;

namespace OpinionesClientesETL.Models;

public class Fuente
{
    public int FuenteID { get; set; }

    [Required]
    [MaxLength(100)]
    public string Nombre { get; set; } = string.Empty;

    [Required]
    [MaxLength(50)]
    public string TipoFuente { get; set; } = string.Empty;

    [MaxLength(255)]
    public string? URL { get; set; }

    [MaxLength(500)]
    public string? Descripcion { get; set; }

    public bool Activa { get; set; } = true;

    public DateTime FechaCreacion { get; set; } = DateTime.Now;
}