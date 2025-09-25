using System.ComponentModel.DataAnnotations;

namespace OpinionesClientesETL.Models;

public class Cliente
{
    public int ClienteID { get; set; }

    [Required]
    [MaxLength(50)]
    public string Codigo { get; set; } = string.Empty;

    [Required]
    [MaxLength(100)]
    public string Nombre { get; set; } = string.Empty;

    [Required]
    [MaxLength(100)]
    public string Apellido { get; set; } = string.Empty;

    [MaxLength(255)]
    public string? Email { get; set; }

    [MaxLength(20)]
    public string? Telefono { get; set; }

    public DateOnly? FechaNacimiento { get; set; }

    [MaxLength(10)]
    public string? Genero { get; set; }

    [MaxLength(100)]
    public string? Ciudad { get; set; }

    [MaxLength(100)]
    public string? Pais { get; set; }

    [MaxLength(50)]
    public string SegmentoCliente { get; set; } = "Regular";

    [MaxLength(20)]
    public string Estado { get; set; } = "Activo";

    public DateTime FechaRegistro { get; set; } = DateTime.Now;

    public DateTime FechaActualizacion { get; set; } = DateTime.Now;
}