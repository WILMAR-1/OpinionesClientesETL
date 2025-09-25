using System.ComponentModel.DataAnnotations;

namespace OpinionesClientesETL.Models;

public class Producto
{
    public int ProductoID { get; set; }

    [Required]
    [MaxLength(50)]
    public string Codigo { get; set; } = string.Empty;

    [Required]
    [MaxLength(200)]
    public string Nombre { get; set; } = string.Empty;

    [MaxLength(100)]
    public string? Categoria { get; set; }

    [MaxLength(100)]
    public string? Subcategoria { get; set; }

    public decimal? Precio { get; set; }

    [MaxLength(1000)]
    public string? Descripcion { get; set; }

    [MaxLength(100)]
    public string? Marca { get; set; }

    [MaxLength(20)]
    public string Estado { get; set; } = "Activo";

    public DateTime FechaCreacion { get; set; } = DateTime.Now;

    public DateTime FechaActualizacion { get; set; } = DateTime.Now;
}