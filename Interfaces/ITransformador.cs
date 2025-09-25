namespace OpinionesClientesETL.Interfaces;

public interface ITransformador<T> where T : class
{
    Task<IEnumerable<T>> TransformAsync(IEnumerable<T> datos);
    Task<T> ValidateAndCleanAsync(T entidad);
    Task<bool> ValidateIntegrityAsync(T entidad);
}