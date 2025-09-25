namespace OpinionesClientesETL.Interfaces;

public interface ICargador<T> where T : class
{
    Task<int> LoadAsync(IEnumerable<T> datos);
    Task<int> LoadBatchAsync(IEnumerable<T> datos, int batchSize);
    Task<bool> TestConnectionAsync();
}