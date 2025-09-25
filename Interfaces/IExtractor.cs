namespace OpinionesClientesETL.Interfaces;

public interface IExtractor<T> where T : class
{
    Task<IEnumerable<T>> ExtractFromCsvAsync(string filePath);
    Task<bool> ValidateFileAsync(string filePath);
}