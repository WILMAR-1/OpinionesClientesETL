using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpinionesClientesETL.Services;
using System.Diagnostics;

namespace OpinionesClientesETL;

internal class Program
{
    static async Task Main(string[] args)
    {
        // Configurar consola para UTF-8
        Console.OutputEncoding = System.Text.Encoding.UTF8;

        Console.WriteLine("Sistema de Análisis de Opiniones de Clientes - Pipeline ETL");
        Console.WriteLine("================================================================");
        Console.WriteLine("Estudiante: Wilmar Gomez | Matrícula: 2024-0103");
        Console.WriteLine("C3-2025 | Electiva 1 | Prof. Francis Ramirez");
        Console.WriteLine("Desarrollado con .NET 9 | Principios SOLID y POO");
        Console.WriteLine();

        try
        {
            var stopwatch = Stopwatch.StartNew();

            // Crear el host con configuración
            var host = CreateHostBuilder(args).Build();

            using (var scope = host.Services.CreateScope())
            {
                var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
                var pipeline = scope.ServiceProvider.GetRequiredService<ETLPipeline>();

                logger.LogInformation("Iniciando aplicación ETL");

                // Validar argumentos de línea de comandos
                if (args.Length > 0 && args[0] == "--help")
                {
                    ShowHelp();
                    return;
                }

                // Ejecutar pipeline completo
                bool success = await pipeline.ExecuteFullPipelineAsync();

                stopwatch.Stop();

                if (success)
                {
                    Console.WriteLine();
                    Console.WriteLine("PIPELINE ETL COMPLETADO EXITOSAMENTE");
                    Console.WriteLine($"Tiempo total de ejecución: {stopwatch.Elapsed.TotalMinutes:F2} minutos");
                    Console.WriteLine("Para ver los resultados, ejecuta las consultas SQL en tu base de datos");

                    // Mostrar estadísticas básicas
                    await ShowDatabaseStats(pipeline, logger);
                }
                else
                {
                    Console.WriteLine("EL PIPELINE ETL FALLÓ");
                    Console.WriteLine("Revisa los logs para más detalles");
                    Environment.ExitCode = 1;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ERROR CRÍTICO: {ex.Message}");
            Console.WriteLine("Detalles completos del error:");
            Console.WriteLine(ex.ToString());
            Environment.ExitCode = 1;
        }

        Console.WriteLine();
        Console.WriteLine("Proceso completado.");
    }

    static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((context, config) =>
            {
                config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
                config.AddCommandLine(args);
            })
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole(options =>
                {
                    options.IncludeScopes = true;
                    options.TimestampFormat = "[HH:mm:ss] ";
                });
                logging.SetMinimumLevel(LogLevel.Information);
            })
            .ConfigureServices((context, services) =>
            {
                // Registrar servicios
                services.AddSingleton<ETLPipeline>();
            });

    static void ShowHelp()
    {
        Console.WriteLine("AYUDA - Sistema de Análisis de Opiniones de Clientes ETL");
        Console.WriteLine("============================================================");
        Console.WriteLine();
        Console.WriteLine("DESCRIPCIÓN:");
        Console.WriteLine("Pipeline ETL para procesar archivos CSV y cargarlos en SQL Server");
        Console.WriteLine();
        Console.WriteLine("ARCHIVOS CSV ESPERADOS (en la carpeta 'Data/'):");
        Console.WriteLine("  • fuentes.csv          - Fuentes de datos");
        Console.WriteLine("  • productos.csv        - Catálogo de productos");
        Console.WriteLine("  • clientes.csv         - Información de clientes");
        Console.WriteLine("  • encuestas.csv        - Encuestas de satisfacción");
        Console.WriteLine("  • comentarios_sociales.csv - Comentarios de redes sociales");
        Console.WriteLine("  • reseñas_web.csv      - Reseñas de sitios web");
        Console.WriteLine();
        Console.WriteLine("CONFIGURACIÓN:");
        Console.WriteLine("  Edita 'appsettings.json' para configurar:");
        Console.WriteLine("  • Cadena de conexión a SQL Server");
        Console.WriteLine("  • Tamaño de lote para procesamiento");
        Console.WriteLine("  • Ruta de archivos CSV");
        Console.WriteLine();
        Console.WriteLine("USO:");
        Console.WriteLine("  dotnet run              - Ejecutar pipeline completo");
        Console.WriteLine("  dotnet run --help       - Mostrar esta ayuda");
        Console.WriteLine();
        Console.WriteLine("REQUISITOS:");
        Console.WriteLine("  • .NET 9.0");
        Console.WriteLine("  • SQL Server (LocalDB o instancia completa)");
        Console.WriteLine("  • Ejecutar Database.sql antes del primer uso");
    }

    static async Task ShowDatabaseStats(ETLPipeline pipeline, ILogger<Program> logger)
    {
        try
        {
            Console.WriteLine();
            Console.WriteLine("ESTADÍSTICAS DE BASE DE DATOS");
            Console.WriteLine("================================");
            Console.WriteLine("Para obtener estadísticas detalladas, ejecuta estas consultas:");
            Console.WriteLine();
            Console.WriteLine("SELECT 'Fuentes' AS Tabla, COUNT(*) AS Registros FROM Fuentes");
            Console.WriteLine("UNION ALL SELECT 'Productos', COUNT(*) FROM Productos");
            Console.WriteLine("UNION ALL SELECT 'Clientes', COUNT(*) FROM Clientes");
            Console.WriteLine("UNION ALL SELECT 'Encuestas', COUNT(*) FROM Encuestas");
            Console.WriteLine("UNION ALL SELECT 'ComentariosSociales', COUNT(*) FROM ComentariosSociales");
            Console.WriteLine("UNION ALL SELECT 'ReseñasWeb', COUNT(*) FROM ReseñasWeb");
            Console.WriteLine();
            Console.WriteLine("-- Para ver muestra de datos:");
            Console.WriteLine("SELECT TOP 5 * FROM vw_OpinionesPorProducto");
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "No se pudieron obtener estadísticas de la base de datos");
        }
    }
}