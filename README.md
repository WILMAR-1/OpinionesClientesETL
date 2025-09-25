# Customer Opinions Analysis ETL Pipeline

ETL pipeline for processing customer opinion data from CSV files to SQL Server database.

## Features

- **Extract**: Read CSV files (products, customers, surveys, social comments, web reviews, sources)
- **Transform**: Data cleaning, validation, and normalization
- **Load**: Batch insertion to SQL Server with referential integrity

## Architecture

Built with .NET 9 following SOLID principles:

- `IExtractor<T>`: CSV data extraction
- `ITransformador<T>`: Data transformation and validation
- `ICargador<T>`: Database loading with transactions
- `ETLPipeline`: Main orchestration service

## Database Model

```
Fuentes ←─── Encuestas ────→ Clientes
  ↑              ↑               ↑
  │              ├─→ Productos ←─┤
  │              ↓               │
  ├── ComentariosSociales ←──────┤
  └── ReseñasWeb ←───────────────┘
```

## Quick Start

1. **Setup Database**:
   ```bash
   sqlcmd -S . -i Database.sql
   ```

2. **Configure Connection**:
   Update `appsettings.json` connection string

3. **Run Pipeline**:
   ```bash
   dotnet run
   ```

## CSV Files Required

Place in `/Data` folder:
- `fuentes.csv`
- `productos.csv`
- `clientes.csv`
- `encuestas.csv`
- `comentarios_sociales.csv`
- `reseñas_web.csv`

## Built With

- .NET 9
- CsvHelper
- Microsoft.Data.SqlClient
- Microsoft.Extensions.Logging