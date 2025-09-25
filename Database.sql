-- ============================================
-- Sistema de Análisis de Opiniones de Clientes
-- Pipeline ETL - Modelo de Base de Datos
-- ============================================

-- Crear base de datos
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'OpinionesClientesDB')
BEGIN
    CREATE DATABASE OpinionesClientesDB;
END
GO

USE OpinionesClientesDB;
GO

-- ============================================
-- TABLA: Fuentes
-- Almacena las fuentes de datos de opiniones
-- ============================================
CREATE TABLE Fuentes (
    FuenteID INT IDENTITY(1,1) PRIMARY KEY,
    Nombre NVARCHAR(100) NOT NULL,
    TipoFuente NVARCHAR(50) NOT NULL CHECK (TipoFuente IN ('Red Social', 'Web', 'Encuesta', 'API', 'CSV')),
    URL NVARCHAR(255),
    Descripcion NVARCHAR(500),
    Activa BIT DEFAULT 1,
    FechaCreacion DATETIME2 DEFAULT GETDATE()
);

-- ============================================
-- TABLA: Productos
-- Catálogo de productos del sistema
-- ============================================
CREATE TABLE Productos (
    ProductoID INT IDENTITY(1,1) PRIMARY KEY,
    Codigo NVARCHAR(50) NOT NULL UNIQUE,
    Nombre NVARCHAR(200) NOT NULL,
    Categoria NVARCHAR(100),
    Subcategoria NVARCHAR(100),
    Precio DECIMAL(10,2),
    Descripcion NVARCHAR(1000),
    Marca NVARCHAR(100),
    Estado NVARCHAR(20) DEFAULT 'Activo' CHECK (Estado IN ('Activo', 'Inactivo', 'Descontinuado')),
    FechaCreacion DATETIME2 DEFAULT GETDATE(),
    FechaActualizacion DATETIME2 DEFAULT GETDATE()
);

-- ============================================
-- TABLA: Clientes
-- Información de clientes del sistema
-- ============================================
CREATE TABLE Clientes (
    ClienteID INT IDENTITY(1,1) PRIMARY KEY,
    Codigo NVARCHAR(50) NOT NULL UNIQUE,
    Nombre NVARCHAR(100) NOT NULL,
    Apellido NVARCHAR(100) NOT NULL,
    Email NVARCHAR(255) UNIQUE,
    Telefono NVARCHAR(20),
    FechaNacimiento DATE,
    Genero NVARCHAR(10) CHECK (Genero IN ('M', 'F', 'Otro')),
    Ciudad NVARCHAR(100),
    Pais NVARCHAR(100),
    SegmentoCliente NVARCHAR(50) DEFAULT 'Regular',
    Estado NVARCHAR(20) DEFAULT 'Activo' CHECK (Estado IN ('Activo', 'Inactivo')),
    FechaRegistro DATETIME2 DEFAULT GETDATE(),
    FechaActualizacion DATETIME2 DEFAULT GETDATE()
);

-- ============================================
-- TABLA: Encuestas
-- Encuestas formales realizadas a clientes
-- ============================================
CREATE TABLE Encuestas (
    EncuestaID INT IDENTITY(1,1) PRIMARY KEY,
    ClienteID INT NOT NULL,
    ProductoID INT NOT NULL,
    FuenteID INT NOT NULL,
    TituloEncuesta NVARCHAR(200) NOT NULL,
    PreguntaPrincipal NVARCHAR(500),
    CalificacionGeneral INT CHECK (CalificacionGeneral BETWEEN 1 AND 10),
    CalificacionCalidad INT CHECK (CalificacionCalidad BETWEEN 1 AND 5),
    CalificacionServicio INT CHECK (CalificacionServicio BETWEEN 1 AND 5),
    CalificacionPrecio INT CHECK (CalificacionPrecio BETWEEN 1 AND 5),
    Comentario NVARCHAR(2000),
    SentimientoAnalizado NVARCHAR(20) CHECK (SentimientoAnalizado IN ('Positivo', 'Negativo', 'Neutral')),
    ConfianzaSentimiento DECIMAL(3,2) CHECK (ConfianzaSentimiento BETWEEN 0 AND 1),
    FechaEncuesta DATETIME2 NOT NULL,
    FechaCreacion DATETIME2 DEFAULT GETDATE(),

    -- Claves foráneas
    CONSTRAINT FK_Encuestas_Cliente FOREIGN KEY (ClienteID) REFERENCES Clientes(ClienteID),
    CONSTRAINT FK_Encuestas_Producto FOREIGN KEY (ProductoID) REFERENCES Productos(ProductoID),
    CONSTRAINT FK_Encuestas_Fuente FOREIGN KEY (FuenteID) REFERENCES Fuentes(FuenteID)
);

-- ============================================
-- TABLA: ComentariosSociales
-- Comentarios de redes sociales y plataformas
-- ============================================
CREATE TABLE ComentariosSociales (
    ComentarioID INT IDENTITY(1,1) PRIMARY KEY,
    ClienteID INT,
    ProductoID INT NOT NULL,
    FuenteID INT NOT NULL,
    PlataformaSocial NVARCHAR(50) NOT NULL CHECK (PlataformaSocial IN ('Facebook', 'Twitter', 'Instagram', 'LinkedIn', 'TikTok', 'YouTube')),
    UsuarioSocial NVARCHAR(100),
    TextoComentario NVARCHAR(4000) NOT NULL,
    NumLikes INT DEFAULT 0,
    NumCompartidos INT DEFAULT 0,
    NumRespuestas INT DEFAULT 0,
    HashtagsPrincipales NVARCHAR(500),
    SentimientoAnalizado NVARCHAR(20) CHECK (SentimientoAnalizado IN ('Positivo', 'Negativo', 'Neutral')),
    ConfianzaSentimiento DECIMAL(3,2) CHECK (ConfianzaSentimiento BETWEEN 0 AND 1),
    FechaPublicacion DATETIME2 NOT NULL,
    FechaExtraccion DATETIME2 DEFAULT GETDATE(),

    -- Claves foráneas
    CONSTRAINT FK_ComentariosSociales_Cliente FOREIGN KEY (ClienteID) REFERENCES Clientes(ClienteID),
    CONSTRAINT FK_ComentariosSociales_Producto FOREIGN KEY (ProductoID) REFERENCES Productos(ProductoID),
    CONSTRAINT FK_ComentariosSociales_Fuente FOREIGN KEY (FuenteID) REFERENCES Fuentes(FuenteID)
);

-- ============================================
-- TABLA: ReseñasWeb
-- Reseñas de sitios web y plataformas de comercio
-- ============================================
CREATE TABLE ReseñasWeb (
    ReseñaID INT IDENTITY(1,1) PRIMARY KEY,
    ClienteID INT,
    ProductoID INT NOT NULL,
    FuenteID INT NOT NULL,
    SitioWeb NVARCHAR(100) NOT NULL,
    TituloReseña NVARCHAR(300),
    TextoReseña NVARCHAR(4000) NOT NULL,
    CalificacionNumerica DECIMAL(3,2) CHECK (CalificacionNumerica BETWEEN 0 AND 5),
    CalificacionEstrellas INT CHECK (CalificacionEstrellas BETWEEN 1 AND 5),
    UsuarioReseñador NVARCHAR(100),
    CompraVerificada BIT DEFAULT 0,
    VotosUtiles INT DEFAULT 0,
    VotosTotal INT DEFAULT 0,
    SentimientoAnalizado NVARCHAR(20) CHECK (SentimientoAnalizado IN ('Positivo', 'Negativo', 'Neutral')),
    ConfianzaSentimiento DECIMAL(3,2) CHECK (ConfianzaSentimiento BETWEEN 0 AND 1),
    FechaReseña DATETIME2 NOT NULL,
    FechaExtraccion DATETIME2 DEFAULT GETDATE(),

    -- Claves foráneas
    CONSTRAINT FK_ReseñasWeb_Cliente FOREIGN KEY (ClienteID) REFERENCES Clientes(ClienteID),
    CONSTRAINT FK_ReseñasWeb_Producto FOREIGN KEY (ProductoID) REFERENCES Productos(ProductoID),
    CONSTRAINT FK_ReseñasWeb_Fuente FOREIGN KEY (FuenteID) REFERENCES Fuentes(FuenteID)
);

-- ============================================
-- ÍNDICES PARA OPTIMIZACIÓN DE CONSULTAS
-- ============================================

-- Índices en Encuestas
CREATE INDEX IX_Encuestas_Cliente ON Encuestas(ClienteID);
CREATE INDEX IX_Encuestas_Producto ON Encuestas(ProductoID);
CREATE INDEX IX_Encuestas_Fecha ON Encuestas(FechaEncuesta);
CREATE INDEX IX_Encuestas_Sentimiento ON Encuestas(SentimientoAnalizado);

-- Índices en ComentariosSociales
CREATE INDEX IX_ComentariosSociales_Cliente ON ComentariosSociales(ClienteID);
CREATE INDEX IX_ComentariosSociales_Producto ON ComentariosSociales(ProductoID);
CREATE INDEX IX_ComentariosSociales_Fecha ON ComentariosSociales(FechaPublicacion);
CREATE INDEX IX_ComentariosSociales_Plataforma ON ComentariosSociales(PlataformaSocial);

-- Índices en ReseñasWeb
CREATE INDEX IX_ReseñasWeb_Cliente ON ReseñasWeb(ClienteID);
CREATE INDEX IX_ReseñasWeb_Producto ON ReseñasWeb(ProductoID);
CREATE INDEX IX_ReseñasWeb_Fecha ON ReseñasWeb(FechaReseña);
CREATE INDEX IX_ReseñasWeb_Sitio ON ReseñasWeb(SitioWeb);

-- ============================================
-- VISTAS PARA ANÁLISIS
-- ============================================

-- Vista resumen de opiniones por producto
CREATE VIEW vw_OpinionesPorProducto AS
SELECT
    p.ProductoID,
    p.Nombre AS ProductoNombre,
    p.Categoria,
    COUNT(e.EncuestaID) AS TotalEncuestas,
    COUNT(cs.ComentarioID) AS TotalComentarios,
    COUNT(rw.ReseñaID) AS TotalReseñas,
    AVG(CAST(e.CalificacionGeneral AS FLOAT)) AS PromedioCalificacion,
    COUNT(CASE WHEN e.SentimientoAnalizado = 'Positivo' THEN 1 END) +
    COUNT(CASE WHEN cs.SentimientoAnalizado = 'Positivo' THEN 1 END) +
    COUNT(CASE WHEN rw.SentimientoAnalizado = 'Positivo' THEN 1 END) AS OpinionesPositivas,
    COUNT(CASE WHEN e.SentimientoAnalizado = 'Negativo' THEN 1 END) +
    COUNT(CASE WHEN cs.SentimientoAnalizado = 'Negativo' THEN 1 END) +
    COUNT(CASE WHEN rw.SentimientoAnalizado = 'Negativo' THEN 1 END) AS OpinionesNegativas
FROM Productos p
LEFT JOIN Encuestas e ON p.ProductoID = e.ProductoID
LEFT JOIN ComentariosSociales cs ON p.ProductoID = cs.ProductoID
LEFT JOIN ReseñasWeb rw ON p.ProductoID = rw.ProductoID
GROUP BY p.ProductoID, p.Nombre, p.Categoria;

GO

PRINT 'Base de datos OpinionesClientesDB creada exitosamente con todas las tablas, índices y vistas.';