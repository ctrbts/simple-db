# SimpleDb - BaseModel

Una librería de abstracción de base de datos ligera y segura para PHP, basada en PDO. Diseñada para funcionar como un query builder simple o una capa tipo ORM en aplicaciones MVC.

Este repositorio contiene una versión modernizada y refactorizada del clásico PDODb manteniendo la compatibilidad con el código existente.

## Requisitos

- PHP 8.0 o superior.
- Extensión PDO habilitada.
- Drivers de base de datos correspondientes (mysql, sqlite, pgsql, sqlsrv).

## Instalación

Instalar vía composer:

```bash
composer require ctrbts/simple-db
```

## Cambios Principales (Refactorización)

Esta librería ha sido actualizada para cumplir con estándares modernos de PHP y seguridad:

1. Namespace: Se ha introducido el namespace SimpleDb. Deberás actualizar tus llamadas new BaseModel(...) a new SimpleDb\BaseModel(...) o agregar use SimpleDb\BaseModel;.
2. Tipado Estricto: Se ha implementado declare(strict_types=1) y tipado fuerte en propiedades y argumentos de métodos. Esto previene errores silenciosos y mejora la estabilidad.
3. Seguridad en Constructor: Se eliminó el uso inseguro de "variables variables" ($$key) en el constructor. Ahora los parámetros se asignan de manera explícita y controlada.
4. Sanitización: Mejorada la sanitización en cláusulas ORDER BY y nombres de tablas para prevenir inyecciones SQL en identificadores.
5. Manejo de Errores: Uso consistente de Exception y PDOException.
6. PHP 8 Match: Uso de expresiones match en lugar de switch para operaciones más limpias y estrictas.

Nota de compatibilidad: Aunque el código interno ha cambiado drásticamente, se han conservado los nombres de todos los métodos públicos y el orden de sus parámetros para asegurar que tu código existente funcione con mínimos cambios.

## Uso Básico


```php
use SimpleDb\BaseModel;
```

### Inicialización

```php
$db = new BaseModel([
    'type' => 'mysql',
    'host' => 'localhost',
    'username' => 'root',
    'password' => 'secret',
    'dbname' => 'mi_app',
    'charset' => 'utf8mb4'
]);
```

### Select (Get)

```php
// Obtener todos los usuarios activos
$users = $db->where('active', 1)
            ->get('users');

// Obtener un solo usuario
$user = $db->where('id', 15)
           ->getOne('users');
```

### Insert

```php
$data = [
    'username' => 'nuevo_usuario',
    'email' => 'email@test.com',
    'created_at' => $db->now()
];

$id = $db->insert('users', $data);
```

### Update

```php
$data = ['active' => 0];
$db->where('last_login', $db->interval('-1 year'), '<');
$db->update('users', $data);
```

### Delete

```php
$db->where('id', 50)->delete('users');
```


## Contribuir

Las contribuciones son bienvenidas. Por favor asegura que cualquier cambio pase los estándares de codificación PSR-12.

## Créditos y Reconocimientos

Esta refactorización y modernización del código fue realizada con asistencia de IA, enfocándose en la seguridad, el tipado estricto y el rendimiento, siguiendo el mismo proceso de calidad aplicado en proyectos similares como la [Refactorización de TimThumb](https://github.com/ctrbts/secure-timthumb).

## Licencia

LGPL v3