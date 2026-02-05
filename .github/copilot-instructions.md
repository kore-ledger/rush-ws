# Instrucciones de Codificación Rust para GitHub Copilot

## Estilo y Convenciones

- Usa nombres descriptivos en snake_case para variables y funciones
- Usa CamelCase para tipos, structs, enums y traits
- Usa SCREAMING_SNAKE_CASE para constantes
- Prefiere expresiones sobre sentencias cuando sea posible
- Usa `?` para propagación de errores en lugar de `unwrap()` o `expect()` cuando sea apropiado

## Gestión de Errores

- Siempre maneja errores explícitamente, evita `unwrap()` en código de producción
- Usa `Result<T, E>` para operaciones que pueden fallar
- Usa `Option<T>` para valores opcionales
- Implementa el trait `Error` para tipos de error personalizados
- Considera usar `anyhow` o `thiserror` para manejo de errores más ergonómico

## Ownership y Lifetimes

- Prefiere referencias sobre clonación cuando sea posible
- Usa `&str` en lugar de `String` para parámetros de funciones cuando solo se necesite lectura
- Marca lifetimes explícitamente solo cuando el compilador no pueda inferirlos
- Usa `Cow<str>` cuando necesites flexibilidad entre owned y borrowed

## Concurrencia

- Usa `Arc` para compartir datos entre threads de forma segura
- Usa `Mutex` o `RwLock` para sincronización
- Prefiere canales (mpsc, tokio channels) para comunicación entre threads
- Usa `async/await` con tokio o async-std para programación asíncrona

## Documentación

- Documentación en inglés
- Documenta todas las funciones públicas con comentarios `///`
- Incluye ejemplos en la documentación cuando sea relevante
- Documenta panics, errores y casos especiales
- Usa `//!` para documentación a nivel de módulo

## Testing

- Escribe tests unitarios en módulos `#[cfg(test)]`
- Usa `assert!`, `assert_eq!` y `assert_ne!` apropiadamente
- Nombra tests descriptivamente: `test_nombre_funcionalidad_caso_especifico`
- Considera tests de integración en el directorio `tests/`

## Performance

- Usa iteradores en lugar de loops cuando sea posible
- Evita clonaciones innecesarias
- Considera `Vec::with_capacity()` cuando conozcas el tamaño anticipadamente
- Usa `&[T]` en lugar de `&Vec<T>` para parámetros de función

## Patrones Comunes

- Usa pattern matching exhaustivo con `match`
- Prefiere `if let` y `while let` para matching simple
- Usa el operador `?` para propagación de errores
- Implementa `From` y `Into` para conversiones de tipos
- Usa `derive` para traits comunes (Debug, Clone, etc.)

## Seguridad

- Minimiza el uso de `unsafe`
- Documenta claramente los bloques `unsafe` y sus invariantes
- Valida inputs en funciones públicas
- Ten cuidado con integer overflow en modo release