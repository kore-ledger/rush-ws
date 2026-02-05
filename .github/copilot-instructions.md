# Rust Coding Instructions for GitHub Copilot

## Style and Conventions

- Use descriptive names in snake_case for variables and functions
- Use CamelCase for types, structs, enums, and traits
- Use SCREAMING_SNAKE_CASE for constants
- Prefer expressions over statements when possible
- Use `?` for error propagation instead of `unwrap()` or `expect()` when appropriate

## Error Handling

- Always handle errors explicitly, avoid `unwrap()` in production code
- Use `Result<T, E>` for operations that can fail
- Use `Option<T>` for optional values
- Implement the `Error` trait for custom error types
- Consider using `anyhow` or `thiserror` for more ergonomic error handling

## Ownership and Lifetimes

- Prefer references over cloning when possible
- Use `&str` instead of `String` for function parameters when only reading is needed
- Mark lifetimes explicitly only when the compiler cannot infer them
- Use `Cow<str>` when you need flexibility between owned and borrowed

## Concurrency

- Use `Arc` to share data between threads safely
- Use `Mutex` or `RwLock` for synchronization
- Prefer channels (mpsc, tokio channels) for communication between threads
- Use `async/await` with tokio or async-std for asynchronous programming

## Documentation

- Documentation in English
- Document all public functions with `///` comments
- Include examples in documentation when relevant
- Document panics, errors, and special cases
- Use `//!` for module-level documentation

## Testing

- Write unit tests in `#[cfg(test)]` modules
- Use `assert!`, `assert_eq!`, and `assert_ne!` appropriately
- Name tests descriptively: `test_function_name_specific_case`
- Consider integration tests in the `tests/` directory

## Performance

- Use iterators instead of loops when possible
- Avoid unnecessary cloning
- Consider `Vec::with_capacity()` when you know the size in advance
- Use `&[T]` instead of `&Vec<T>` for function parameters

## Common Patterns

- Use exhaustive pattern matching with `match`
- Prefer `if let` and `while let` for simple matching
- Use the `?` operator for error propagation
- Implement `From` and `Into` for type conversions
- Use `derive` for common traits (Debug, Clone, etc.)

## Safety

- Minimize the use of `unsafe`
- Clearly document `unsafe` blocks and their invariants
- Validate inputs in public functions
- Be careful with integer overflow in release mode