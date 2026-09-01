// Task 1 scaffold: the catalog has no runtime consumer yet, so it is only
// compiled for tests (clippy still covers it via --all-targets). Task 2
// replaces this with plain `mod catalog;` alongside `mod server;`.
#[cfg(test)]
mod catalog;

fn main() {}
