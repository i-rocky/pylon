// Task 1 scaffold: the catalog and plumbing have no runtime consumer yet, so
// they are only compiled for tests (clippy still covers them via
// --all-targets). Later tasks replace this with plain `mod catalog;` /
// `mod plumbing;` alongside `mod server;` (server::wait_ready will call
// plumbing::http_get).
#[cfg(test)]
mod catalog;

#[cfg(test)]
mod plumbing;

fn main() {}
