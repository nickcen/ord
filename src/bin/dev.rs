use std::env;

fn main() {
  env::set_var("RUST_LOG", "debug");
  ord::debug();
}
