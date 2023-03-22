use std::env;

fn main() {
  env::set_var("RUST_LOG", "info");
  ord::debug();
}
