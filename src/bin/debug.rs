use std::env;

fn main() {
  env::set_var("RUST_LOG", "trace");
  ord::debug();
}
