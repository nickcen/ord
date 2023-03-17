use super::*;

#[derive(Debug, Parser)]
pub(crate) enum Db {
  #[clap(about = "index bit transactions")]
  Index,
  #[clap(about = "List index records")]
  Outputs,
}

impl Db {
  pub(crate) fn run(self, options: Options) -> Result {
    let index = Index::open(&options)?;

    match self {
      Self::Index => {
        index.update()?;

        Ok(())
      },
      Self::Outputs => {
        index.outputs()?;
        Ok(())
      },
    }
  }
}
