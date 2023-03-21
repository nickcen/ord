use super::*;

#[derive(Debug, Parser)]
pub(crate) enum Indexer {
  #[clap(about = "index bit transactions")]
  Index,
  #[clap(about = "List index records")]
  Outputs,
}

impl Indexer {
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
