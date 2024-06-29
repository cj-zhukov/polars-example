use std::time::Instant;

use anyhow::Result;

use dev::*;
use examples::*;
use polars::prelude::*;

fn main() -> Result<()> {
    let now = Instant::now();

    let df = get_df()?;
    println!("{:?}", df);
    // write_df_to_file(&mut res, "data/foo_pa.parquet")?;

    println!("end processing elapsed: {:.2?}", now.elapsed());

    Ok(())
}   
