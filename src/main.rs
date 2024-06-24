use std::time::Instant;

use anyhow::Result;

use dev::*;

fn main() -> Result<()> {
    let now = Instant::now();

    // let df = get_df()?;
    // let df = df
    //     .select(["name", "data"])?
    //     .into_struct("metadata")
    //     .into_series()
    //     .into_frame();

    // println!("{:?}", df);

    println!("end processing elapsed: {:.2?}", now.elapsed());

    Ok(())
}   
