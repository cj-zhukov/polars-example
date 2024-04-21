use std::time::Instant;

use dev::*;


fn main() -> anyhow::Result<()> {
    let now = Instant::now();

    let df = get_df()?;
    println!("{:?}", df);

    println!("end processing elapsed: {:.2?}", now.elapsed());

    Ok(())
}   
