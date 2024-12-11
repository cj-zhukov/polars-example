use anyhow::Result;
use polars::prelude::*;

fn main() -> Result<()> {
    join()?;

    Ok(())
}

pub fn join() -> Result<()> {
    let df1 = df!(
        "id" => [1, 2, 3],
        "name" => &["foo", "bar", "baz"], 
        "data" => &[42, 43, 44], 
    )?;

    let df2 = df!(
        "id" => [1, 2, 3],
        "vals" => &[42, 43, 44], 
    )?;

    let res = df1.join(&df2, &["id"], &["id"], JoinArgs { how: JoinType::Inner, validation: JoinValidation::ManyToMany, suffix: None, slice: None, join_nulls: false })?;

    println!("{:?}", res);

    Ok(())
}

