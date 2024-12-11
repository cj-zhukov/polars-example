use anyhow::Result;
use polars::prelude::*;
use polars_sql::SQLContext;

fn main() -> Result<()> {
    query1()?;

    Ok(())
}

pub fn query1() -> Result<()> {
    let df = df!(
        "id" => [1, 2, 3],
        "name" => &["foo", "bar", "baz"], 
        "data" => &[42, 43, 44], 
    )?.lazy();

    let mut ctx = SQLContext::new();
    ctx.register("t", df);
    let query = "select * from t where id > 2 and name in ('foo', 'bar', 'baz')";
    let res = ctx.execute(query)?.collect()?;
    println!("{:?}", res);

    Ok(())
}