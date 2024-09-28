use anyhow::Result;
use polars::prelude::*;

fn main() -> Result<()> {
    actions()?;

    Ok(())
}

pub fn actions() -> Result<()> {
    let records = [
        "foo",
        "bar",
        "baz",
    ];

    let mut dfs = Vec::with_capacity(records.len());
    for (id, record) in records.iter().enumerate() {
        let name = record.to_string();
        let data_val = name.as_bytes();
        let id = id as i32;
        let s0 = Series::new("name", &[name.clone()]);
        let s1 = Series::new("data_val", &[data_val]);
        let s2 = Series::new("id", &[id]);
        let df = DataFrame::new(vec![s0, s1, s2]).unwrap();
        dfs.push(df);
    }

    let empty_head = dfs   
        .get(0).expect("df has len 0")
        .clone()
        .lazy()
        .limit(0);

    let df = dfs
        .into_iter()
        .fold(empty_head, |acc, df| concat([acc, df.lazy()], UnionArgs { parallel: true, rechunk: true, to_supertypes: true }).unwrap())
        .collect()?;

    println!("{:?}", df);

    Ok(())
}