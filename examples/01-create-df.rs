use std::collections::HashMap;

use anyhow::Result;
use polars::prelude::*;
use rand::Rng;

fn main() -> Result<()> {
    create_df()?;
    create_df2()?;
    create_df_json()?;
    create_df_with_list_col()?;

    Ok(())
}

pub fn create_df() -> Result<()> {
    let df = df!(
        "id" => [1, 2, 3],
        "name" => &["foo", "bar", "baz"], 
        "data" => &[42, 43, 44], 
    )?;

    println!("{:?}", df);

    Ok(())
}

pub fn create_df2() -> Result<()> {
    let mut rng = rand::thread_rng();
    
    let df = df!(
        "id" => 0..10,
        "name" => (0..10).map(|_| rng.gen::<i32>()).collect::<Vec<i32>>(),
        "data" => (0..10).map(|_| rng.gen_range(0..100)).collect::<Vec<i32>>(),
    )?;

    println!("{:?}", df);

    Ok(())
}

pub fn create_df_json() -> Result<()> {
    let s1 = Series::new("id", &[1, 2, 3]);

    let map1 = HashMap::from([
        ("val1", 0.4),
        ("val2", 0.7),
        ("val3", 1.0),
    ]);
    let map2 = HashMap::from([
        ("val1", 0.4),
        ("val2", 0.7),
    ]);
    let map3 = HashMap::from([
        ("val1", 0.4),
    ]);
    let j1 = serde_json::to_string(&map1).unwrap();
    let j2 = serde_json::to_string(&map2).unwrap();
    let j3 = serde_json::to_string(&map3).unwrap();

    let s2 = Series::new("metadata", vec![j1, j2, j3]);
    let df = DataFrame::new(vec![s1, s2]).unwrap();
    println!("{:?}", df);

    Ok(())
}

pub fn create_df_with_list_col() -> Result<()> {
    let s1 = Series::new("Fruit", &["Apple", "Ananas", "Pear"]);

    let red = Series::new("Red", &[1, 1, 1]);
    let yellow = Series::new("Yellow", &[10, 10, 10]);
    let green = Series::new("Green", &[101, 112, 1]);
    let colors = vec![red, yellow, green];
    let list = Series::new("Color", colors);

    let df = DataFrame::new(vec![s1, list])?;
    println!("{:?}", df);

    Ok(())
}