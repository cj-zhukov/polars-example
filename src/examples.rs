use crate::utils::{join_dfs, partition_df};

use std::collections::HashMap;

use anyhow::Result;
use polars::prelude::*;

pub fn get_df() -> Result<DataFrame> {
    let df = df!(
        "id" => [1, 2, 3],
        "name" => &["foo", "bar", "baz"], 
        "data" => &[42, 43, 44], 
    )?;

    Ok(df)
}

pub fn read_data_to_df_example() {
    let imgs = [
        "image_1.png",
        "image_2.png",
    ];

    let mut dfs = Vec::with_capacity(imgs.len());
    for img in imgs.iter() {
        let img_name = img.to_string();
        let data_val = "data val".to_string();
        let id = "some_id";
        let s0 = Series::new("img_name", &[img_name.clone()]);
        let s1 = Series::new("data_val", &[data_val]);
        let s2 = Series::new("id", &[id]);
        let df = DataFrame::new(vec![s0, s1, s2]).unwrap();
        dfs.push(df);
    }

    dbg!(&dfs);

    // collect all dataframes from vector into one dataframe
    let empty_head = dfs   
        .get(0).expect("df has len 0")
        .clone()
        .lazy()
        .limit(0);

    // let df = dfs
    //     .into_iter()
    //     .fold(empty_head, |acc, df| concat([acc, df.lazy()], false, true).unwrap())
    //     .collect()
    //     .unwrap();

    // new version
    let df = dfs
        .into_iter()
        .fold(empty_head, |acc, df| concat([acc, df.lazy()], UnionArgs { parallel: true, rechunk: true, to_supertypes: true }).unwrap())
        .collect()
        .unwrap();

    dbg!(df);

    //let df = DataFrame::new(
    //columns.into_iter()
    //    .map(|(name, values)| Series::new(name, values))
    //    .collect::<Vec<_>>()
    //).unwrap();
}

pub fn join_dfs_example() -> Result<()> {
    // let mut rng = rand::thread_rng();

    let df1 = df!(
                    "id" => 1..6,
                    // "a"=> (1..6).map(|_| rng.gen::<i32>()).collect::<Vec<i32>>(),
                    //"a"=> (1..6).map(|_| rng.gen_range(0..100)).collect::<Vec<i32>>(),
                    "b"=> [Some(42.0), Some(42.0), None, None, Some(0.0)]
    ).expect("should not fail").lazy();
    
    let df2 = df!(
                    "id" => 1..6,
                    "c"=> &["foo", "foo", "foo", "foo", "foo"],
    ).expect("should not fail").lazy();
    
    let df3 = df!(
                    "id" => 1..6,
                    "d"=> &["bar", "bar", "bar", "bar", "bar"],
    ).expect("should not fail").lazy();

    // dbg!(df1, df2, df3);
    // let dfs = vec![df1, df2, df3];

    // let df = df1.select([col("id"), col("a")]).collect()?;
    // let df = df1.select([cols(columns)]).collect()?;

    // let columns = ["id"];
    // let df = join_dfs2(dfs, &columns)?.collect()?;
    let df = join_dfs(vec![df1, df2, df3], &["id"], None).unwrap().collect().unwrap();
    dbg!(df);

    Ok(())
}

pub fn join_dfs_simple(ldf: LazyFrame, other: LazyFrame, columns: &[&str], join_type: Option<&str>) -> LazyFrame {
    let columns = columns
        .iter()
        .map(|x| col(x))
        .collect::<Vec<Expr>>();

    if let Some(join_type) = join_type {
        let join_type = match join_type {
            "inner" => JoinArgs::new(JoinType::Inner),
            "left" => JoinArgs::new(JoinType::Left),
            "cross" => JoinArgs::new(JoinType::Cross),
            _ => JoinArgs::new(JoinType::Inner),
        };
        ldf.join(other, columns.clone(), columns.clone(), join_type)
    } else {
        ldf.join(other, columns.clone(), columns.clone(), JoinArgs::new(JoinType::Inner))
    }
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

pub fn df_to_partion_example() {
    let mut df = df!(
        "id" => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "name" => &["foo", "bar", "baz", "cj", "new", "one", "ccc", "xxx", "sss", "dd"], 
    ).unwrap();

    let chunk_size = 3;

    let chunked_df = partition_df(&mut df, chunk_size);
    println!("{:?}", chunked_df);
}