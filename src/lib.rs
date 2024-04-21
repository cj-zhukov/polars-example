use std::any;
use std::fs::File;
use std::collections::HashMap;

use polars::prelude::*;
use anyhow::Context;
use serde_json::Value;
use rayon::prelude::*;

pub fn get_df() -> anyhow::Result<DataFrame> {
    let df = df!(
        "id" => [1, 2, 3],
        "name" => &["foo", "bar", "baz"], 
        "data" => &[42, 42, 43], 
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

pub fn dummy_join_dfs_example() -> anyhow::Result<()> {
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

pub fn create_struct_df() {
    let df = df!(
        "id" => [1, 2, 3],
        "name" => &["foo", "bar", "baz"], 
        "val" => &["val1", "val2", "val3"], 
        "data" => &[42, 42, 43], 
    ).unwrap();

    println!("{:?}", df);

    let metadata = df 
        .select(["val", "data", "name"])
        .unwrap()
        .into_struct("metadata")
        .into_series()
        .into_frame();

    println!("{:?}", metadata);
}

pub fn create_df_json() -> anyhow::Result<()> {
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

pub fn create_df_with_list_col() -> anyhow::Result<()> {
    let s1 = Series::new("Fruit", &["Apple", "Ananas", "Pear"]);

    let red = Series::new("Red", &[1, 1, 1]);
    let yellow = Series::new("Yellow", &[10, 10, 10]);
    let green = Series::new("Green", &[101, 112, 1]);
    let colors = vec![red, yellow, green];
    let list = Series::new("Color", colors);

    let df = DataFrame::new(vec![s1, list])?;
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

pub fn join_dfs(dfs: Vec<LazyFrame>, columns: &[&str], join_type: Option<&str>) -> anyhow::Result<LazyFrame> {    
    let columns = columns
        .iter()
        .map(|x| col(x))
        .collect::<Vec<Expr>>();

    let join_type = match join_type {
        Some(val) => {
            let val = match val {
                "inner" => JoinType::Inner,
                "left" => JoinType::Left,
                "cross" => JoinType::Cross,
                _ => JoinType::Inner,
            };
            val
        },
        None => JoinType::Inner,
    };

    let df = dfs
        .into_iter()
        .reduce(|acc, e| 
            acc.join(
                e, 
                columns.clone(), 
                columns.clone(), 
                JoinArgs::new(join_type.clone())))
        .expect("reduced df is none")
        .collect()
        .context("could not join dfs")?
        .lazy();

    Ok(df)
}

fn join_dfs2(dfs: Vec<LazyFrame>, columns: &[&str]) -> anyhow::Result<LazyFrame> {    
    let columns = columns
        .iter()
        .map(|x| col(x))
        .collect::<Vec<Expr>>();

    let df = dfs
        .into_iter()
        .reduce(|acc, e| 
            acc.join(
                e, 
                columns.clone(), 
                columns.clone(), 
                JoinType::Inner.into()))
        .unwrap()
        .collect()
        .context("cannot join dfs")?
        .lazy();

    Ok(df)
}

// works not ok with fold
fn join_dfs_old2(dfs: Vec<LazyFrame>, columns: &[&str]) -> anyhow::Result<LazyFrame> {
    let head = dfs   
        .get(0).context("df has len 0")?
        .clone()
        .select([cols(columns)]);
    
    let columns = columns
        .iter()
        .map(|x| col(x))
        .collect::<Vec<Expr>>();

    let df = dfs
        .into_iter()
        .fold(head, |acc, df| 
            acc.join(
                df, 
                columns.clone(), 
                columns.clone(), 
                JoinType::Inner.into()))
        .collect()
        .context("could not join dfs")?
        .lazy();

    Ok(df)
}

pub fn concat_dfs(dfs: Vec<LazyFrame>) -> anyhow::Result<LazyFrame> {
    let empty_head = dfs   
        .get(0).context("df has len 0")?
        .clone()
        .limit(0);

    let res = dfs
        .into_iter()
        .fold(empty_head, |acc, df| 
            concat([acc, df], 
            UnionArgs { parallel: true, rechunk: true, to_supertypes: true })
        .expect("could not concat dfs"));

    Ok(res)
}

// fn concat_dfs_old(dfs: Vec<LazyFrame>) -> anyhow::Result<LazyFrame> {
//     let empty_head = dfs   
//         .get(0).context("df has len 0")?
//         .clone()
//         .limit(0);

//     let res = dfs
//         .into_iter()
//         .fold(empty_head, |acc, df| concat([acc, df], false, true).expect("cannot fold when concating dfs"))
//         .with_streaming(true);

//     Ok(res)
// }

pub fn df_to_partion_example() {
    let mut df = df!(
        "id" => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "name" => &["foo", "bar", "baz", "cj", "new", "one", "ccc", "xxx", "sss", "dd"], 
    ).unwrap();

    let chunk_size = 3;

    let chunked_df = partition_df(&mut df, chunk_size);
    println!("{:?}", chunked_df);
}

pub fn partition_df(df: &mut DataFrame, chunk_size: usize) -> Vec<DataFrame> {
    let mut offset = 0i64;
    let mut chunked_df = Vec::new();
    loop {
        if offset > df.shape().0 as i64 {
            break;
        }
        let chunk = df.slice_par(offset, chunk_size);
        chunked_df.push(chunk);
        offset += chunk_size as i64;
    }
    
    chunked_df
}

pub fn read_csv(file_path: &str) -> anyhow::Result<DataFrame> {
    let df = CsvReader::from_path(file_path)?.finish()?;
    
    Ok(df)
}

pub fn read_parquet_files(files: &[&str]) -> anyhow::Result<Vec<DataFrame>> {
    let mut dfs = Vec::new();
    for file in files {
        let mut file = File::open(file).unwrap();
        let df = ParquetReader::new(&mut file).finish().unwrap();
        dfs.push(df);
    }

    Ok(dfs)
}

pub fn read_parquet_files_lazy(file_path: &str) -> anyhow::Result<DataFrame> {
    let df = LazyFrame::scan_parquet(file_path, ScanArgsParquet::default())?
        .select([
            // select all columns
            all(),
            // and do some aggregations
            // cols(["fats_g", "sugars_g"]).sum().suffix("_summed"),
        ])
        .collect()?;

    Ok(df)
}

pub fn write_to_parquet(mut df: &mut DataFrame, file_path: &str) -> anyhow::Result<()> {
    let mut file = File::create(file_path).context("could not create file")?;
    ParquetWriter::new(&mut file).finish(&mut df).context("could not write to file")?;

    Ok(())
}

pub fn df_cols_to_json(df: &mut DataFrame, cols: &[&str], new_col: Option<&str>) -> anyhow::Result<DataFrame> {
    let metadata = df
        .select(cols)?
        .into_struct("metadata")
        .into_series()
        .into_frame();

    let metadata_json = serde_json::to_value(&metadata)?;
    let mut vals = vec![];
    for idx in 0..cols.len() {
        let val = &metadata_json["columns"][0]["values"][idx]["values"];
        vals.push(val);
    }
    // println!("metadata: {:?}", vals);

    let mut mv = vec![];
    let mut idx = 0usize;
    let mut col_name = cols.iter();
    for val in vals {
        let mut mvv = vec![];
        let col_name = col_name.next().unwrap();
        while idx < metadata.shape().0 {
            let val = &val[idx]; 
            let m = HashMap::from([
                (col_name, val),
            ]);
            mvv.push(m);
            idx += 1;
        }   
        idx = 0;
        mv.push(mvv);
    }
    println!("mv: {:?}", mv);

    let mut j = 0usize;
    let mut res = vec![];
    for _ in 0..mv[0].len() {
        let mut x: HashMap<&&str, &Value> = HashMap::new();
        for i in 0..mv.len() {
            let val = &mv[i][j];
            x.extend(val);
        }
        res.push(x);
        j += 1;
    } 
    println!("res: {:?}", res);

    let mv = res
        .iter()
        .map(|x| serde_json::to_string(x).context(format!("could not ser to string val={:?} from={:?}", x, mv)))
        .collect::<Result<Vec<_>, _>>().unwrap();

    let s = Series::new(new_col.unwrap_or("metadata"), mv);

    let res = df
        .with_column(s)?
        .clone()
        .lazy()
        .select(&[col("*").exclude(cols)]);

    Ok(res.collect()?)
}

pub fn df_cols_to_json2(df: &mut DataFrame, metadata_cols: &[&str], new_col: Option<&str>) -> anyhow::Result<()> {
    let metadata = df
        .select(metadata_cols)?
        .into_struct("metadata")
        .into_series()
        .into_frame();

    let metadata_json = serde_json::to_value(&metadata)?;
    let mut vals = vec![];
    for idx in 0..metadata_cols.len() {
        let val = &metadata_json["columns"][0]["values"][idx]["values"];
        vals.push(val);
    }

    let mut mv = vec![];
    let mut idx = 0usize;
    let mut col_name = metadata_cols.iter();
    for val in vals {
        let mut mvv = vec![];
        let col_name = col_name.next().unwrap();
        while idx < metadata.shape().0 {
            let val = &val[idx]; 
            let m = HashMap::from([
                (col_name, val),
            ]);
            mvv.push(m);
            idx += 1;
        }   
        idx = 0;
        mv.push(mvv);
    }

    let mut j = 0usize;
    let mut res = vec![];
    for _ in 0..mv[0].len() {
        let mut x: HashMap<&&str, &Value> = HashMap::new();
        for i in 0..mv.len() {
            let val = &mv[i][j];
            x.extend(val);
        }
        res.push(x);
        j += 1;
    } 

    // with rayon for performance
    let mv = res
        .par_iter()
        .map(|x| serde_json::to_string(x).context(format!("could not ser to string val={:?} from={:?}", x, mv)))
        .collect::<Result<Vec<_>, _>>().unwrap();

    let s = Series::new(new_col.unwrap_or("metadata"), mv);

    df.with_column(s)?;

    Ok(())
}

pub fn df_cols_to_json3(mut df: DataFrame, metadata_cols: &[&str], new_col: Option<&str>) -> DataFrame {
    let metadata = df.select(metadata_cols).unwrap().into_struct("metadata").into_series().into_frame();
    let metadata_json = serde_json::to_value(&metadata).unwrap();
    let mut vals = vec![];
    for idx in 0..metadata_cols.len() {
        let val = &metadata_json["columns"][0]["values"][idx]["values"];
        vals.push(val);
    }

    let mut mv = vec![];
    let mut idx = 0usize;
    let mut col_name = metadata_cols.iter();
    for val in vals {
        let mut mvv = vec![];
        let col_name = col_name.next().unwrap();
        while idx < metadata.shape().0 {
            let val = &val[idx]; 
            let m = HashMap::from([(col_name, val)]);
            mvv.push(m);
            idx += 1;
        }   
        idx = 0;
        mv.push(mvv);
    }

    let mut j = 0usize;
    let mut res = vec![];
    for _ in 0..mv[0].len() {
        let mut x: HashMap<&&str, &Value> = HashMap::new();
        for i in 0..mv.len() {
            let val = &mv[i][j];
            x.extend(val);
        }
        res.push(x);
        j += 1;
    } 

    let mv = res.iter() .map(|x| serde_json::to_string(x).unwrap()).collect::<Vec<_>>();
    let s = Series::new(new_col.unwrap_or("metadata"), mv);
    df.with_column(s).unwrap();
    df.lazy().select(&[col("*").exclude(metadata_cols)]).collect().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[test]
    fn test_join_dfs() {
        let mut rng = rand::thread_rng();
        
        let df1 = df!(
            "id" => 1..6,
            "a"=> &["foo", "foo", "foo", "foo", "foo"],
            "b"=> &[Some(42.0), Some(42.0), None, None, Some(0.0)],
            "c" => (1..6).map(|_| rng.gen_range(0..100)).collect::<Vec<i32>>(),
        ).expect("should not fail").lazy();

        let df2 = df!(
            "id" => 1..6,
            "d"=> &["bar", "bar", "bar", "bar", "bar"],
        ).expect("should not fail").lazy();

        let df3 = df!(
            "id" => 1..6,
            "e"=> &["baz", "baz", "baz", "baz", "baz"],
        ).expect("should not fail").lazy();

        let res = join_dfs(vec![df1, df2, df3], &["id"], None).unwrap().collect().unwrap();
        assert_eq!(res.shape(), (5, 6));

        let sv: Vec<&Series> = res.columns(&["id", "a", "b", "c", "d", "e"]).unwrap();
        assert_eq!(&res[0], sv[0]);
        assert_eq!(&res[1], sv[1]);
        assert_eq!(&res[2], sv[2]);
        assert_eq!(&res[3], sv[3]);
        assert_eq!(&res[4], sv[4]);
        assert_eq!(&res[5], sv[5]);
    }

    #[test]
    fn test_concat_dfs() {
        let df1 = df!(
            "id" => [1, 2, 3],
            "val" => &["foo", "foo", "foo"], 
        ).unwrap();
    
        let df2 = df!(
            "id" => [4, 5],
            "val" => &["bar", "bar"], 
        ).unwrap();
    
        let df3 = df!(
            "id" => [6],
            "val" => &["baz"], 
        ).unwrap();
    
        let res = concat_dfs(vec![df1.lazy(), df2.lazy(), df3.lazy()]).unwrap().collect().unwrap();
        assert_eq!(res.shape(), (6, 2));

        let sv: Vec<&Series> = res.columns(&["id", "val"]).unwrap();
        assert_eq!(&res[0], sv[0]);
        assert_eq!(&res[1], sv[1]);
    }

    #[test]
    fn test_partition_df() {
        let mut df = df!(
            "id" => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "val" => &["foo", "foo", "foo", "bar", "bar", "bar", "baz", "baz", "baz", "baz"], 
        ).unwrap();
    
        let res = partition_df(&mut df, 3);
        assert_eq!(res.len(), 4);
        assert_eq!(res[0].shape(), (3, 2));
        assert_eq!(res[1].shape(), (3, 2));
        assert_eq!(res[2].shape(), (3, 2));
        assert_eq!(res[3].shape(), (1, 2));
    }

    #[test]
    fn test_cols_to_json() {
        let mut df = df!(
            "id" => &[1, 2, 3],
            "name" => &["foo", "bar", "baz"], 
            "data" => &[43, 43, 44], 
        ).unwrap();
    
        let res = df_cols_to_json(&mut df, &["name", "data"], Some("metadata")).unwrap();
 
        // let metadata = df.select(&["metadata"]).unwrap();
        assert_eq!(res.shape(), (3, 2)); 
    }
}