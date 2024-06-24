pub mod examples;

use std::fs::File;
use std::collections::HashMap;

use anyhow::{Context, Result};
use polars::prelude::*;
use serde_json::Value;
use rayon::prelude::*;

pub fn get_df() -> Result<DataFrame> {
    let df = df!(
        "id" => [1, 2, 3],
        "name" => &["foo", "bar", "baz"], 
        "data" => &[42, 43, 44], 
    )?;

    Ok(df)
}

pub fn df_cols_to_struct() {
    let mut df = df!(
        "id" => [1, 2, 3],
        "name" => &["foo", "bar", "baz"], 
        "data" => &[42, 43, 44], 
    ).unwrap();

    println!("{:?}", df);

    let metadata = df 
        .select(["data", "name"])
        .unwrap()
        .into_struct("metadata")
        .into_series();

    let res = df.with_column(metadata).unwrap().select(["id", "metadata"]).unwrap();

    println!("{:?}", res);
}

pub fn join_dfs(dfs: Vec<LazyFrame>, columns: &[&str], join_type: Option<&str>) -> Result<LazyFrame> {    
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

pub fn join_dfs2(dfs: Vec<LazyFrame>, columns: &[&str]) -> Result<LazyFrame> {    
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

pub fn concat_dfs(dfs: Vec<LazyFrame>) -> Result<LazyFrame> {
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

pub fn read_csv(file_path: &str) -> Result<DataFrame> {
    let df = CsvReader::from_path(file_path)?.finish()?;
    
    Ok(df)
}

pub fn read_parquet_files(files: &[&str]) -> Result<Vec<DataFrame>> {
    let mut dfs = Vec::new();
    for file in files {
        let mut file = File::open(file).unwrap();
        let df = ParquetReader::new(&mut file).finish().unwrap();
        dfs.push(df);
    }

    Ok(dfs)
}

pub fn read_parquet_files_lazy(file_path: &str) -> Result<DataFrame> {
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

pub fn write_to_parquet(mut df: &mut DataFrame, file_path: &str) -> Result<()> {
    let mut file = File::create(file_path).context("could not create file")?;
    ParquetWriter::new(&mut file).finish(&mut df).context("could not write to file")?;

    Ok(())
}

pub fn df_cols_to_json(df: &mut DataFrame, cols: &[&str], new_col: Option<&str>) -> Result<DataFrame> {
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

pub fn df_cols_to_json2(df: &mut DataFrame, metadata_cols: &[&str], new_col: Option<&str>) -> Result<()> {
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