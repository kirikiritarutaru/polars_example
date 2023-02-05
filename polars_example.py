import polars as pl


def polars_ten_line():
    # データ読み込み
    df = pl.read_csv("https://j.mp/iriscsv")
    df_agg = (
        df
        .select([pl.col("^sepal_.*$"), pl.col("species")])           # 列の選択
        .with_columns((pl.col("sepal_width") * 2).alias("new_col"))  # 列の追加
        .filter(pl.col("sepal_length") > 5)                          # 行の選択
        .groupby("species")                                        # グループ化
        .agg(pl.all().mean())                          # 全列に対して平均を集計
    )
    print(df_agg)


def user_guide_example():
    df = pl.DataFrame(
        {
            "A": [1, 2, 3, 4, 5],
            "fruits": ["banana", "banana", "apple", "apple", "banana"],
            "B": [5, 4, 3, 2, 1],
            "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
        }
    )
    print(df)

    df.sort("fruits").select(
        [
            "fruits",
            "cars",
            pl.lit("fruits").alias("literal_string_fruits"),
            pl.col("B").filter(pl.col("cars") == "beetle").sum(),
            pl.col("A").filter(pl.col("B") > 2).sum().over("cars").alias("sum_A_by_cars"),
            pl.col("A").sum().over("fruits").alias("sum_A_by_fruits"),
            pl.col("A").reverse().over("fruits").alias("rev_A_by_fruits"),
            pl.col("A").sort_by("B").over("fruits").alias("sort_A_by_B_by_fruits"),
        ]
    )
    print(df)


if __name__ == '__main__':
    # polars_ten_line()
    user_guide_example()
