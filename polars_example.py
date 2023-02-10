import polars as pl

# 参考：


def polars_ten_line():
    # データ読み込み
    df = pl.read_csv("https://j.mp/iriscsv")
    # pandasと比較して見通しよく書ける
    df_agg = (
        df
        .select([pl.col("^sepal_.*$"), pl.col("species")])           # 列の選択
        .with_columns((pl.col("sepal_width") * 2).alias("new_col"))  # 列の追加
        .filter(pl.col("sepal_length") > 5)                          # 行の選択
        .groupby("species")                                        # グループ化
        .agg(pl.all().mean())                          # 全列に対して平均を集計
    )
    print(df_agg)


def agg_example():
    # データ読み込み
    df = pl.read_csv("https://j.mp/iriscsv")
    df_agg = df.groupby('species').agg([  # speciesでgroupby
        pl.col('sepal_length').min().alias('sepal_length_min'),  # sepal_length の最小値取り出してリネーム
        pl.col('sepal_length').max().alias('sepal_length_max'),  # sepal_length の最大値取り出してリネーム
        (pl.col('sepal_length') - pl.col('sepal_width')).mean().alias('sepal_length_width_diff_mean'),
        # sepal_lengthとsepal_widthの差の平均を計算しりネーム
    ])
    print(df_agg)


def apply_example():
    # データ読み込み
    df = pl.read_csv("https://j.mp/iriscsv")
    # 超はや条件分岐のapply処理
    # pandasと比較して高速に処理できる
    df_app = df.with_column(
        pl.when((pl.col('sepal_length') + pl.col('sepal_width') % 2 == 0) | (pl.col('sepal_length') > 5.6))
        .then('い')
        .when((pl.col('sepal_width') * 2) > 6)
        .then('ろ')
        .otherwise('は')
        .alias('c')
    )
    print(df_app)


def lazy_example():
    # データ読み込み
    df = pl.read_csv("https://j.mp/iriscsv")
    # 遅延評価：Polars内部でクエリの最適化や並列実行を行い、高速に指示した一連の処理を実行してくれる機能
    df_lazy = df.lazy().with_columns(  # ←lazy以降の処理を遅延評価
        pl.when((pl.col('sepal_length') + pl.col('sepal_width') % 2 == 0) | (pl.col('sepal_length') > 5.6))
        .then('い')
        .when((pl.col('sepal_width') * 2) > 6)
        .then('ろ')
        .otherwise('は')
        .alias('c')
    ).collect()  # ←まとめて実行
    print(df_lazy)


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
    # agg_example()
    # apply_example()
    lazy_example()
    # user_guide_example()
