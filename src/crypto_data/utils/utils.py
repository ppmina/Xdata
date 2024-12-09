# -*- coding: utf-8 -*-

from typing import List
import os
import pandas as pd
import numpy as np
from joblib import Parallel, delayed


def read_term_for_date(
    date: int, paths: List[str] = None, local: bool = True
) -> pd.DataFrame:
    if local:
        path_df = None
        for path in paths:
            full_path = os.path.join(path, f"{date}.feather")
            df = pd.read_feather(full_path)
            if path_df is None:
                path_df = df
            else:
                path_df = pd.merge(path_df, df, on=["D", "T", "symbol"], how="outer")
        return path_df
    else:
        raise NotImplementedError("read term for date from db Not implemented")


class UtilsMixin:
    """Utils"""

    def gen_term_name(
        self, name: str = None, freq: str = "m30", horizon: int = 8, y: str = None
    ) -> str:
        if y is None:
            return f"{name}_{freq}_{horizon}"
        return f"{name}_{freq}_{horizon}_{y}"

    def read_term(
        self,
        paths: List[str] = None,
        start_date: int = None,
        end_date: int = None,
        local: bool = True,
        n_jobs=-1,
    ) -> pd.DataFrame:
        if local:
            if n_jobs == 1:
                dfs = [
                    read_term_for_date(date, paths, local)
                    for date in self.get_trading_dates(start_date, end_date)
                ]
            else:
                dfs = Parallel(n_jobs=n_jobs)(
                    delayed(read_term_for_date)(date, paths, local)
                    for date in self.get_trading_dates(start_date, end_date)
                )
                # Parallel(n_jobs=-1)(delayed(dfs.append)(self.read_term_for_date(date, paths, local)) for date in self.get_trading_dates(start_date, end_date))
            combined_df = pd.concat(dfs, ignore_index=True)
            return combined_df
        else:
            dfs = []
            for path in paths:
                sqls = []
                for date in self.get_trading_dates(start_date, end_date):
                    sql = (
                        f"select toInt32(formatDateTime(event_time, '%Y%m%d', 'Asia/Shanghai')) as D, formatDateTime(event_time, '%H:%i:%S.%f', 'Asia/Shanghai') AS T, symbol, val as {path.split('_')[0]} "
                        f"from MD.{'_'.join(path.split('_')[1:])} "
                        f"where toDate(event_time) = '{date}' "
                        f"and name = '{path.split('_')[0]}' "
                    )
                    sqls.append(sql)

                combined_sql = " UNION ALL ".join(sqls)
                combined_df = self.query(combined_sql)
                dfs.append(combined_df)

            final_df = pd.concat(dfs, ignore_index=True)
            return final_df

    def write_term(
        self, df: pd.DataFrame, path: str = "~/res/term/m30", overwrite: bool = False
    ):
        full_path = os.path.expanduser(path)
        os.makedirs(full_path, exist_ok=True)

        df["D"] = np.int64(df["D"])
        dates = df["D"].unique()
        alpha_columns = [s for s in df.columns if s not in ["DT", "D", "T", "symbol"]]

        for alpha_column in alpha_columns:
            alpha_dir = os.path.join(full_path, alpha_column)
            os.makedirs(alpha_dir, exist_ok=True)

            for date in dates:
                alpha_path = os.path.join(alpha_dir, f"{date}.feather")

                if not overwrite and os.path.exists(alpha_path):
                    print(f"File {alpha_path} already exists. Skipping.")
                    continue

                df_to_save = df.loc[
                    df["D"] == date, ["D", "T", "symbol", alpha_column]
                ].dropna()
                df_to_save.to_feather(alpha_path)
