import os
import pandas as pd
from datetime import datetime
from config.setting import MERGE_SOURCES


class RankMerger:

    @staticmethod
    def find_all_secrank_dates(path):
        """Find all date-named directories under the SecRank path, sorted in reverse chronological order."""
        files = os.listdir(path)
        dates = []
        for f in files:
            try:
                datetime.strptime(f, "%Y-%m-%d")
                dates.append(f)
            except ValueError:
                continue
        if not dates:
            raise FileNotFoundError(f"No date files found under directory {path}")
        dates.sort(reverse=True)
        return dates

    @staticmethod
    def load_full_secrank(path):
        """Load the full SecRank ranking list."""
        df = pd.read_csv(
            path,
            header=None,
            names=["Domain", "Score", "Rank"],  # three columns
            sep="\t",
            dtype={"Domain": str}
        )
        df["Domain"] = df["Domain"].str.strip()
        return df[["Domain", "Rank"]]  # keep only domain and rank

    @staticmethod
    def merge_with_full(our_df, full_df, score_col=None, ascending=False):
        """通用合并逻辑"""
        our_df["Domain"] = our_df["Domain"].astype(str)
        full_df["Domain"] = full_df["Domain"].astype(str)

        merged = full_df.merge(our_df, on="Domain", how="left")

        if score_col:
            if score_col not in merged.columns:
                merged[score_col] = 0
            merged[score_col] = merged[score_col].fillna(0)
            merged = merged.sort_values(by=[score_col, "Rank"], ascending=[ascending, True])
        else:
            merged = merged.sort_values(by="Rank", ascending=True)

        merged = merged.head(1000000).reset_index(drop=True)
        merged["FinalRank"] = range(1, len(merged) + 1)
        return merged

    @staticmethod
    def save_results(df, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"Saved successfully: {path}")

    @staticmethod
    @staticmethod
    def merge_umbrella(our_df, full_df):
        """
        Merge the Umbrella ranking:
        - Domains already in our_df retain their original order.
        - New domains are appended, sorted by their original Rank in full_secrank.
        """
        our_df["Domain"] = our_df["Domain"].astype(str)
        full_df["Domain"] = full_df["Domain"].astype(str)

        # 已有域名：在 full_df 中能找到的才保留
        existing = our_df.merge(full_df, on="Domain", how="left")

        # 新域名（没有在 our_df 中）
        new_domains = full_df[~full_df["Domain"].isin(our_df["Domain"])].copy()
        new_domains = new_domains.sort_values(by="Rank")  # 按 full_secrank 原始顺序

        # 合并已有域名和新域名
        merged = pd.concat([existing, new_domains], ignore_index=True)

        # 添加 FinalRank
        merged["FinalRank"] = range(1, len(merged) + 1)
        return merged

    @staticmethod
    def run_merge():
        # Find all SecRank date directories
        dates = RankMerger.find_all_secrank_dates(MERGE_SOURCES['secrank_data_path'])
        print(f"Found {len(dates)} date files. Processing in reverse chronological order.")

        for date in dates:
            print(f"\nProcessing date: {date}")

            secrank_file = os.path.join(MERGE_SOURCES['secrank_data_path'], date)
            toplist_file = os.path.join(MERGE_SOURCES['TopList'], f"TopList{date}.csv")
            secrank_mine_file = os.path.join(MERGE_SOURCES['SecRank'], f"SecRank{date}.csv")
            umbrella_file = os.path.join(MERGE_SOURCES['Umbrella'], f"umbrella{date}.csv")

            # Check for missing files
            missing_files = []
            for f in [secrank_file, toplist_file, secrank_mine_file, umbrella_file]:
                if not os.path.exists(f):
                    missing_files.append(f)
            if missing_files:
                print(f"Missing files, skipping date {date}: {missing_files}")
                continue

            # 加载 full_secrank
            full_secrank = RankMerger.load_full_secrank(secrank_file)

            # 加载数据
            toplist = pd.read_csv(toplist_file)
            secrank_mine = pd.read_csv(secrank_mine_file)
            umbrella = pd.read_csv(umbrella_file)

            # 合并
            merged_secrank = RankMerger.merge_with_full(secrank_mine, full_secrank, score_col="Sd_Score", ascending=False)
            merged_toplist = RankMerger.merge_with_full(toplist, full_secrank, score_col="Final_Score", ascending=False)
            merged_umbrella = RankMerger.merge_umbrella(umbrella, full_secrank)


            # 保存结果
            RankMerger.save_results(
                merged_secrank, os.path.join(MERGE_SOURCES['MergeSecRank'], f"MergeSecRank{date}.csv")
            )
            RankMerger.save_results(
                merged_toplist, os.path.join(MERGE_SOURCES['MergeTopList'], f"MergeTopList{date}.csv")
            )
            RankMerger.save_results(
                merged_umbrella, os.path.join(MERGE_SOURCES['MergeUmbrella'], f"MergeUmbrella{date}.csv")
            )


if __name__ == "__main__":
    RankMerger.run_merge()
