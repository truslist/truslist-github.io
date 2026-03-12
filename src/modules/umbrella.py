# -*- coding: utf-8 -*-
"""
Umbrella Ranking Module
Ranks domains (SLDs) by the number of unique IPs that access them.
"""

import pandas as pd
import logging
import tldextract

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UmbrellaRanker:
    def __init__(self, config=None):
        """
        Args:
            config: dict (optional)
                - min_ip_threshold: int, minimum unique IP count to keep a domain (default: 1).
        """
        self.config = config or {}
        self.min_ip_threshold = self.config.get("min_ip_threshold", 1)
        logger.info(f"UmbrellaRanker initialized. min_ip_threshold={self.min_ip_threshold}")

    def run(self, userbehavior_df: pd.DataFrame) -> pd.DataFrame:
        """
        Rank domains (SLDs) by the number of unique IPs that access them.

        Args:
            userbehavior_df: DataFrame, must contain columns ['src_ip', 'dns.rrname'].

        Returns:
            DataFrame with columns ['Domain', 'Unique_IPs', 'UmbrellaRank'].
        """
        logger.info("Starting Umbrella ranking computation (by SLD)...")

        # Verify required columns exist
        required_cols = {"src_ip", "dns.rrname"}
        if not required_cols.issubset(userbehavior_df.columns):
            raise ValueError(f"Input data is missing required columns: {required_cols}")

        # Extract SLD (second-level domain)
        def extract_sld(domain):
            try:
                if not isinstance(domain, str):  # Skip non-string or NaN values
                    return None
                ext = tldextract.extract(domain)
                if ext.domain and ext.suffix:
                    return f"{ext.domain}.{ext.suffix}".lower()
                return domain.lower()
            except Exception:
                return None

        userbehavior_df = userbehavior_df.copy()
        userbehavior_df["Domain"] = userbehavior_df["dns.rrname"].apply(extract_sld)
        # Drop rows with invalid domains (None or empty string)
        userbehavior_df = userbehavior_df.dropna(subset=["Domain"])
        # Count unique IPs per SLD
        domain_ip_counts = (
            userbehavior_df.groupby("Domain")["src_ip"]
            .nunique()
            .reset_index(name="Unique_IPs")
        )

        # Filter out domains with too few unique IPs
        domain_ip_counts = domain_ip_counts[domain_ip_counts["Unique_IPs"] >= self.min_ip_threshold]

        # Sort and assign ranks
        domain_ip_counts = domain_ip_counts.sort_values(
            by="Unique_IPs", ascending=False
        ).reset_index(drop=True)
        domain_ip_counts["UmbrellaRank"] = domain_ip_counts.index + 1

        logger.info(f"Umbrella ranking complete. Total SLDs: {len(domain_ip_counts)}")

        return domain_ip_counts[["Domain", "Unique_IPs", "UmbrellaRank"]]

    def save_results(self, df, path, fmt='csv'):
        """Save results to file."""
        if fmt == 'csv':
            df.to_csv(path, index=False, encoding='utf-8-sig')
        elif fmt == 'xlsx':
            df.to_excel(path, index=False)
        else:
            raise ValueError(f"Unsupported output format: {fmt}")

# =================== 使用示例 ===================
if __name__ == "__main__":
    # 模拟数据
    data = {
        "timestamp": [
            "2025-08-07T00:00:01.238438+0800",
            "2025-08-07T00:00:01.238458+0800",
            "2025-08-07T00:00:01.238467+0800",
            "2025-08-07T00:00:01.238504+0800",
            "2025-08-07T00:00:02.643158+0800",
        ],
        "src_ip": [
            "202.127.23.185",
            "202.127.23.185",
            "202.127.23.185",
            "202.127.23.185",
            "121.195.186.1",
        ],
        "dns.rrname": [
            "settings-win.data.microsoft.com",
            "settings-win.data.microsoft.com",
            "settings-win.data.microsoft.com",
            "settings-win.data.microsoft.com",
            "n.dnso.fun",
        ],
        "dest_ip": [
            "159.226.8.6",
            "159.226.8.6",
            "159.226.8.6",
            "159.226.8.6",
            "159.226.8.6",
        ],
    }

    df = pd.DataFrame(data)
    ranker = UmbrellaRanker()
    result = ranker.run(df)
    print(result)
