# -*- coding: utf-8 -*-
"""
AHP Weight Fusion Module
"""

import pandas as pd
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AHPIntegrator:
    def __init__(self, weights: Dict[str, float]):
        """
        Args:
            weights: dict of weights for the four modules, e.g.:
                     {"Sd": 0.4, "Sr": 0.2, "St": 0.2, "Sp": 0.2}
        """
        if abs(sum(weights.values()) - 1.0) > 1e-6:
            raise ValueError("AHP weights must sum to 1.")
        self.weights = weights
        logger.info(f"AHPIntegrator initialized. Weights={weights}")

    def save_results(self, df, path, fmt='csv'):
        """Save results to file."""
        if fmt == 'csv':
            df.to_csv(path, index=False, encoding='utf-8-sig')
        elif fmt == 'xlsx':
            df.to_excel(path, index=False)
        else:
            raise ValueError(f"Unsupported output format: {fmt}")

    def integrate(self, user_scores: pd.DataFrame,
                  registrar_scores: pd.DataFrame,
                  tld_scores: pd.DataFrame,
                  link_scores: pd.DataFrame) -> pd.DataFrame:
        """
        Fuse scores from the four modules to produce a final ranking.

        Args:
            user_scores: DataFrame with columns [Domain, Sd_Score].
            registrar_scores: DataFrame with columns [Domain, Sr_Score].
            tld_scores: DataFrame with columns [Domain, St_Score].
            link_scores: DataFrame with columns [Domain, Sp_Score].

        Returns:
            DataFrame with columns [Domain, Sd_Score, Sr_Score, St_Score, Sp_Score, Final_Score].
        """
        logger.info("Starting AHP weight fusion...")

        # Merge all four score DataFrames
        df = user_scores.merge(registrar_scores, on="Domain", how="outer")
        df = df.merge(tld_scores, on="Domain", how="outer")
        df = df.merge(link_scores, on="Domain", how="outer")

        # Fill missing values with 0
        df = df.fillna(0)

        # Compute weighted final score
        df['Final_Score'] = (
            df['Sd_Score'] * self.weights['Sd'] +
            df['Sr_Score'] * self.weights['Sr'] +
            df['St_Score'] * self.weights['St'] +
            df['Sp_Score'] * self.weights['Sp']
        )

        # Sort by Final_Score descending
        df = df.sort_values(by="Final_Score", ascending=False).reset_index(drop=True)

        logger.info("AHP weight fusion complete.")
        return df


# =================== Usage example ===================
if __name__ == "__main__":
    AHP_WEIGHTS = {"Sd": 0.4, "Sr": 0.2, "St": 0.2, "Sp": 0.2}

    ahp = AHPIntegrator(AHP_WEIGHTS)

    # Assuming all four module results are available:
    # user_scores = pd.DataFrame({"Domain": [...], "Sd_Score": [...]})
    # registrar_scores = pd.DataFrame({"Domain": [...], "Sr_Score": [...]})
    # tld_scores = pd.DataFrame({"Domain": [...], "St_Score": [...]})
    # link_scores = pd.DataFrame({"Domain": [...], "Sp_Score": [...]})
    #
    # final_scores = ahp.integrate(user_scores, registrar_scores, tld_scores, link_scores)
    # print(final_scores.head())
