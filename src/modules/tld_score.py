# -*- coding: utf-8 -*-
"""
TLD-Based Domain Credibility Scoring
Scores each domain based on its top-level domain (TLD) according to the paper's
algorithm, and outputs the TLD credibility score St_Score for each domain.
"""

import pandas as pd
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TLDScorer:
    def __init__(self, config: Dict):
        """
        Args:
            config: Configuration dictionary.
                - strict_tlds: dict mapping strictly authenticated TLDs to their scores.
                - default_score: float, default score for non-strictly authenticated TLDs.
        """
        self.strict_tlds = config.get('strict_tlds', {})
        self.default_score = config.get('default_score', 0)
        logger.info(f"TLDScorer initialized. {len(self.strict_tlds)} strict TLD(s) configured.")

    def run(self, user_scores: pd.DataFrame, tld_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute the TLD score St_Score for each domain.

        Args:
            user_scores: DataFrame containing at least a Domain column.
            tld_df: DataFrame containing a domain column or fields from which TLD can be extracted.

        Returns:
            DataFrame with columns: Domain, St_Score.
        """
        logger.info("Starting TLD score computation...")

        df = user_scores.copy()
        # Extract the top-level domain (TLD) from each domain name
        df['TLD'] = df['Domain'].apply(self.extract_tld)

        # Assign scores
        df['St_Score'] = df['TLD'].apply(lambda x: self.strict_tlds.get(x, self.default_score))

        result_df = df[['Domain', 'St_Score']]
        logger.info("TLD score computation complete.")
        return result_df

    @staticmethod
    def extract_tld(domain: str) -> str:
        """
        Extract the top-level domain (TLD) from a domain name. Examples:
            'www.example.edu.cn' -> '.edu.cn'
            'example.com'        -> '.com'
        """
        domain = domain.lower().strip()
        parts = domain.split('.')
        if len(parts) < 2:
            return ''
        # Check for two-level TLDs (e.g. .edu.cn)
        last_two = f".{parts[-2]}.{parts[-1]}"
        if last_two in ['.gov.cn', '.edu.cn', '.mil.cn', '.ac.cn', '.org.cn']:
            return last_two
        return f".{parts[-1]}"  # Default: single-level TLD


# =================== Usage example ===================
if __name__ == "__main__":
    TLD_CONFIG = {
        'strict_tlds': {
            '.gov.cn': 1.0,
            '.edu.cn': 1.0,
            '.mil.cn': 1.0,
            '.gov': 0.8,
            '.edu': 0.8,
            '.ac.cn': 0.9,
            '.org.cn': 0.8,
        },
        'default_score': 0
    }

    scorer = TLDScorer(TLD_CONFIG)

    # Example data loading:
    # user_scores = pd.read_csv('user_scores.csv')
    # tld_df = pd.read_csv('tld_df.csv')  # optional
    # result = scorer.run(user_scores, tld_df)
    # print(result.head())
