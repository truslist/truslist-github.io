# -*- coding: utf-8 -*-
"""
Link Structure Based Domain Credibility Scoring
Inspired by PageRank but simplified
"""

import pandas as pd
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LinkStructureScorer:
    def __init__(self, config: Dict):
        """
        Args:
            config: dict
                - reference_sources: list, reference sources ['tranco', 'secrank']
                - max_reference_domains: int, maximum number of reference domains
                - ranking_data: dict, ranking format configuration
        """
        self.config = config
        self.max_ref = config.get('max_reference_domains', 1000)
        logger.info(f"LinkStructureScorer initialized. Reference domain limit={self.max_ref}")

    def run(self, user_scores: pd.DataFrame, link_df: pd.DataFrame,
            tranco_df: pd.DataFrame, secrank_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute the link structure score Sp_Score for each domain.
        Reference set = Tranco Top-10000 ∪ SecRank Top-10000 ∪ PDNS Top-10000
        (union of three sources, ~25,000 domains as in the paper).
        """
        logger.info("Starting link structure score computation...")

        top_n = self.config['ranking_data'].get('top_n_domains', 10000)

        # 1. Take Top-N union of three sources as reference seeds (paper §Structural Inter-domain Dependencies)
        tranco_domains = set(tranco_df.head(top_n)['domain'].str.lower().tolist())

        # SecRank: already sorted by Rank ascending; take first top_n
        secrank_domains = set(secrank_df.head(top_n)['Domain'].str.lower().tolist())

        # PDNS self-ranking (user_scores already sorted by Sd_Score descending)
        pdns_domains = set(user_scores.head(top_n)['Domain'].str.lower().tolist())

        ref_domains_set = tranco_domains | secrank_domains | pdns_domains
        logger.info(f"Three-source union reference domain count={len(ref_domains_set)} "
                    f"(Tranco={len(tranco_domains)}, SecRank={len(secrank_domains)}, PDNS={len(pdns_domains)})")

        # 2. Assign initial value V_d to each reference domain.
        #    Paper formula: V_d = (Rank_max - Rank(d)) / (Rank_max - 1) ∈ [0,1]
        #    Tranco rank is used as primary order; SecRank/PDNS-only domains use their own ranks (take max).
        ref_rank = {}
        # Tranco
        for rank_0, domain in enumerate(tranco_df.head(top_n)['domain'].str.lower()):
            ref_rank[domain] = min(ref_rank.get(domain, rank_0 + 1), rank_0 + 1)
        # SecRank
        for rank_0, domain in enumerate(secrank_df.head(top_n)['Domain'].str.lower()):
            ref_rank[domain] = min(ref_rank.get(domain, rank_0 + 1), rank_0 + 1)
        # PDNS
        for rank_0, domain in enumerate(user_scores.head(top_n)['Domain'].str.lower()):
            ref_rank[domain] = min(ref_rank.get(domain, rank_0 + 1), rank_0 + 1)

        rank_max = top_n  # Rank_max
        # V_d = (Rank_max - Rank(d)) / (Rank_max - 1): Rank=1 → V_d≈1; last rank → 0
        ref_dict = {
            domain: (rank_max - rank) / (rank_max - 1)
            for domain, rank in ref_rank.items()
            if rank_max > 1
        }
        logger.info(f"Reference domain V_d assignment complete. Total: {len(ref_dict)} domains.")

        # 3. V_d propagation: for each (source→target) edge, accumulate source V_d into target LinkSum
        link_df = link_df.copy()
        link_df['source_sld'] = link_df['source_sld'].str.lower()
        link_df['target_sld'] = link_df['target_sld'].str.lower()

        valid_links = link_df[link_df['source_sld'].isin(ref_dict.keys())]
        logger.info(f"Valid edge count (source in reference domain set)={len(valid_links)}")

        if valid_links.empty:
            logger.warning("No matching valid edges found. Sp_Score may be all zeros.")

        valid_links = valid_links.copy()
        valid_links['Contribution'] = valid_links['source_sld'].map(ref_dict)
        target_scores = valid_links.groupby('target_sld')['Contribution'].sum().reset_index()
        target_scores.rename(columns={'target_sld': 'Domain', 'Contribution': 'Sp_Score'}, inplace=True)

        # 4. Align with user_scores
        result_df = user_scores[['Domain']].copy()
        result_df = result_df.merge(target_scores, on='Domain', how='left')
        result_df['Sp_Score'] = result_df['Sp_Score'].fillna(0)

        # Min-Max normalization to [0, 1]
        # (Raw LinkSum depends on reference domain count; normalization ensures consistent scale with other modules.)
        sp_min, sp_max = result_df['Sp_Score'].min(), result_df['Sp_Score'].max()
        if sp_max > sp_min:
            result_df['Sp_Score'] = (result_df['Sp_Score'] - sp_min) / (sp_max - sp_min)
            logger.info("Sp_Score normalized to [0, 1].")
        else:
            result_df['Sp_Score'] = 0.0
            logger.warning("Sp_Score is constant; normalized to all zeros.")

        print("Sp_Score statistics after normalization:")
        print(result_df['Sp_Score'].describe())

        logger.info("Link structure score computation complete.")
        return result_df

# =================== Usage example ===================
if __name__ == "__main__":
    LINK_STRUCTURE_CONFIG = {
        'reference_sources': ['tranco', 'secrank'],
        'max_reference_domains': 1000,
        'ranking_data': {
            'secrank_format': 'space_separated',
            'tranco_format': 'csv_ranked',
            'top_n_domains': 1000
        }
    }

    scorer = LinkStructureScorer(LINK_STRUCTURE_CONFIG)

    # Assuming data is already loaded as DataFrames:
    # user_scores = pd.read_csv("user_scores.csv")
    # link_df = pd.read_csv("link_df.csv")
    # tranco_df = pd.read_csv("tranco.csv", names=['rank','domain'])
    # secrank_df = pd.read_csv("secrank.txt", sep="\t", names=['domain','value','rank'])
    # result = scorer.run(user_scores, link_df, tranco_df, secrank_df)
    # print(result.head())
