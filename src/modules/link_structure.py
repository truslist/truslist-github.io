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
                - reference_sources: list, 参考来源 ['tranco', 'secrank']
                - max_reference_domains: int, 参考域最大数量
                - ranking_data: dict, 包含 ranking 格式配置
        """
        self.config = config
        self.max_ref = config.get('max_reference_domains', 1000)
        logger.info(f"LinkStructureScorer 初始化完成, 参考域上限={self.max_ref}")

    def run(self, user_scores: pd.DataFrame, link_df: pd.DataFrame,
            tranco_df: pd.DataFrame, secrank_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算域名的链接结构得分 Sp_Score
        参考域 = Tranco Top-10000 ∪ SecRank Top-10000 ∪ PDNS Top-10000（三源并集，论文约25000个）
        """
        logger.info("开始计算 Link Structure 评分...")

        top_n = self.config['ranking_data'].get('top_n_domains', 10000)

        # 1. 三源取 Top-N 并集作为参考种子（论文 §Structural Inter-domain Dependencies）
        tranco_domains = set(tranco_df.head(top_n)['domain'].str.lower().tolist())

        # SecRank: 已按 Rank 升序，取前 top_n
        secrank_domains = set(secrank_df.head(top_n)['Domain'].str.lower().tolist())

        # PDNS 自身排名（user_scores 已按 Sd_Score 降序）
        pdns_domains = set(user_scores.head(top_n)['Domain'].str.lower().tolist())

        ref_domains_set = tranco_domains | secrank_domains | pdns_domains
        logger.info(f"三源并集参考域数量={len(ref_domains_set)} "
                    f"(Tranco={len(tranco_domains)}, SecRank={len(secrank_domains)}, PDNS={len(pdns_domains)})")

        # 2. 给参考域赋初始值 V_d，论文公式: V_d = (Rank_max - Rank(d)) / (Rank_max - 1) ∈ [0,1]
        #    以 Tranco 排名为主序；SecRank/PDNS 专有域名按其各自排名赋值，取最大值
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
        # V_d = (Rank_max - Rank(d)) / (Rank_max - 1)，最高排名(Rank=1)得 V_d≈1，末位得 0
        ref_dict = {
            domain: (rank_max - rank) / (rank_max - 1)
            for domain, rank in ref_rank.items()
            if rank_max > 1
        }
        logger.info(f"参考域 V_d 赋值完成，共 {len(ref_dict)} 个域")

        # 3. V_d 值传播：对每条 (source→target) 边，累加 source 的 V_d 到 target 的 LinkSum
        link_df = link_df.copy()
        link_df['source_sld'] = link_df['source_sld'].str.lower()
        link_df['target_sld'] = link_df['target_sld'].str.lower()

        valid_links = link_df[link_df['source_sld'].isin(ref_dict.keys())]
        logger.info(f"有效边数量（source 在参考域内）={len(valid_links)}")

        if valid_links.empty:
            logger.warning("没有匹配的有效边，Sp_Score 最终可能全为 0")

        valid_links = valid_links.copy()
        valid_links['Contribution'] = valid_links['source_sld'].map(ref_dict)
        target_scores = valid_links.groupby('target_sld')['Contribution'].sum().reset_index()
        target_scores.rename(columns={'target_sld': 'Domain', 'Contribution': 'Sp_Score'}, inplace=True)

        # 4. 对齐 user_scores
        result_df = user_scores[['Domain']].copy()
        result_df = result_df.merge(target_scores, on='Domain', how='left')
        result_df['Sp_Score'] = result_df['Sp_Score'].fillna(0)

        # Min-Max 归一化到 [0,1]（LinkSum 绝对值依赖参考域数量，归一化保证与其他模块量纲一致）
        sp_min, sp_max = result_df['Sp_Score'].min(), result_df['Sp_Score'].max()
        if sp_max > sp_min:
            result_df['Sp_Score'] = (result_df['Sp_Score'] - sp_min) / (sp_max - sp_min)
            logger.info("Sp_Score 已归一化到 [0,1]")
        else:
            result_df['Sp_Score'] = 0.0
            logger.warning("Sp_Score 全为常数，归一化后全为 0")

        print("归一化后 Sp_Score 统计信息:")
        print(result_df['Sp_Score'].describe())

        logger.info("Link Structure 评分计算完成")
        return result_df

# =================== 使用示例 ===================
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

    # 假设数据已加载成 DataFrame
    # user_scores = pd.read_csv("user_scores.csv")
    # link_df = pd.read_csv("link_df.csv")
    # tranco_df = pd.read_csv("tranco.csv", names=['rank','domain'])
    # secrank_df = pd.read_csv("secrank.txt", sep="\t", names=['domain','value','rank'])
    # result = scorer.run(user_scores, link_df, tranco_df, secrank_df)
    # print(result.head())
