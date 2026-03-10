# -*- coding: utf-8 -*-
"""
TLD-Based Domain Credibility Scoring
根据论文算法，对域名的顶级域名(TLD)进行评分
输出每个域名的 TLD 评分 St_Score
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
            config: 配置字典
                - strict_tlds: dict, 严格认证的TLD及其得分
                - default_score: float, 非严格认证TLD默认得分
        """
        self.strict_tlds = config.get('strict_tlds', {})
        self.default_score = config.get('default_score', 0)
        logger.info(f"TLDScorer 初始化完成, {len(self.strict_tlds)} 个严格TLD配置")

    def run(self, user_scores: pd.DataFrame, tld_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算每个域名的 TLD 得分 St_Score

        Args:
            user_scores: DataFrame，至少包含 Domain 字段
            tld_df: DataFrame，包含 domain 字段或可以提取 TLD 的字段

        Returns:
            DataFrame: Domain, St_Score
        """
        logger.info("开始计算TLD评分...")

        df = user_scores.copy()
        # 提取域名的顶级域名 TLD
        df['TLD'] = df['Domain'].apply(self.extract_tld)

        # 赋分
        df['St_Score'] = df['TLD'].apply(lambda x: self.strict_tlds.get(x, self.default_score))

        result_df = df[['Domain', 'St_Score']]
        logger.info("TLD评分计算完成，输出 DataFrame")
        return result_df

    @staticmethod
    def extract_tld(domain: str) -> str:
        """
        提取域名的顶级域名（TLD），例如：
        'www.example.edu.cn' -> '.edu.cn'
        'example.com' -> '.com'
        """
        domain = domain.lower().strip()
        parts = domain.split('.')
        if len(parts) < 2:
            return ''
        # 判断是否为两级TLD（如 .edu.cn）
        last_two = f".{parts[-2]}.{parts[-1]}"
        if last_two in ['.gov.cn', '.edu.cn', '.mil.cn', '.ac.cn', '.org.cn']:
            return last_two
        return f".{parts[-1]}"  # 默认单级TLD


# =================== 使用示例 ===================
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

    # 示例数据加载
    # user_scores = pd.read_csv('user_scores.csv')
    # tld_df = pd.read_csv('tld_df.csv')  # 可选
    # result = scorer.run(user_scores, tld_df)
    # print(result.head())
