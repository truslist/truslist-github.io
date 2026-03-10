# -*- coding: utf-8 -*-
"""
AHP 权重融合模块
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
            weights: dict, 四个模块的权重, 例如:
                     {"Sd": 0.4, "Sr": 0.2, "St": 0.2, "Sp": 0.2}
        """
        if abs(sum(weights.values()) - 1.0) > 1e-6:
            raise ValueError("AHP 权重之和必须为 1")
        self.weights = weights
        logger.info(f"AHPIntegrator 初始化完成, 权重={weights}")

    def save_results(self, df, path, fmt='csv'):
        """保存结果"""
        if fmt == 'csv':
            df.to_csv(path, index=False, encoding='utf-8-sig')
        elif fmt == 'xlsx':
            df.to_excel(path, index=False)
        else:
            raise ValueError(f"不支持的输出格式: {fmt}")

    def integrate(self, user_scores: pd.DataFrame,
                  registrar_scores: pd.DataFrame,
                  tld_scores: pd.DataFrame,
                  link_scores: pd.DataFrame) -> pd.DataFrame:
        """
        融合四个模块的得分, 得到最终排名

        Args:
            user_scores: DataFrame, [Domain, Sd_Score]
            registrar_scores: DataFrame, [Domain, Sr_Score]
            tld_scores: DataFrame, [Domain, St_Score]
            link_scores: DataFrame, [Domain, Sp_Score]

        Returns:
            DataFrame: [Domain, Sd_Score, Sr_Score, St_Score, Sp_Score, Final_Score]
        """
        logger.info("开始 AHP 权重融合...")

        # 逐步合并
        df = user_scores.merge(registrar_scores, on="Domain", how="outer")
        df = df.merge(tld_scores, on="Domain", how="outer")
        df = df.merge(link_scores, on="Domain", how="outer")

        # 空值填 0
        df = df.fillna(0)

        # 计算最终加权得分
        df['Final_Score'] = (
            df['Sd_Score'] * self.weights['Sd'] +
            df['Sr_Score'] * self.weights['Sr'] +
            df['St_Score'] * self.weights['St'] +
            df['Sp_Score'] * self.weights['Sp']
        )

        # 排序
        df = df.sort_values(by="Final_Score", ascending=False).reset_index(drop=True)

        logger.info("AHP 权重融合完成")
        return df


# =================== 使用示例 ===================
if __name__ == "__main__":
    AHP_WEIGHTS = {"Sd": 0.4, "Sr": 0.2, "St": 0.2, "Sp": 0.2}

    ahp = AHPIntegrator(AHP_WEIGHTS)

    # 假设已有四个模块的结果
    # user_scores = pd.DataFrame({"Domain": [...], "Sd_Score": [...]})
    # registrar_scores = pd.DataFrame({"Domain": [...], "Sr_Score": [...]})
    # tld_scores = pd.DataFrame({"Domain": [...], "St_Score": [...]})
    # link_scores = pd.DataFrame({"Domain": [...], "Sp_Score": [...]})
    #
    # final_scores = ahp.integrate(user_scores, registrar_scores, tld_scores, link_scores)
    # print(final_scores.head())
