from typing import Dict, Any
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
from pandas import DataFrame

from src.data_foundation.step import Step


class DataComparison(Step):
    """
    處理營運狀態更新的處理器。
    比對兩個DataFrame的統一編號，並更新營運狀態。
    """
    
    def __init__(self):
        """
        初始化處理器，設定狀態對應。
        """
        super().__init__()
        self.status_mapping: Dict[str, str] = {
            'both': '營運中',
            'left_only': '結束營運',
            'right_only': '新增營運'
        }
        self.statistics: Dict[str, int] = {
            '營運中': 0,
            '結束營運': 0,
            '新增營運': 0
        }
        
    def process(self, data: Dict[str, DataFrame]) -> DataFrame:
        """
        處理並更新營運狀態。

        參數:
            data (Dict[str, DataFrame]): 包含基準資料(base_df)和比對資料(adj_df)的字典
                                       格式: {'base_df': DataFrame, 'adj_df': DataFrame}
        
        回傳:
            DataFrame: 更新營運狀態後的資料表
        """
        base_df = data['base_df']
        adj_df = data['adj_df']
        
        # 合併兩個DataFrame並標示來源
        merged_df = pd.merge(base_df, adj_df, 
                           on='統一編號',  
                           how='outer',
                           indicator=True)

        # 建立新的DataFrame，初始化營運狀態欄位
        result_df = base_df.copy()
        result_df['營運狀態'] = self.status_mapping['left_only']

        # 更新營運中狀態
        both_mask = merged_df['_merge'] == 'both'
        both_ids = merged_df[both_mask]['統一編號']
        result_df.loc[result_df['統一編號'].isin(both_ids), '營運狀態'] = self.status_mapping['both']

        # 處理新增營運
        right_only_mask = merged_df['_merge'] == 'right_only'
        new_records = adj_df[adj_df['統一編號'].isin(merged_df[right_only_mask]['統一編號'])]
        new_records['營運狀態'] = self.status_mapping['right_only']

        # 合併結果
        final_df = pd.concat([result_df, new_records], ignore_index=True)
        
        # 更新統計資訊
        self.statistics['營運中'] = len(both_ids)
        self.statistics['結束營運'] = len(result_df) - len(both_ids)
        self.statistics['新增營運'] = len(new_records)
        
        return final_df
    
    def display_statistics(self) -> None:
        """
        顯示營運狀態的統計資訊。
        包含各狀態的數量及占比。
        """
        total = sum(self.statistics.values())
        if total == 0:
            print("沒有資料可供統計")
            return
            
        print("\n=== 營運狀態統計 ===")
        print(f"總筆數: {total:,} 筆")
        print("-" * 20)
        
        for status, count in self.statistics.items():
            percentage = (count / total) * 100
            print(f"{status}: {count:,} 筆 ({percentage:.1f}%)")