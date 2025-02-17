import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd

from src.data_foundation.step import Step


class NumToStrFormat(Step):
    def __init__(self):
        super().__init__()
        
        # 定義欄位格式化規則
        self.format_rules = {
            '行業代號': {'zfill_length': 6},
            '統一編號': {'zfill_length': 8},
            '總機構統一編號': {'zfill_length': 8}
        }

    def _format_numeric_field(self, df: pd.DataFrame, column_name: str, 
                            zfill_length: int) -> pd.DataFrame:
        """
        處理數值欄位的通用方法
        
        Args:
            df (pd.DataFrame): 要處理的DataFrame
            column_name (str): 要處理的欄位名稱
            zfill_length (int): 補零後的長度
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        # 複製DataFrame避免修改原始資料
        result_df = df.copy()
        
        # 轉換為數值
        result_df[column_name] = pd.to_numeric(result_df[column_name], errors='coerce')
        
        # 只處理非空值
        mask = result_df[column_name].notna()
        
        # 格式化並補零
        result_df.loc[mask, column_name] = result_df.loc[mask, column_name].apply(
            lambda x: str(int(x)).zfill(zfill_length)
        )
        
        # 轉換為分類型別
        result_df[column_name] = pd.Categorical(result_df[column_name])
        
        return result_df

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        處理所有需要格式化的欄位
        
        Args:
            df (pd.DataFrame): 輸入的DataFrame
            
        Returns:
            pd.DataFrame: 格式化後的DataFrame
            
        Raises:
            ValueError: 當格式化過程失敗時
        """
        try:
            # 檢查是否有需要處理的欄位
            columns_to_process = [col for col in self.format_rules.keys() if col in df.columns]
            if not columns_to_process:
                print("Warning: 未找到需要格式化的欄位")
                return df
            
            # 複製DataFrame以避免修改原始資料
            result_df = df.copy()
            
            # 對每個欄位應用格式化規則
            for column, rules in self.format_rules.items():
                if column in result_df.columns:
                    result_df = self._format_numeric_field(
                        df=result_df,
                        column_name=column,
                        zfill_length=rules['zfill_length']
                    )
            
            return result_df
            
        except Exception as e:
            raise ValueError(f"欄位格式化失敗：{str(e)}")


class DateFormat(Step):
    def __init__(self):
        super().__init__()

    def convert_date(self, date_str: str) -> str:
        """將中華民國年份轉換為西元年份的日期字串
        
        Args:
            date_str (str): 中華民國年份的日期字串
            
        Returns:
            str: 轉換後的日期字串 (YYYY-MM-DD格式)，或None如果轉換失敗
        """
        try:
            # 確保數據是 6 或 7 位的數字
            if not date_str.isdigit() or not (6 <= len(date_str) <= 7):
                return None
            
            # 根據長度決定年份解析方式
            if len(date_str) == 7:
                year = int(date_str[:3]) + 1911  # 7 位數：取前三位（如 108）
            elif len(date_str) == 6:
                year = int(date_str[:2]) + 1911  # 6 位數：取前兩位（如 40）

            month = date_str[-4:-2]  # 倒數第 4~5 位是月份
            day = date_str[-2:]      # 最後兩位是日期
            return f"{year}-{month}-{day}"
        except:
            return None

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """處理DataFrame中的日期欄位
        
        Args:
            df (pd.DataFrame): 輸入的DataFrame
            
        Returns:
            pd.DataFrame: 日期格式轉換後的DataFrame
            
        Raises:
            ValueError: 當日期格式轉換失敗時
        """
        try:
            # 檢查必要欄位
            if '設立日期' not in df.columns:
                raise ValueError("找不到'設立日期'欄位")
                
            # 複製DataFrame以避免修改原始資料
            result_df = df.copy()
            
            # 確保日期欄位為字串格式，並去掉小數點
            result_df['設立日期'] = (result_df['設立日期']
                                   .astype(str)
                                   .str.strip()
                                   .str.replace('.0', '', regex=False))
            
            # 轉換日期格式
            result_df['設立日期'] = result_df['設立日期'].apply(self.convert_date)
            
            # 轉換為datetime格式
            result_df['設立日期'] = pd.to_datetime(
                result_df['設立日期'], 
                format='%Y-%m-%d', 
                errors='coerce'
            )
            
            return result_df

        except Exception as e:
            raise ValueError(f"日期格式轉換失敗：{str(e)}")
