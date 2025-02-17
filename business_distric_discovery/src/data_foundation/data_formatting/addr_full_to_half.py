import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

import re

import pandas as pd

from src.data_foundation.step import Step


class AddrFullToHalf(Step):
    '''全形轉半形轉換器
    
    Args:
        input_column (str): 輸入地址欄位名稱
        output_column (str): 輸出正規化地址欄位
    '''
    def __init__(self, output_column: str):
        super().__init__()
        self.input_column = '營業地址'
        self.output_column = output_column

    def _convert_single_string(self, text: str) -> str:
        """轉換單個字串從全形到半形
        
        Args:
            text (str): 要轉換的字串
            
        Returns:
            str: 轉換後的字串
        """
        if pd.isna(text):
            return text
        if not isinstance(text, str):
            text = str(text)

        # 全形轉半形
        converted = ''.join(
            chr(32) if ord(uchar) == 12288 else
            chr(ord(uchar) - 65248) if 65281 <= ord(uchar) <= 65374 else
            uchar
            for uchar in text
        )

        # 處理特殊字元和格式
        converted = re.sub(r'[^\u0000-\uFFFF]', '', converted)  # 移除非法 Unicode
        converted = converted.replace("―", "-")  # 處理破折號
        converted = re.sub(r'\s+', '', converted)  # 移除空白
        
        return converted
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        處理 DataFrame 中的地址欄位，新增正規化後的地址欄位
        
        Args:
            df (pd.DataFrame): 輸入的DataFrame
            
        Returns:
            pd.DataFrame: 包含新增正規化地址欄位的 DataFrame
        
        Raises:
            ValueError: 當指定的輸入欄位不存在時拋出
        """
        try:
            # 檢查輸入欄位是否存在
            if self.input_column not in df.columns:
                raise ValueError(f"找不到輸入欄位：{self.input_column}")
            
            # 複製DataFrame以避免修改原始資料
            result_df = df.copy()
            
            # 生成正規化後的地址序列
            normalized_addresses = result_df[self.input_column].apply(
                self._convert_single_string
            )
            
            # 找到原始欄位的位置
            input_column_idx = result_df.columns.get_loc(self.input_column)
            
            # 在原始欄位後插入正規化欄位
            result_df.insert(
                loc=input_column_idx + 1,  # 在原始欄位後插入
                column=self.output_column,
                value=normalized_addresses
            )
            
            return result_df
            
        except Exception as e:
            raise ValueError(f"地址正規化處理失敗：{str(e)}")