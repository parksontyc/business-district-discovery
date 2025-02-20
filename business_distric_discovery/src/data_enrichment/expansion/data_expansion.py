import re
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from typing import Dict, Any, Callable, List, Optional
from datetime import datetime

import pandas as pd
import numpy as np

from src.data_foundation.step import Step


class CityTownStrip(Step):
    def __init__(self):
        super().__init__()
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # 假設前3個字為縣市，只要其中含有「市」或「縣」
        df['縣市'] = df['正規化營業地址'].apply(
            lambda x: x[:3] if ('市' in x[:3] or '縣' in x[:3]) else None
        )
        
        def extract_district(address):
            # 若地址前3個字是縣市，則將其去除，剩下部分用來匹配行政區
            county = address[:3] if ('市' in address[:3] or '縣' in address[:3]) else ''
            remaining = address[len(county):]
            # 用正則：行政區通常由1～2個字加上鄉、鎮、市、區其中一個組成
            m = re.match(r'^(.{1,2}[鄉鎮市區])', remaining)
            if m:
                return m.group(1)
            return None
            
        df['行政區'] = df['正規化營業地址'].apply(extract_district)
        
        return df
    

class BizIDCategoryClassifier(Step):
   """產業分類處理類"""
   
   def __init__(self, major_category: Dict, medium_category: Dict, 
                subcategory: Dict, detailed_subcategory: Dict):
       """
       初始化分類處理器
       
       Args:
           medium_category (Dict): 中類對照字典
           subcategory (Dict): 小類對照字典
           detailed_subcategory (Dict): 細類對照字典
       """
       super().__init__()
       
       self.major_category = major_category
       self.medium_category = medium_category
       self.subcategory = subcategory
       self.detailed_subcategory = detailed_subcategory

   def _convert_industry_code(self, df: pd.DataFrame) -> None:
       """將行業代號轉為字串型態"""
       df['行業代號'] = df['行業代號'].astype(str)

   def _get_major_category(self, code: str) -> str:
       """取得大類名稱"""
       return self.major_category.get(str(code)[:2], '')

   def _get_medium_category(self, code: str) -> str:
       """取得中類名稱""" 
       return self.medium_category.get(str(code)[:2], '')

   def _get_subcategory(self, code: str) -> str:
       """取得小類名稱"""
       return self.subcategory.get(str(code)[:3], '')

   def _get_detailed_subcategory(self, code: str) -> str:
       """取得細類名稱"""
       return self.detailed_subcategory.get(str(code)[:4], '')

   def _insert_category_column(self, df: pd.DataFrame, column_name: str, 
                             category_func: Callable[[str], str], 
                             position_ref: str) -> None:
       """
       在參考欄位前插入新的分類欄位
       
       Args:
           df (pd.DataFrame): 要處理的DataFrame
           column_name (str): 新欄位名稱
           category_func (Callable): 分類函數
           position_ref (str): 參考欄位名稱
       """
       # 新增分類欄位
       df[column_name] = df['行業代號'].apply(category_func)
       # 取得參考欄位的位置
       column_position = df.columns.get_loc(position_ref)
       # 將新欄位插入到參考欄位之前
       df.insert(column_position, column_name, df.pop(column_name))

   def process(self, df: pd.DataFrame) -> pd.DataFrame:
       """
       處理產業分類並返回更新後的 DataFrame
       
       Args:
           df (pd.DataFrame): 要處理的DataFrame
           
       Returns:
           pd.DataFrame: 包含新增分類欄位的 DataFrame
       """
       try:
           # 轉換行業代號格式
           self._convert_industry_code(df)

           # 依序插入中類、小類及細類欄位
           category_mappings = [
               ('大類', self._get_major_category),
               ('中類', self._get_medium_category),
               ('小類', self._get_subcategory),
               ('細類', self._get_detailed_subcategory)
           ]

           for column_name, category_func in category_mappings:
               self._insert_category_column(
                   df=df,
                   column_name=column_name,
                   category_func=category_func,
                   position_ref='行業代號'
               )

           return df

       except Exception as e:
           raise ValueError(f"產業分類處理失敗：{str(e)}")
       

class DateProcessor(Step):
    """日期處理類"""
    
    def __init__(self, 
                 date_column: str = '設立日期',
                 enable_year: bool = True,
                 enable_month: bool = False,
                 enable_quarter: bool = True,
                 enable_operation_years: bool = True,
                 enable_decade: bool = False):
        """
        初始化日期處理器
        
        Args:
            date_column (str): 要處理的日期欄位名稱
            enable_year (bool): 是否啟用年份提取
            enable_month (bool): 是否啟用月份提取
            enable_quarter (bool): 是否啟用季度提取
            enable_operation_years (bool): 是否啟用營業年數計算
            enable_decade (bool): 是否啟用年代計算
        """
        super().__init__()
        self.date_column = date_column
        self.enable_year = enable_year
        self.enable_month = enable_month
        self.enable_quarter = enable_quarter
        self.enable_operation_years = enable_operation_years
        self.enable_decade = enable_decade
        
    def _convert_to_datetime(self, df: pd.DataFrame) -> None:
        """將日期欄位轉換為datetime格式"""
        df[self.date_column] = pd.to_datetime(df[self.date_column])
        
    def _extract_year(self, df: pd.DataFrame) -> None:
        """提取年份並轉為類別"""
        # 先提取年份
        years = df[self.date_column].dt.year
        
        # 處理NaN值
        years_filled = years.fillna(-1)  # 使用-1表示缺失值
        
        # 先轉為整數
        years_int = years_filled.astype('int')
        
        # 再轉為類別
        df['設立年份'] = pd.Categorical(years_int)
        
        # 將-1轉回NaN（如果需要的話）
        df.loc[df['設立年份'] == -1, '設立年份'] = pd.NA

        # 如果想要查看有多少NaN值，可以加入以下檢查（選擇性）
        nan_count = years.isna().sum()
        if nan_count > 0:
            print(f"警告：設立年份中有 {nan_count} 筆空值")
        
    def _extract_month(self, df: pd.DataFrame) -> None:
        """提取月份"""
        df['設立月份'] = df[self.date_column].dt.month
        
    def _extract_quarter(self, df: pd.DataFrame) -> None:
        """提取季度並轉換格式"""
        quarter_mapping = {1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'}
        df['設立季度'] = df[self.date_column].dt.quarter.map(quarter_mapping)
        
    def _calculate_operation_years(self, df: pd.DataFrame) -> None:
        """計算營業年數(取至小數點後1位)"""
        df['營業年數'] = ((datetime.now() - df[self.date_column]).dt.days / 365).round(1)
        
    def _calculate_decade(self, df: pd.DataFrame) -> None:
        """計算設立年代"""
        df['設立年代'] = (df[self.date_column].dt.year // 10) * 10
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        處理日期資料
        
        Args:
            df (pd.DataFrame): 要處理的DataFrame
            
        Returns:
            pd.DataFrame: 處理後的DataFrame
        """
        try:
            # 轉換日期格式
            self._convert_to_datetime(df)
            
            # 根據設定執行各項處理
            if self.enable_year:
                self._extract_year(df)
                
            if self.enable_month:
                self._extract_month(df)
                
            if self.enable_quarter:
                self._extract_quarter(df)
                
            if self.enable_operation_years:
                self._calculate_operation_years(df)
                
            if self.enable_decade:
                self._calculate_decade(df)
                
            return df
            
        except Exception as e:
            raise ValueError(f"日期處理失敗：{str(e)}")
        


class ChainIdentifier(Step):
    def __init__(self):
        super().__init__()
        
    def process(self, df: pd.DataFrame):
        # 根據「總機構統一編號」出現次數決定「型態」欄位
        df['營業型態'] = np.where(
            df.groupby('總機構統一編號')['總機構統一編號'].transform('count') >= 2,
            '連鎖',
            '非連鎖'
        )
        return df