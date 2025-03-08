import logging
from datetime import datetime
from typing import Dict, Optional
import pandas as pd
from pathlib import Path

from src.data_pipeline.pipeline_builder import PipelineBuilder
from src.data_foundation.data_formatting.data_comparison import DataComparison

class DataProcessingSystem:
    """資料處理系統核心"""
    
    def __init__(self, project_root: str):
        """
        初始化資料處理系統
        
        Args:
            project_root: 專案根目錄路徑
        """
        self.project_root = Path(project_root)
        self.setup_logging()
        self.comparison = DataComparison() 
        
    def setup_logging(self):
        """設定基本日誌"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
    def check_file_exists(self, file_path: Path) -> bool:
        """
        檢查檔案是否存在
        
        Args:
            file_path: 檔案路徑
            
        Returns:
            bool: 檔案是否存在
        """
        if not file_path.exists():
            logging.error(f"找不到檔案: {file_path}")
            return False
        return True
    
    def initialize_base_data(self, version1: str, version2: str) -> bool:
        """
        初始化基準資料 (使用兩個版本的資料)
        
        Args:
            version1: 第一個版本 (例如: '11312')
            version2: 第二個版本 (例如: '11401')
            
        Returns:
            bool: 處理是否成功
        """
        try:
            logging.info(f"開始初始化基準資料 (版本: {version1} 和 {version2})")
            
            # 檢查檔案是否存在
            file_path1 = self.project_root / 'data' / 'raw' / f'BusinessRegistration_{version1}.csv'
            file_path2 = self.project_root / 'data' / 'raw' / f'BusinessRegistration_{version2}.csv'
            
            if not self.check_file_exists(file_path1) or not self.check_file_exists(file_path2):
                return False
            
            # 處理第一個版本
            print(f'處理 {version1} 版本資料...')
            data1 = (PipelineBuilder(str(file_path1))
                    .add_csv_extractor()
                    .add_df_creator()
                    .add_num_str_format()
                    .add_date_format()
                    .add_addr_full_to_half(output_column='正規化營業地址')
                    .add_addr_splitter()
                    .add_addr_parser()
                    .build()
                    .execute())
            
            # 處理第二個版本
            print(f'處理 {version2} 版本資料...')
            data2 = (PipelineBuilder(str(file_path2))
                    .add_csv_extractor()
                    .add_df_creator()
                    .add_num_str_format()
                    .add_date_format()
                    .add_addr_full_to_half(output_column='正規化營業地址')
                    .add_addr_splitter()
                    .add_addr_parser()
                    .build()
                    .execute())
            
            # 比較資料
            
            base_data = self.comparison.process({
                'base_df': data1,
                'adj_df': data2
            })
            
            # 建立輸出目錄
            output_dir = self.project_root / 'data' / 'output'/'base'
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # 儲存基準資料
            output_path = output_dir / 'BusinessRegistration_base.pkl'
            base_data.to_pickle(str(output_path))
            
            logging.info("基準資料初始化完成")
            print(f"\n{version1} 和 {version2} 版本比較結果:")
            self.comparison.display_statistics()
            return True
            
        except Exception as e:
            logging.error(f"初始化基準資料失敗: {str(e)}")
            return False
    
    def update_base_data(self, version: str) -> bool:
        """
        更新基準資料
        
        Args:
            version: 新的資料版本 (例如: '11402')
            
        Returns:
            bool: 更新是否成功
        """
        try:
            logging.info(f"開始更新基準資料 (版本: {version})")
            
            # 檢查檔案是否存在
            base_path = self.project_root / 'data' / 'output' / 'base'/'BusinessRegistration_base.pkl'
            new_file_path = self.project_root / 'data' / 'raw' / f'BusinessRegistration_{version}.csv'
            
            if not self.check_file_exists(base_path) or not self.check_file_exists(new_file_path):
                return False
            
            # 讀取現有基準資料
            base_data = pd.read_pickle(str(base_path))
            
            # 處理新版本資料
            print(f'處理 {version} 版本資料...')
            new_data = (PipelineBuilder(str(new_file_path))
                      .add_csv_extractor()
                      .add_df_creator()
                      .add_num_str_format()
                      .add_date_format()
                      .add_addr_full_to_half(output_column='正規化營業地址')
                      .add_addr_splitter()
                      .add_addr_parser()
                      .build()
                      .execute())
            
            # 比較並更新資料
            updated_base = self.comparison.process({
                'base_df': base_data,
                'adj_df': new_data
            })

            
            
            print(f'儲存更新基準資料')
            updated_base.to_pickle(str(base_path))
            updated_base.to_csv(str(self.project_root / 'data' / 'processed'/'BusinessRegistration_base.csv'))
            
            logging.info("基準資料更新完成")
            print(f"\n{version} 版本更新比較結果:")
            self.comparison.display_statistics()
            return True
            
            
        except Exception as e:
            logging.error(f"更新基準資料失敗: {str(e)}")
            return False