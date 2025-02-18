import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from typing import Any, List

import pandas as pd
from tqdm import tqdm

from src.data_foundation.step import Step

class CsvExtractor(Step):
    def __init__(self, chunksize: int = 10000, include_header: bool = True):
        """
        初始化 CSV 提取器
        
        Args:
            chunksize (int): 每次讀取的行數，預設為 10000
            include_header (bool): 是否保留欄位名稱，預設為 True
        """
        super().__init__()
        self.chunksize = chunksize
        self.include_header = include_header
        
    def _count_lines(self, file_path: str) -> int:
        """
        計算檔案總行數
        
        Args:
            file_path (str): CSV檔案路徑
            
        Returns:
            int: 檔案的總行數
        """
        try:
            with open(file_path, 'r', encoding='utf8') as f:
                return sum(1 for _ in f)
        except Exception as e:
            print(f'Error counting lines: {e}')
            return 0

    def _extract_period(self, file_path: str) -> str:
        """
        從檔案路徑中提取期別資訊
        
        Args:
            file_path (str): CSV檔案路徑
            
        Returns:
            str: 期別資訊（如：11312）
        """
        try:
            # 取得檔名（不含副檔名）
            file_name = Path(file_path).stem
            # 提取最後的數字部分
            period = ''.join(filter(str.isdigit, file_name))
            return period
        except Exception as e:
            print(f'Error extracting period: {e}')
            return ''
        
    def process(self, file_path: str) -> List[List[Any]]:
        """
        從指定的 CSV 檔案中分塊提取數據，並轉換為二維列表。
        同時加入資料期別欄位。
        
        Args:
            file_path (str): CSV檔案路徑
            
        Returns:
            List[List[Any]]: 包含數據的二維列表，如果發生錯誤則返回空列表
        """
        try:
            # 檢查檔案是否存在
            if not Path(file_path).exists():
                raise FileNotFoundError(f'The file {file_path} was not found.')
            
            # 計算總行數
            total_rows = self._count_lines(file_path)
            print(f'Total rows: {total_rows:,}')
            
            # 提取期別
            period = self._extract_period(file_path)
            
            extract_list = []
            
            # 設定進度條
            with tqdm(
                total=total_rows,
                desc="Extracting rows",
                unit="rows",
                unit_scale=True,
                ncols=80,
                dynamic_ncols=True
            ) as pbar:
                # 分塊讀取 CSV
                for i, chunk in enumerate(pd.read_csv(
                    file_path,
                    encoding='utf8',
                    chunksize=self.chunksize,
                    on_bad_lines='warn'  # 處理損壞的行
                )):
                    # 處理標題行
                    if self.include_header and i == 0:
                        # 在標題加入期別欄位
                        headers = chunk.columns.tolist()
                        headers.append('期別')
                        extract_list.append(headers)
                    
                    # 在每一列加入期別
                    chunk_data = chunk.values.tolist()
                    for row in chunk_data:
                        row.append(period)
                    
                    # 加入數據內容
                    extract_list.extend(chunk_data)
                    
                    # 更新進度條
                    pbar.update(len(chunk))
            
            # print('\nExtraction completed successfully')
            return extract_list
            
        except FileNotFoundError as e:
            print(f'Error: {e}')
        except pd.errors.EmptyDataError:
            print(f'Error: The file {file_path} is empty.')
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f'An unexpected error occurred: {e}')
            
        return []