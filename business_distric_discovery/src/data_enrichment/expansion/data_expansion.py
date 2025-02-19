import re
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd

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