import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd

from src.data_foundation.step import Step


class DataFrameCreator(Step):
    '''建立df
    1.統一編號欄位如為空值，則該列資料移除'''
    def __init__(self):
        super().__init__()
        
    def process(self, data: any) -> pd.DataFrame:
        df = pd.DataFrame(data[1:], columns=data[0])
        df = df[df['統一編號'].notna()]

        return df.reset_index(drop=True)