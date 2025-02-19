import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from src.data_pipeline.expansion_pipeline import DataExpansionPipeline

import pandas as pd

from src.data_enrichment.expansion.data_expansion import CityTownStrip




# 先建立新的 DataFramePipelineBuilder
class DataExpansionPipelineBuilder:
    """使用Builder模式建構處理DataFrame的Pipeline"""
    
    def __init__(self, df: pd.DataFrame):
        self.pipeline = DataExpansionPipeline(df)
    
    def add_city_and_town(self) -> 'DataExpansionPipelineBuilder':
        self.pipeline.add_step(CityTownStrip())
        return self
        
    def build(self) -> DataExpansionPipeline:
        return self.pipeline