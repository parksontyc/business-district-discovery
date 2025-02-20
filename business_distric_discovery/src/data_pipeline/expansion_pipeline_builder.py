import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from src.data_pipeline.expansion_pipeline import DataExpansionPipeline

import pandas as pd

from src.data_enrichment.expansion.data_expansion import CityTownStrip
from src.data_enrichment.expansion.data_expansion import BizIDCategoryClassifier
from src.data_enrichment.expansion.data_expansion import DateProcessor

from src.utils.biz_id_category import MajorCategory, MediumCategory, DetailedSubcategory, Subcategory




# 先建立新的 DataFramePipelineBuilder
class DataExpansionPipelineBuilder:
    """使用Builder模式建構處理DataFrame的Pipeline"""
    
    def __init__(self, df: pd.DataFrame):
        self.pipeline = DataExpansionPipeline(df)
    
    def add_city_and_town(self) -> 'DataExpansionPipelineBuilder':
        self.pipeline.add_step(CityTownStrip())
        return self
    
    def add_bizid_category_classifier(self) -> 'DataExpansionPipelineBuilder':
        self.pipeline.add_step(BizIDCategoryClassifier(
            major_category=MajorCategory,
            medium_category=MediumCategory,
            subcategory=Subcategory,
            detailed_subcategory=DetailedSubcategory))
        return self
    
    def add_date_processor(self, **kwargs) -> 'DataExpansionPipelineBuilder':
        """
        添加日期處理器
        
        Args:
            **kwargs: 傳遞給DateProcessor的參數
        """
        self.pipeline.add_step(DateProcessor(**kwargs))
        return self
        
    def build(self) -> DataExpansionPipeline:
        return self.pipeline