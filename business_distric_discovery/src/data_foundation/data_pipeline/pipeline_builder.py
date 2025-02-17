import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from src.data_foundation.data_pipeline.pipeline import Pipeline

from src.data_foundation.data_ingestion.csv_extractor import CsvExtractor
from src.data_foundation.data_formatting.df_creator import DataFrameCreator
from src.data_foundation.data_formatting.type_converter import NumToStrFormat
from src.data_foundation.data_formatting.type_converter import DateFormat


class PipelineBuilder:
    """使用Builder模式建構Pipeline"""
    
    def __init__(self, file_path: str):
        self.pipeline = Pipeline(file_path)
    
    def add_csv_extractor(self) -> 'PipelineBuilder':
        self.pipeline.add_step(CsvExtractor())
        return self
    
    def add_df_creator(self) -> 'PipelineBuilder':
        self.pipeline.add_step(DataFrameCreator())
        return self
    
    def add_num_str_format(self) -> 'PipelineBuilder':
        self.pipeline.add_step(NumToStrFormat())
        return self
    
        
    
    def build(self) -> Pipeline:
        return self.pipeline