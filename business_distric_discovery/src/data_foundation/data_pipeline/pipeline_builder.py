import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from src.data_foundation.data_pipeline.pipeline import Pipeline

from src.data_foundation.data_ingestion.csv_extractor import CsvExtractor


class PipelineBuilder:
    """使用Builder模式建構Pipeline"""
    
    def __init__(self, file_path: str):
        self.pipeline = Pipeline(file_path)
    
    def add_csv_extractor(self) -> 'PipelineBuilder':
        self.pipeline.add_step(CsvExtractor())
        return self
        
    
    def build(self) -> Pipeline:
        return self.pipeline