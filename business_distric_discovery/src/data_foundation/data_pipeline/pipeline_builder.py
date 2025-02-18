import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from src.data_foundation.data_pipeline.pipeline import Pipeline

from src.data_foundation.data_ingestion.csv_extractor import CsvExtractor
from src.data_foundation.data_formatting.df_creator import DataFrameCreator
from src.data_foundation.data_formatting.type_converter import NumToStrFormat
from src.data_foundation.data_formatting.type_converter import DateFormat
from src.data_foundation.data_formatting.addr_full_to_half import AddrFullToHalf
from src.data_foundation.data_formatting.addr_splitter import AddressSplitter
from src.data_foundation.data_formatting.addr_parser import AddressParser


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
    
    def add_date_format(self) -> 'PipelineBuilder':
        self.pipeline.add_step(DateFormat())
        return self
    
    def add_addr_full_to_half(self, output_column: str) -> 'PipelineBuilder':
        self.pipeline.add_step(AddrFullToHalf(output_column=output_column))
        return self
    
    def add_addr_splitter(self) -> 'PipelineBuilder':
        self.pipeline.add_step(AddressSplitter())
        return self
    
    def add_addr_parser(self) -> 'PipelineBuilder':
        self.pipeline.add_step(AddressParser())
        return self
        
    def build(self) -> Pipeline:
        return self.pipeline