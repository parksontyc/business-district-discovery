from typing import List

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from src.data_foundation.step import Step

class Pipeline:
    """資料處理Pipeline, 用於管理和執行一系列處理步驟"""
    
    def __init__(self, file_path: str):
        self.steps: List[Step] = []
        self.file_path = file_path
        self.data = None
        self.current_step = 0
        
    def add_step(self, step: Step) -> 'Pipeline':
        """新增處理步驟"""
        self.steps.append(step)
        return self
        
    def execute(self) -> any:
        """執行所有處理步驟"""
        try:
            total_steps = len(self.steps)
            for i, step in enumerate(self.steps, 1):
                print(f"\n執行步驟 {i}/{total_steps}: {step.__class__.__name__}")
                if i == 1:  # CSV提取步驟
                    self.data = step.process(self.file_path)
                else:
                    # 處理可能返回tuple的情況
                    result = step.process(self.data)
                    # 如果返回tuple，取第一個元素(DataFrame)
                    self.data = result[0] if isinstance(result, tuple) else result
                    
            return self.data
            
        except Exception as e:
            print(f"Pipeline執行失敗於步驟 {i}: {str(e)}")
            raise