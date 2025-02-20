from typing import List
import sys
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import time

import pandas as pd

project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from src.data_foundation.step import Step


# 1. 建立基礎的 DataFramePipeline 類別
class DataExpansionPipeline:
    """處理 DataFrame 的 Pipeline"""
    STEP_NAMES = {
        'CityTownStrip': '縣市行政區擷取',
        'BizIDCategoryClassifier':'行業代號分類',
        'DateProcessor':'設立日期轉換'
    }
    
    def __init__(self, df: pd.DataFrame):
        self.steps: List[Step] = []
        self.data = df  # 直接存儲 DataFrame
        self._start_time = None
        
    def add_step(self, step: Step) -> 'DataExpansionPipeline':
        """新增處理步驟"""
        self.steps.append(step)
        return self
    
    def _format_time(self, seconds: float) -> str:
        """格式化時間顯示"""
        if seconds < 60:
            return f"{seconds:.1f} 秒"
        minutes = int(seconds // 60)
        seconds = seconds % 60
        if minutes < 60:
            return f"{minutes} 分 {seconds:.1f} 秒"
        hours = int(minutes // 60)
        minutes = minutes % 60
        return f"{hours} 小時 {minutes} 分 {seconds:.1f} 秒"
    
    def _get_step_name(self, step: Step) -> str:
        """取得步驟的中文名稱"""
        class_name = step.__class__.__name__
        return self.STEP_NAMES.get(class_name, class_name)
    
    def execute(self) -> any:
        """執行所有處理步驟"""
        try:
            total_steps = len(self.steps)
            self._start_time = time.time()
            
            print("\n" + "="*80)
            print("開始 DataFrame 處理流程")
            print("="*80)
            
            with tqdm(total=total_steps, 
                     desc="總進度",
                     bar_format="{desc}: {percentage:3.0f}%|{bar:50}| {n_fmt}/{total_fmt}",
                     colour='blue') as pbar:
                
                for i, step in enumerate(self.steps, 1):
                    step_name = self._get_step_name(step)
                    step_start_time = time.time()
                    
                    print(f"\n【步驟 {i}/{total_steps}】{step_name}")
                    print("-"*80)
                    
                    # 處理 DataFrame
                    result = step.process(self.data)
                    self.data = result[0] if isinstance(result, tuple) else result
                    
                    step_time = time.time() - step_start_time
                    print(f"✓ {step_name} 完成 (耗時: {self._format_time(step_time)})")
                    
                    pbar.update(1)
            
            total_time = time.time() - self._start_time
            print("\n" + "="*80)
            print("DataFrame 處理流程完成!")
            print(f"總執行時間: {self._format_time(total_time)}")
            print("="*80 + "\n")
            
            return self.data
            
        except Exception as e:
            print(f"\n錯誤: 執行失敗於步驟 {i} ({step_name})")
            print(f"錯誤訊息: {str(e)}")
            raise

