from typing import List
import sys
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import time

project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from src.data_foundation.step import Step

class Pipeline:
    """資料處理Pipeline"""
    STEP_NAMES = {
        'CSVExtractor': 'CSV 資料擷取',
        'DataFrameCreator': 'DataFrame 建立',
        'NumberStringFormatter': '數字字串格式標準化',
        'DateFormatter': '日期格式標準化',
        'AddressFullToHalf': '地址全形轉半形',
        'AddressSplitter': '地址分割處理',
        'AddressParser': '地址解析轉換',
    }
    
    def __init__(self, file_path: str):
        self.steps: List[Step] = []
        self.file_path = file_path
        self.data = None
        self._start_time = None
        
    def add_step(self, step: Step) -> 'Pipeline':
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
            
            # 顯示處理開始
            print("\n" + "="*80)
            print("開始資料處理流程")
            print("="*80)
            
            # 建立總進度條
            with tqdm(total=total_steps, 
                     desc="總進度",
                     bar_format="{desc}: {percentage:3.0f}%|{bar:50}| {n_fmt}/{total_fmt}",
                     colour='blue') as pbar:
                
                for i, step in enumerate(self.steps, 1):
                    step_name = self._get_step_name(step)
                    step_start_time = time.time()
                    
                    # 顯示當前步驟
                    print(f"\n【步驟 {i}/{total_steps}】{step_name}")
                    print("-"*80)
                    
                    # 執行步驟
                    if i == 1:  # CSV提取步驟
                        self.data = step.process(self.file_path)
                    else:
                        result = step.process(self.data)
                        self.data = result[0] if isinstance(result, tuple) else result
                    
                    # 計算並顯示步驟執行時間
                    step_time = time.time() - step_start_time
                    print(f"✓ {step_name} 完成 (耗時: {self._format_time(step_time)})")
                    
                    # 更新總進度條
                    pbar.update(1)
            
            # 顯示總執行時間和完成訊息
            total_time = time.time() - self._start_time
            print("\n" + "="*80)
            print("資料處理流程完成!")
            print(f"總執行時間: {self._format_time(total_time)}")
            print("="*80 + "\n")
            
            return self.data
            
        except Exception as e:
            print(f"\n錯誤: 執行失敗於步驟 {i} ({step_name})")
            print(f"錯誤訊息: {str(e)}")
            raise