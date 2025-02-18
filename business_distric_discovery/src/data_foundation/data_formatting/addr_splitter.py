import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

import re
from typing import List, Dict, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum

import pandas as pd

from src.data_foundation.step import Step


class SkipReason(Enum):
    """跳過原因的列舉"""
    INVALID_TYPE = '非字串格式'
    LAND_NUMBER = '包含地號'
    SECTION = '包含小段'
    INVALID_FORMAT = '非正規地址格式'

@dataclass
class AddressChange:
    """地址變更紀錄的資料類別"""
    original: str
    modified: str
    changes_made: List[str] = field(default_factory=list)

@dataclass
class SkippedAddress:
    """跳過處理的地址資料類別"""
    address: str
    reason: SkipReason
    type: str

class AddressSplitter(Step):
    """分割複數地址的處理類別"""
    
    # 類別常數定義保持不變
    DEFAULT_DELIMITERS = [',', '、', '及', '.', '﹒', '~', '–', '，', '˙', '丶']
    EXEMPT_ENDINGS = (
        '地下', '層', '樓', 'B1', ')', '室', '旁', '地', 
        '邊', '園', '公尺', '前', '面', '場', '路', '街', '巷', '段', 'F', '屋', '口',
        '側', '位', '攤', '棟', '區', '壁', '部', '棚', ']', '房', '櫃', '﹞', '1', '一', '內', '方', '鋪', '市', '庭','類', '分', '舖', '庫', '販', '右', '廊', '處', '外', '空', '慺','耬',
        '〕', '份', '半', '絡', '檈', '物', '間', '廳', '商', '〉', '後', '門', '樓`', '編', '院', '二', '摟', '橋', '等', '臨', '熡', '屢', '角', '下', '﹚', '牌', '近', '左', '>', '}', '厝', '址'
    )
    
    def __init__(self):
        """初始化地址分割處理器"""
        super().__init__()
        self.input_column = '正規化營業地址'
        self.output_column = '正規化營業地址'
        self.status_column = '地址狀態'
        delimiters: List[str] = None
        self.batch_size = 10000
        
        self.delimiters = delimiters or self.DEFAULT_DELIMITERS
        self.delimiters_pattern = re.compile('|'.join(map(re.escape, self.delimiters)))

        exempt_endings = '|'.join(map(re.escape, self.EXEMPT_ENDINGS))
        self.exempt_endings_pattern = re.compile(
            f"({exempt_endings}|之\\d+|號-\\d+|號\\d+|[A-Za-z]\\d+|樓-\\d+|樓\\d+|[A-Za-z]|附\\d+|位\\d+|層-\\d+|編號\\d+|層\\d+|編號\\d+)$"     
        )

        # 重置變更追蹤
        self.reset_changes()

    def reset_changes(self):
        """重置變更追蹤狀態"""
        self.changes = {
            'modified': [],
            'skipped': []
        }
        self.total = 0

    def _validate_address(self, addr: str) -> Union[SkippedAddress, None]:
        """驗證地址的有效性"""
        if not isinstance(addr, str):
            return SkippedAddress(addr, SkipReason.INVALID_TYPE, 'invalid')
        
        if '地號' in addr:
            return SkippedAddress(addr, SkipReason.LAND_NUMBER, 'land_number')
        
        if '小段' in addr:
            return SkippedAddress(addr, SkipReason.SECTION, 'section')
        
        if not re.search(r'[市區鄉鎮].*[號]', addr):
            return SkippedAddress(addr, SkipReason.INVALID_FORMAT, 'invalid')
        
        return None

    def _process_single_address(self, addr: str) -> Tuple[str, str]:
        """處理單一地址，回傳(處理後地址, 狀態)"""
        # 驗證地址
        validation_result = self._validate_address(addr)
        if validation_result:
            self.changes['skipped'].append(validation_result)
            return addr, validation_result.reason.value

        change_record = AddressChange(original=addr, modified=addr)
        
        # 檢查是否為複數地址
        is_multiple = bool(self.delimiters_pattern.search(addr))
        
        # 分割處理
        split_addr = self.delimiters_pattern.split(addr, maxsplit=1)[0]
        if split_addr != addr:
            change_record.modified = split_addr
            change_record.changes_made.append('分割')
            addr = split_addr

        # 補充「號」
        if not addr.endswith('號') and not self.exempt_endings_pattern.search(addr):
            addr += '號'
            change_record.modified = addr
            change_record.changes_made.append('補充號')

        # 檢查是否為"號+數字"結尾
        if re.search(r'(?<!攤)號\d+$', addr) and not addr.endswith('樓'):
            addr += '樓'
            change_record.modified = addr
            change_record.changes_made.append('補充樓')

        # 決定狀態
        if change_record.original != addr:
            if is_multiple:
                status = '複數地址'
            else:
                status = '已修改'
            self.changes['modified'].append(change_record)
        else:
            status = '未變動'

        return addr, status

    def _process_batch(self, batch: pd.Series) -> Tuple[pd.Series, pd.Series]:
        """批次處理地址，回傳(處理後地址序列, 狀態序列)"""
        results = batch.apply(self._process_single_address)
        addresses = pd.Series([r[0] for r in results])
        statuses = pd.Series([r[1] for r in results])
        return addresses, statuses

    def _print_report(self):
        """印出處理報告"""
        print("\n地址處理報告:")
        print("=" * 80)

        if self.changes['modified']:
            print("\n【已修改的地址】")
            for idx, record in enumerate(self.changes['modified'], 1):
                print(f"\n地址 {idx}:")
                print(f"原始地址：{record.original}")
                print(f"修改後：{record.modified}")
                print(f"修改類型：{' + '.join(record.changes_made)}")

        if self.changes['skipped']:
            print("\n【跳過處理的地址】")
            for idx, record in enumerate(self.changes['skipped'], 1):
                print(f"\n地址 {idx}:")
                print(f"原始地址：{record.address}")
                print(f"跳過原因：{record.reason.value}")

        print("\n處理統計:")
        print(f"修改的地址數：{len(self.changes['modified'])}個")
        print(f"跳過的地址數：{len(self.changes['skipped'])}個")
        print(f"總地址數：{self.total}個")

    def process(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        處理DataFrame中的地址欄位
        
        Args:
            df (pd.DataFrame): 輸入的DataFrame
            
        Returns:
            Tuple[pd.DataFrame, Dict]: 處理後的DataFrame和變更紀錄
            
        Raises:
            ValueError: 當找不到輸入欄位或處理失敗時
        """
        try:
            # 檢查輸入欄位
            if self.input_column not in df.columns:
                raise ValueError(f"找不到輸入欄位：{self.input_column}")

            # 重置變更追蹤
            self.reset_changes()
            
            # 更新總數
            self.total = len(df)
            
            # 複製DataFrame避免修改原始資料
            result_df = df.copy()
            
            # 初始化結果列表
            all_addresses = []
            all_statuses = []
            
            # 批次處理
            for start_idx in range(0, len(result_df), self.batch_size):
                batch = result_df[self.input_column].iloc[start_idx:start_idx + self.batch_size]
                addresses, statuses = self._process_batch(batch)
                all_addresses.extend(addresses.values)
                all_statuses.extend(statuses.values)

            # 將處理結果轉換為 Series，使用原始 DataFrame 的 index
            processed_addresses = pd.Series(all_addresses, index=df.index)
            processed_statuses = pd.Series(all_statuses, index=df.index)

            # 更新或插入處理後的欄位
            input_column_idx = result_df.columns.get_loc(self.input_column)
            
            # 如果輸出欄位已存在，先刪除
            if self.output_column in result_df.columns:
                result_df = result_df.drop(columns=[self.output_column])
            if self.status_column in result_df.columns:
                result_df = result_df.drop(columns=[self.status_column])
            
            # 在指定位置插入新欄位
            result_df.insert(
                loc=input_column_idx + 1,
                column=self.output_column,
                value=processed_addresses
            )
            result_df.insert(
                loc=input_column_idx + 2,
                column=self.status_column,
                value=processed_statuses
            )

            # 印出報告
            self._print_report()
            
            return result_df, self.changes

        except Exception as e:
            raise ValueError(f"地址分割處理失敗：{str(e)}")