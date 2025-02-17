project_root/
│
├── data/                          # 資料目錄
│   ├── raw/                      # 原始資料
│   ├── interim/                  # 中間處理資料
│   ├── processed/                # 已處理資料
│   └── external/                 # 外部資料
│
├── data_pipeline/                 # 資料處理管線
│   ├── data_ingestion/           # 資料擷取
│   ├── data_preprocessing/        # 資料前處理
│   └── data_enrichment/          # 資料充實
│       ├── exploration_phase/     # 探索階段
│       ├── statistical_phase/     # 統計階段
│       ├── interpretation_phase/  # 解釋階段
│       ├── visualization_phase/   # 視覺化階段
│       └── synthesis_phase/       # 綜合階段
│
├── notebooks/                     # Jupyter notebooks
│   ├── exploration/              # 探索筆記本
│   ├── analysis/                 # 分析筆記本
│   └── reports/                  # 報告筆記本
│
├── src/                          # 源代碼
│   ├── data/                     # 資料處理模組
│   ├── features/                 # 特徵工程模組
│   ├── models/                   # 模型相關模組
│   └── visualization/            # 視覺化模組
│
├── scripts/                      # 執行腳本目錄
│   ├── run/                     # 主要執行檔
│   │   ├── pipeline_runner.py   # 管線執行器
│   │   ├── model_trainer.py     # 模型訓練
│   │   └── report_generator.py  # 報告生成
│   │
│   ├── tools/                   # 工具腳本
│   │   ├── data_checker.py     # 資料檢查
│   │   └── monitor.py          # 效能監控
│   │
│   └── jobs/                    # 排程作業
│       ├── daily_update.py      # 每日更新
│       └── weekly_report.py     # 週報生成
│
├── tests/                        # 測試代碼
│   ├── data/                     # 資料測試
│   ├── features/                 # 特徵測試
│   └── models/                   # 模型測試
│
├── docs/                         # 文檔
│   ├── data/                     # 資料文檔
│   ├── analysis/                 # 分析文檔
│   └── reports/                  # 報告文檔
│
├── configs/                      # 配置文件
│   ├── data_configs/             # 資料配置
│   └── model_configs/            # 模型配置
│
├── requirements.txt              # 專案依賴
├── setup.py                      # 安裝腳本
├── README.md                     # 專案說明
└── .gitignore                   # Git忽略文件