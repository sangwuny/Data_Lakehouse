# 새 채팅용 전체 인수인계 및 대화 맥락

이 문서는 새로운 Codex 채팅이 현재 프로젝트와 지금까지의 대화를 최대한 손실 없이 이어가기 위한 단일 상세 인수인계 문서다. 짧은 개요가 아니라, 프로젝트의 현재 파일 상태, 구현된 변경, 폐기된 접근, 판단이 바뀐 이유, 실행 결과, 통계적 논의, 문서 정책, 논문 읽기 상태, Windows 작업 환경 문제와 아직 남은 작업을 함께 기록한다.

새 채팅의 assistant는 이 파일을 중간에서 멈추지 말고 끝까지 읽은 뒤 작업해야 한다. 이 문서와 실제 코드가 다를 경우에는 실제 코드와 현재 Git 상태를 다시 확인하고, 차이를 사용자에게 명시해야 한다. 이 문서는 2026-08-05 기준 상태를 기록한다.

## 1. 가장 먼저 지켜야 하는 사용자 결정

다음 결정은 현재 프로젝트의 명시적인 운영 원칙이다.

1. 실제 원본 저장소는 `C:\Users\mike1\OneDrive\Desktop\Data_Lakehouse`다.
2. 과거 임시 경로였던 `C:\Users\note\Desktop\데이터레이크하우스`는 사용하지 않는다. 2026-08-05 확인 결과 이 임시 경로는 존재하지 않았다.
3. 문서는 사용자가 명시적으로 수정 또는 작성해 달라고 요청했을 때만 변경한다. 일반 코드 작업 중에 문서를 자동으로 고치지 않는다.
4. `03b`는 순수한 lagged Pearson correlation 분석이다. 탐색적 데이터 분석, dashboard ranking, 시차 공행성 확인을 위한 것이며 causal candidate screening이 아니다.
5. 상관분석 결과를 인과추론의 변수 포함 또는 제외 기준으로 사용하지 않는다.
6. 상관관계가 약하거나 없다는 이유로 인과관계가 없다고 결론 내리지 않는다. 반대로 상관관계가 강하다는 이유로 인과관계를 주장하지 않는다.
7. 이전 Gold `04` 인과추론 구현은 활성 설계에서 삭제했다. 현재 수집한 관측 변수만으로 비관측 교란이 없다는 가정을 정당화할 수 없기 때문이다.
8. 향후 인과추론은 알고리즘부터 고르는 방식이 아니라 연구 질문, 처치 또는 노출, 결과, 시간 척도, 조정 집합, 잠재적 비관측 교란, 식별 가정, 민감도 분석, 위약 또는 반증 검사를 먼저 명시한 뒤 시작한다.
9. 논문을 읽었다고 말하려면 본문 text뿐 아니라 수식, 표, 그림, algorithm을 실제로 확인해야 한다. text extraction이나 OCR만 수행한 상태를 시각적으로 완독했다고 표현하지 않는다.
10. 현재 작업 트리는 dirty 상태다. 사용자 또는 이전 작업의 변경을 임의로 되돌리지 않는다.

## 2. 프로젝트의 목적과 현재 범위

이 저장소는 FRED와 ALFRED 경제 시계열을 Databricks Lakehouse의 Medallion 구조로 적재하고, 시점 일관성을 보존한 분석용 feature mart를 만드는 프로젝트다.

현재 활성 흐름은 다음과 같다.

```text
FRED/ALFRED API + 외부 bootstrap source
  -> Bronze 원천형 데이터와 수집 lineage
  -> Silver 정제, typing, quality tag, revision/current-only 의미 분리
  -> Gold as-of observation, period feature, transformed feature, snapshot
  -> 선택적 03b lagged correlation EDA/ranking
```

현재 활성 범위에는 causal discovery 또는 causal effect estimation이 포함되지 않는다. Gold는 point-in-time feature engineering과 exploratory association analysis까지 담당한다.

## 3. 저장소와 환경의 기준 경로

### 3.1 실제 원본 경로

```text
C:\Users\mike1\OneDrive\Desktop\Data_Lakehouse
```

모든 읽기, 쓰기, Git 확인, 새 파일 링크는 이 경로를 기준으로 해야 한다. 문서와 Databricks notebook에서는 가능하면 repository-relative path를 사용한다.

### 3.2 현재 Codex 세션 경로 문제

이전 Codex 세션의 기본 cwd가 다음처럼 잘못 고정되어 있었다.

```text
C:\Users\note\Desktop\데이터레이크하우스
```

사용자의 실제 Windows 계정과 원본 저장소는 `mike1` 경로에 있었다. Codex cwd의 `note`는 사용자의 실제 사용자 이름을 의미하는 것이 아니며, Windows 사용자 이름을 `note`로 바꾼다고 해결되는 문제도 아니다. 세션 시작 시 주입된 workspace root가 잘못된 것이 핵심이었다.

새 세션을 시작하기 어려워 다음 junction workaround도 시도했다.

```powershell
New-Item -ItemType Directory -Force -Path "C:\Users\note\Desktop"
cmd /c mklink /J "C:\Users\note\Desktop\데이터레이크하우스" "C:\Users\mike1\OneDrive\Desktop\Data_Lakehouse"
```

사용자는 폴더 복사도 시도하고 관리자 PowerShell에서 junction 명령도 실행했지만, image tool의 DPAPI 문제는 해결되지 않았다. 이후 복사본임을 확인하고 임시 `C:\Users\note`를 제거해 원래 경로로 돌아가기로 결정했다. 2026-08-05 점검 결과 원본 경로는 존재하고 임시 경로는 존재하지 않았다.

새 채팅에서 먼저 확인할 명령은 다음과 같다.

```powershell
Get-Location
Test-Path -LiteralPath "C:\Users\mike1\OneDrive\Desktop\Data_Lakehouse"
Test-Path -LiteralPath "C:\Users\note\Desktop\데이터레이크하우스"
```

### 3.3 Windows sandbox와 DPAPI 문제

이전 세션에서 local image를 `view_image`로 열 때 다음 오류가 발생했다.

```text
windows sandbox failed: CryptUnprotectData failed: 2148073483
```

이 오류는 PNG 또는 PDF 파일 자체의 손상으로 확인된 것이 아니라 Codex Windows sandbox helper와 DPAPI context의 문제로 추정됐다. 원본 저장소를 명시한 escalated PowerShell 명령은 파일을 읽고 쓸 수 있었지만, `view_image`는 실패했다.

현재 세션도 environment context의 cwd가 삭제된 `C:\Users\note\...`로 표시될 수 있다. 이 경우 원본 저장소 작업은 `C:\Users\mike1\OneDrive\Desktop\Data_Lakehouse`를 명시적 `workdir`로 사용해야 한다. 새 채팅에서는 원본 폴더 자체를 workspace로 다시 열었는지 먼저 확인한다.

## 4. 현재 repository file map

2026-08-05 확인된 주요 파일은 다음과 같다.

```text
README.md
학연생.png

docs/
  bronze_layer.md
  silver_layer.md
  gold_layer.md
  gold_lagged_correlation_analysis.md
  fred_series_metadata_config.md
  causality_scope.md
  new_chat_handoff.md

notebooks/databricks/configs/
  fred_seed_series.json
  build_fred_series_metadata.py
  fred_series_metadata.json                 generated, 현재 local에는 없음

notebooks/databricks/macrotrends/
  BAMLH0A0HYM2_chart_20260702T065042.csv

notebooks/databricks/bronze/
  01a_bronze_fred_bootstrap_versions.py
  01b_bronze_fred_incremental_versions.py
  01c_bronze_fred_current_observations.py
  01d_bronze_macrotrends_observations.py
  01e_bronze_alfred_reproducibility_audit.py
  01f_bronze_yahoo_finance_observations.py

notebooks/databricks/silver/
  02a_silver_fred_bootstrap_versions.py
  02b_silver_fred_incremental_versions.py
  02c_silver_fred_current_observations.py

notebooks/databricks/gold/
  03_gold_fred_asof_features.py
  03b_gold_lagged_correlation_analysis.py

논문/
  여러 causal inference와 causal discovery PDF

tmp/
  Pearl 2010 PDF rendering/OCR 실험 산출물
```

`학연생.png`는 저장소 root에 존재하지만 현재 대화에서 용도는 확정하지 않았다. 추측하여 삭제하거나 이동하지 않는다.

## 5. 현재 Git working tree

2026-08-05 실제 원본 저장소에서 확인한 `git status --short`는 다음과 같다.

```text
 M README.md
 M docs/bronze_layer.md
 M docs/gold_layer.md
 M docs/silver_layer.md
 M notebooks/databricks/bronze/01c_bronze_fred_current_observations.py
 M notebooks/databricks/bronze/01d_bronze_macrotrends_observations.py
 M notebooks/databricks/bronze/01f_bronze_yahoo_finance_observations.py
 M notebooks/databricks/configs/fred_seed_series.json
 M notebooks/databricks/gold/03_gold_fred_asof_features.py
 D notebooks/databricks/gold/03_gold_fred_asof_features_legacy_no_analysis_window.py
 M notebooks/databricks/silver/02c_silver_fred_current_observations.py
?? docs/causality_scope.md
?? docs/fred_series_metadata_config.md
?? docs/gold_lagged_correlation_analysis.md
?? docs/new_chat_handoff.md
?? notebooks/databricks/configs/build_fred_series_metadata.py
?? notebooks/databricks/gold/03b_gold_lagged_correlation_analysis.py
?? tmp/
?? 논문/
```

추적 중인 변경 통계는 대략 926 insertions, 1813 deletions였다. 삭제된 legacy notebook 하나가 1183줄을 차지한다. untracked `논문/`과 `tmp/`는 commit 여부가 결정되지 않았다. `tmp/`는 local rendering 산출물일 가능성이 높으므로 commit 전에 반드시 검토한다. `논문/` PDF도 repository에 포함할지 별도 결정이 필요하다.

현재 변경을 임의로 commit, reset, checkout 또는 clean하지 않는다. 특히 `git reset --hard`, `git checkout --`, `git clean`을 사용하지 않는다.

## 6. 문서 정책과 문서 상태

사용자는 문서가 코드 변경마다 자동으로 바뀌는 것이 토큰 낭비라고 명시했다. 따라서 향후 원칙은 다음과 같다.

```text
사용자가 문서 수정 또는 문서 작성을 명시적으로 요청한 경우에만 docs와 README를 변경한다.
```

이번 handoff 작성과 직전 전체 문서 최신화는 사용자가 명시적으로 요청했으므로 수행했다.

현재 문서 역할은 다음과 같다.

| 파일 | 역할 |
|---|---|
| `README.md` | 프로젝트 개요, active layout, 실행 순서, 문서 index |
| `docs/bronze_layer.md` | Bronze 적재, seed routing, 외부 bootstrap, metadata 전파, audit |
| `docs/silver_layer.md` | Silver 정제, quality, revision/current-only 의미, metadata 전파 |
| `docs/gold_layer.md` | Gold as-of mart, period/transformed features, snapshot, quality |
| `docs/gold_lagged_correlation_analysis.md` | 03b lagged Pearson correlation의 순수 EDA 의미와 제한 |
| `docs/fred_series_metadata_config.md` | FRED canonical metadata JSON 생성과 consumer |
| `docs/causality_scope.md` | 현재 causal inference를 활성 범위에서 제외한 이유와 향후 조건 |
| `docs/new_chat_handoff.md` | 새 채팅이 읽을 단일 전체 맥락 문서, 즉 현재 파일 |

직전 문서 작업에서는 README와 layer docs를 최신 구조에 맞췄고, 기존 `relationship candidate` 또는 `causal candidate` 용어가 active docs와 03/03b notebook에 남지 않았는지 `rg`로 확인했다. 관련 검색은 결과가 없었다. Markdown whitespace 검증도 통과했지만 Windows에서는 LF가 나중에 Git에 의해 CRLF로 바뀔 수 있다는 warning이 있었다.
## 7. Seed catalog와 tracked series

`notebooks/databricks/configs/fred_seed_series.json`은 사람이 관리하는 routing catalog다. 이 파일은 canonical FRED metadata의 저장소가 아니라 어떤 series를 수집하고 어떤 pipeline으로 보낼지 결정하는 catalog다.

현재 JSON은 UTF-8로 읽으면 정상 파싱된다. PowerShell에서 `-Encoding UTF8` 없이 출력하면 `role` 한글이 mojibake로 보일 수 있지만 2026-08-05 `ConvertFrom-Json` 검증은 통과했다. 출력 깨짐만 보고 파일을 임의로 재인코딩하거나 내용을 덮어쓰지 않는다.

현재 총 29개 series가 있다.

| rank | series_id | domain | expected_frequency | alfred_available | description |
|---:|---|---|---|---|---|
| 1 | GDPC1 | national_accounts | quarterly | true | Real Gross Domestic Product |
| 2 | PCE | income_consumption | monthly | true | Personal Consumption Expenditures |
| 3 | W875RX1 | income_consumption | monthly | false | Real Personal Income excluding Current Transfer Receipts |
| 4 | PAYEMS | labor | monthly | true | All Employees: Total Nonfarm Payrolls |
| 5 | UNRATE | labor | monthly | true | Unemployment Rate |
| 6 | ICSA | labor | weekly | true | Initial Claims |
| 7 | AWHMAN | labor | monthly | true | Average Weekly Hours: Manufacturing |
| 8 | INDPRO | production | monthly | true | Industrial Production Index |
| 9 | CMRMTSPL | production | monthly | false | Real Manufacturing and Trade Industries Sales |
| 10 | CPIAUCSL | inflation | monthly | true | Consumer Price Index: All Items |
| 11 | PCEPILFE | inflation | monthly | true | Core PCE Price Index |
| 12 | FEDFUNDS | rates | monthly | true | Effective Federal Funds Rate |
| 13 | GS10 | rates | monthly | true | 10-Year Treasury Constant Maturity Rate |
| 14 | T10YFFM | rates | monthly | false | 10-Year Treasury minus Federal Funds Rate |
| 15 | M2SL | money_credit | monthly | true | M2 Money Stock |
| 16 | BAMLH0A0HYM2 | credit | daily | false | ICE BofA US High Yield Index Option-Adjusted Spread |
| 17 | SP500 | financial_markets | daily | false | S&P 500 |
| 18 | PERMIT | housing | monthly | true | Building Permits: Total Units |
| 19 | NEWORDER | investment | monthly | true | New Orders: Nondefense Capital Goods ex. Aircraft |
| 20 | UMCSENT | sentiment | monthly | true | University of Michigan Consumer Sentiment |
| 21 | WALCL | liquidity | weekly | true | Federal Reserve total assets |
| 22 | BOGMBASE | liquidity | monthly | true | Monetary Base: Total |
| 23 | DGS10 | rates | daily | true | 10-Year Treasury market yield |
| 24 | T10Y2Y | rates | daily | true | 10-Year minus 2-Year Treasury spread |
| 25 | T10Y3M | rates | daily | true | 10-Year minus 3-Month Treasury spread |
| 26 | NASDAQCOM | financial_markets | daily | false | NASDAQ Composite |
| 27 | BOGZ1FL663067003Q | leverage | quarterly | true | Broker/dealer customer receivables, margin-loan proxy |
| 28 | NASDAQNQUSB40 | financial_markets | daily | true | Nasdaq US Benchmark Consumer Discretionary Index |
| 29 | NASDAQNQUSB45 | financial_markets | daily | true | Nasdaq US Benchmark Consumer Staples Index |

Routing grouping은 다음과 같다.

```text
ALFRED true:
GDPC1,PCE,PAYEMS,UNRATE,ICSA,AWHMAN,INDPRO,CPIAUCSL,PCEPILFE,
FEDFUNDS,GS10,M2SL,PERMIT,NEWORDER,UMCSENT,WALCL,BOGMBASE,DGS10,
T10Y2Y,T10Y3M,BOGZ1FL663067003Q,NASDAQNQUSB40,NASDAQNQUSB45

ALFRED false/current-only:
W875RX1,CMRMTSPL,T10YFFM,BAMLH0A0HYM2,SP500,NASDAQCOM
```

`alfred_available=true`는 01a/01b revision-aware path로 보내고, false는 01c current-only path로 보낸다. FRED current endpoint가 충분한 full history를 제공하지 않는 일부 current-only series는 01d Macrotrends 또는 01f Yahoo Finance로 initial bootstrap한 뒤 01c로 최신 값을 갱신한다.

## 8. Generated FRED metadata config

### 8.1 이 설계를 도입한 이유

사용자가 Bronze 01c, 01d, 01f가 원천 단위를 저장하지 않는 문제를 발견했다. FRED API를 직접 쓰는 01c는 `/fred/series`에서 units와 frequency를 얻을 수 있지만 Macrotrends와 Yahoo Finance는 FRED canonical units, frequency, seasonal adjustment를 제공하지 않는다.

Macrotrends와 Yahoo는 primary canonical provider가 아니라 FRED full-history 제약을 메우는 bootstrap 대체 source다. 따라서 외부 bootstrap value에도 동일 series의 canonical FRED metadata를 붙이는 것이 맞다는 결론을 내렸다.

사용자는 `fred_seed_series.json`에 generated metadata를 섞지 않고 별도 JSON으로 관리하기를 원했다. 그 결과 다음 파일이 추가됐다.

```text
notebooks/databricks/configs/build_fred_series_metadata.py
```

### 8.2 metadata builder 동작

입력과 출력은 다음과 같다.

```text
fred_seed_series.json
  + FRED API https://api.stlouisfed.org/fred/series
  -> fred_series_metadata.json
```

Databricks widget 기본값:

| widget | default |
|---|---|
| `secret_scope` | `fred-lakehouse` |
| `secret_key` | `fred_api_key` |
| `seed_catalog_path` | `fred_seed_series.json` |
| `output_path` | `fred_series_metadata.json` |
| `series_ids` | `ALL` |
| `sleep_seconds` | `0` |
| `retry_sleep_seconds` | `0.05,0.1,0.5` |

local 실행에서는 `FRED_API_KEY`, `SEED_CATALOG_PATH`, `OUTPUT_PATH`, `SERIES_IDS`, `SLEEP_SECONDS`, `RETRY_SLEEP_SECONDS` 환경변수를 사용할 수 있다. API key가 없으면 즉시 실패한다.

각 series에 대해 생성하는 metadata field는 다음과 같다.

```text
series_id
title
frequency
frequency_short
units
units_short
seasonal_adjustment
seasonal_adjustment_short
observation_start
observation_end
last_updated
popularity
notes
expected_frequency
domain
priority
alfred_available
seed_description
metadata_provider = fred_api
metadata_endpoint = https://api.stlouisfed.org/fred/series
```

출력 JSON top-level은 다음 구조다.

```json
{
  "generated_at_utc": "...",
  "metadata_provider": "fred_api",
  "metadata_endpoint": "https://api.stlouisfed.org/fred/series",
  "seed_catalog_path": "...",
  "series_count": 29,
  "series": {
    "GDPC1": { "series_id": "GDPC1", "units": "..." }
  }
}
```

모든 선택 series가 성공해야 파일을 쓴다. 하나라도 실패하면 실패 목록을 포함해 `RuntimeError`를 발생시키므로 partial generated file을 정상 결과처럼 남기지 않는다.

### 8.3 현재 generated file 상태

2026-08-05 local repository에는 다음 generated file이 존재하지 않았다.

```text
notebooks/databricks/configs/fred_series_metadata.json
```

따라서 01d 또는 01f를 새 환경에서 실행하기 전에 metadata builder를 실행하고, Databricks workspace에서 상대 경로가 실제 업로드 위치와 일치하는지 확인해야 한다. generated JSON을 Git에 포함할지 deployment artifact로만 관리할지는 아직 명시적으로 확정하지 않았다.

### 8.4 01a와 column 형식 일치에 관한 결론

사용자는 generated metadata를 사용하면 FRED vintage 적재인 01a와 column 형식을 일치시킬 수 있는지 물었다. 의미상 metadata vocabulary는 맞출 수 있다. 즉 `frequency`, `frequency_short`, `units`, `units_short`, `seasonal_adjustment`, `seasonal_adjustment_short`를 동일 이름으로 downstream에 전달할 수 있다.

그러나 01a revision-aware row와 01c/01d/01f current-only row의 시간 의미까지 동일해지는 것은 아니다.

```text
01a/01b:
  ALFRED revision-aware
  realtime_start, realtime_end, vintage_date, revision history가 실제 의미를 가짐

01c/01d/01f:
  current reconstructed history
  ALFRED vintage가 없고 is_point_in_time_safe=false로 유지해야 함
```

따라서 common observation metadata column은 통일하되 revision semantics는 Silver와 Gold에서 명시적으로 구분하는 현재 설계가 맞다.

## 9. Bronze layer 상세 상태

Bronze의 목적은 source-like data, request lineage, raw value, canonical metadata와 point-in-time reconstruction에 필요한 timing field를 보존하는 것이다.

기본 catalog/schema는 다음과 같다.

```text
catalog = fred_lakehouse
schema  = bronze
format  = Delta
```

주요 table:

| object | 역할 |
|---|---|
| `fred_raw_response_payloads` | raw FRED/ALFRED JSON payload |
| `fred_ingestion_runs` | request-level ingestion history |
| `fred_series_metadata_versions` | versioned loader가 수집한 metadata version |
| `fred_observation_versions` | ALFRED revision-aware observation version |
| `fred_vintage_dates_seen` | 확인한 ALFRED vintage date |
| `fred_incremental_watermarks` | 01b incremental watermark |
| `fred_current_observations_raw` | 01c FRED current + 01d Macrotrends + 01f Yahoo current-only row |
| `fred_run_summary` | Bronze run summary |

### 9.1 `01a_bronze_fred_bootstrap_versions.py`

역할은 ALFRED-capable series의 초기 full revision-history bootstrap이다.

widget 기본값:

```text
catalog=fred_lakehouse
secret_scope=fred-lakehouse
secret_key=fred_api_key
seed_catalog_path=../configs/fred_seed_series.json
series_ids=ALL
include_vintages=true
sleep_seconds=0
retry_sleep_seconds=0.01,0.05,0.1
observation_start=blank
observation_end=blank
observation_chunk_years=5
vintage_chunk_size=1500
realtime_start=1776-07-04
realtime_end=9999-12-31
output_type=1
LOAD_TYPE=bootstrap
```

full response가 너무 크거나 API limit 문제가 생길 때 observation year chunk와 vintage chunk fallback을 사용한다. seed에서 `alfred_available=true`인 series가 대상이다.

### 9.2 `01b_bronze_fred_incremental_versions.py`

역할은 01a 이후 ALFRED incremental refresh다. vintage date와 watermark를 사용하여 변경 가능성이 있는 realtime window를 다시 읽는다.

widget 기본값:

```text
catalog=fred_lakehouse
secret_scope=fred-lakehouse
secret_key=fred_api_key
seed_catalog_path=../configs/fred_seed_series.json
series_ids=ALL
sleep_seconds=0.5
observation_start=blank
observation_end=blank
realtime_start=blank override
realtime_end=blank override
lookback_days=30
overlap_days=3
output_type=1
LOAD_TYPE=incremental
```

### 9.3 `01c_bronze_fred_current_observations.py`

역할은 `alfred_available=false` series의 FRED current-only observation을 적재하고 최신 값을 갱신하는 것이다.

사용자가 직접 확인해 둔 중요한 변경:

```python
dbutils.widgets.text("observation_start", "", "Observation start")
OBSERVATION_START = dbutils.widgets.get("observation_start").strip() or "1776-07-04"
```

widget을 공란으로 두면 2010-01-01이 아니라 1776-07-04를 사용해 FRED가 제공하는 가능한 전체 history를 요청한다. 사용자는 처음에 변수명을 `REALTIME_START`로 바꿨다고 표현했지만, 최종 실제 코드는 `OBSERVATION_START`를 유지한 채 fallback 값만 1776-07-04로 바꾼 상태다. 이 최종 실제 코드를 기준으로 한다.

01c는 observation endpoint뿐 아니라 `/fred/series` metadata endpoint도 호출한다. 다음 common metadata를 `fred_current_observations_raw`에 저장하도록 변경됐다.

```text
domain
priority
expected_frequency
period_start_inferred
period_end_inferred
period_inference_basis
data_provider=fred_api
title
frequency
frequency_short
units
units_short
seasonal_adjustment
seasonal_adjustment_short
source_metadata_json
```

period inference는 FRED `frequency_short`를 기준으로 daily, monthly, quarterly, annual calendar bound를 계산한다. 알 수 없는 frequency는 observation date 자체를 start/end로 사용한다.

기존 Delta table에 새 column이 없어도 `ALTER TABLE ADD COLUMNS`로 schema evolution을 수행한다. merge dedupe key는 다음과 같다.

```text
source, series_id, observation_date
```

value뿐 아니라 metadata field가 달라졌을 때도 update predicate가 참이 되도록 변경됐다.

### 9.4 `01d_bronze_macrotrends_observations.py`

역할은 수동 다운로드한 Macrotrends CSV를 current-only bootstrap history로 적재하는 것이다. 현재 파일과 대상은 다음과 같다.

```text
notebooks/databricks/macrotrends/BAMLH0A0HYM2_chart_20260702T065042.csv
series_id=BAMLH0A0HYM2
```

주요 widget:

```text
source_data_path=../macrotrends
file_glob=*_chart_*.csv
series_ids=BAMLH0A0HYM2
date_formats=%m/%d/%Y,%Y-%m-%d
series_metadata_path=../configs/fred_series_metadata.json
series_units_json={}
LOAD_TYPE=macrotrends_full_history_observations
```

01d는 generated metadata JSON을 기본 metadata source로 사용한다. `series_units_json`은 primary source가 아니라 명시적 override다. metadata가 없으면 builder를 먼저 실행하라는 오류를 낸다.

Macrotrends row의 `source`는 downstream compatibility를 위해 `fred`로 유지하지만 실제 provider는 `data_provider=macrotrends`로 보존한다. CSV file hash, file size, modified time, source path 등도 lineage에 포함한다. row는 ALFRED vintage가 아니라 current reconstructed history다.

### 9.5 `01e_bronze_alfred_reproducibility_audit.py`

역할은 내부 `fred_observation_versions` as-of reconstruction을 live ALFRED vintage response와 비교하는 interactive audit다.

주요 widget:

```text
series_ids=GDPC1
as_of_dates=blank, UTC today
observation_start widget display default=2010-01-01
OBSERVATION_START fallback code=1776-07-04
observation_end=each as_of_date
max_observations_per_series=0, all
sleep_seconds=0.1
fail_on_mismatch=false
```

별도 audit Delta table을 지속적으로 만드는 것이 아니라 선택한 series와 as-of date에 대한 match/mismatch를 표시한다. 이 audit은 ALFRED와 내부 reconstruction의 재현성을 확인하는 것이며 BEA, BLS 등 원 발행기관과의 cross-source validation을 대신하지 않는다.

### 9.6 `01f_bronze_yahoo_finance_observations.py`

역할은 Yahoo Finance chart API의 장기 history를 current-only bootstrap으로 적재하는 것이다. 현재 기본 mapping은 다음과 같다.

```json
{"SP500":"^GSPC"}
```

주요 widget:

```text
series_ids=SP500
observation_start=1900-01-01
observation_end=blank, tomorrow UTC
interval=1d
price_field=close
retry_sleep_seconds=1,2,5
series_metadata_path=../configs/fred_series_metadata.json
series_units_json={}
LOAD_TYPE=yahoo_finance_full_history_observations
```

Yahoo response의 price와 timestamp를 사용하지만 canonical `units`, `frequency`, `seasonal_adjustment`, domain, priority는 generated FRED metadata JSON에서 가져온다. Yahoo response metadata는 supplemental lineage로 `source_metadata_json`에 저장한다. `source=fred`, `data_provider=yahoo_finance` 구분은 01d와 동일하다.

## 10. Bronze 변경의 핵심 의미

단위 metadata를 붙였다고 raw numeric value의 scale을 변환한 것은 아니다. 예를 들어 M2SL은 Billions of Dollars 값 그대로, FEDFUNDS는 Percent 값 그대로 저장된다. metadata는 값의 의미를 보존하고 downstream transform의 해석을 가능하게 한다.

단위가 다르다는 사실 자체는 ADF/KPSS 같은 개별 series 정상성 검정의 장애물이 아니다. 상수 배율로 scale이 달라도 일반적으로 검정 통계의 본질은 바뀌지 않는다. 문제는 단위 통일이 아니라 series의 추세, stochastic trend, seasonality, structural break와 변환 해석이다. 이 통계적 논의는 뒤에서 상세히 기록한다.
## 11. Silver layer 상세 상태

Silver는 Bronze raw-ish 데이터를 typed, quality-tagged, analytics-ready table로 바꾸되 revision-aware와 current-only 시간 의미를 섞지 않는다.

입력과 핵심 출력:

```text
bronze.fred_observation_versions
  -> silver.fred_observation_versions_cleaned

bronze.fred_current_observations_raw
  -> silver.fred_current_observations_cleaned
```

주요 table/view:

```text
fred_lakehouse.silver.fred_observation_versions_cleaned
fred_lakehouse.silver.fred_version_series_catalog
fred_lakehouse.silver.fred_version_quality_report
fred_lakehouse.silver.fred_version_lineage_events
fred_lakehouse.silver.fred_version_run_summary
fred_lakehouse.silver.fred_observations_asof_ready
fred_lakehouse.silver.fred_observations_current
fred_lakehouse.silver.fred_current_observations_cleaned
fred_lakehouse.silver.fred_current_series_catalog
fred_lakehouse.silver.fred_current_quality_report
fred_lakehouse.silver.fred_current_run_summary
fred_lakehouse.silver.fred_current_observations_analytics_ready
```

### 11.1 `02a_silver_fred_bootstrap_versions.py`

초기 ALFRED Bronze 전체를 정제한다.

```text
catalog=fred_lakehouse
bronze_schema=bronze
silver_schema=silver
series_ids=ALL
outlier_threshold=6.0
include_missing_in_silver=true
TRANSFORM_NAME=clean_fred_versioned_observations
TRANSFORM_VERSION=0.3.0
LOAD_TYPE=bootstrap
```

주요 결과 field:

```text
value_numeric
observation_date, period_start, period_end
available_at, realtime_start, realtime_end, vintage_date
revision_number, revision_count, is_current_version
is_point_in_time_usable
quality_status, quality_issues
is_outlier, outlier_score
Bronze/Silver lineage fields
```

### 11.2 `02b_silver_fred_incremental_versions.py`

01b가 만든 incremental ALFRED change를 정제한다. 02a가 target table을 먼저 만들었다고 가정한다.

```text
series_ids=ALL
bronze_run_id=blank, last Silver watermark 이후
outlier_threshold=6.0
include_missing_in_silver=true
TRANSFORM_NAME=clean_fred_versioned_observations
TRANSFORM_VERSION=0.3.0
LOAD_TYPE=incremental
```

### 11.3 `02c_silver_fred_current_observations.py`

01c, 01d, 01f가 함께 쓰는 `bronze.fred_current_observations_raw`를 정제한다.

```text
catalog=fred_lakehouse
bronze_schema=bronze
silver_schema=silver
seed_catalog_path=../configs/fred_seed_series.json
series_ids=ALL
outlier_threshold=6.0
include_missing_in_silver=true
TRANSFORM_NAME=clean_fred_current_observations
TRANSFORM_VERSION=0.3.0-current-common-observation-fields
LOAD_TYPE=current
```

이번 metadata 작업에서 02c는 다음처럼 변경됐다.

1. 오래된 Bronze table에 metadata column이 없으면 `ALTER TABLE ADD COLUMNS`로 보강한다.
2. Bronze의 `domain`, `priority`, `expected_frequency`를 우선 사용하고 null이면 seed catalog 값을 fallback한다.
3. Bronze의 inferred period bound가 있으면 그것을 우선 사용하고, 없으면 expected_frequency와 observation_date로 다시 계산한다.
4. `data_provider`, `title`, `frequency`, `frequency_short`, `units`, `units_short`, `seasonal_adjustment`, `seasonal_adjustment_short`, `source_metadata_json`을 cleaned table, series catalog, analytics-ready view까지 전달한다.
5. Silver target table에 source view의 새 column이 없으면 schema evolution을 수행한다.

current-only 의미는 다음과 같다.

```text
history_type=current_only
is_point_in_time_safe=false
is_current_value=true
```

이 row는 as-of date 이전 observation을 보여 줄 수는 있지만, 과거 시점에 실제로 알려졌던 revision을 재현하는 ALFRED row와 동일하지 않다. 이 차이는 Gold에서도 유지된다.

## 12. Gold `03_gold_fred_asof_features.py` 상세 상태

Gold 03은 현재 project의 중심 feature mart다.

```text
TRANSFORM_NAME=build_fred_gold_asof_feature_mart
TRANSFORM_VERSION=1.0.1-asof-integrated-source-units
LOAD_TYPE=as_of
```

### 12.1 widget

| widget | default | 의미 |
|---|---|---|
| `catalog` | `fred_lakehouse` | Unity Catalog |
| `silver_schema` | `silver` | Silver input schema |
| `gold_schema` | `gold` | Gold output schema |
| `as_of_date` | blank | UTC today |
| `analysis_start_date` | blank | earliest available |
| `analysis_end_date` | blank | as_of_date |
| `series_ids` | `ALL` | 처리할 series |
| `target_frequency` | `monthly` | daily, weekly, monthly, quarterly, annual, native |
| `aggregation_method` | `last` | last 또는 mean |
| `include_quality_warnings` | `true` | Silver warning row 포함 |
| `optimize_tables` | `true` | 가능하면 OPTIMIZE ZORDER |

이전 03 코드에 있던 `candidate_series_ids`, `target_series_id`, `relationship_transform_type`, `max_lag_periods`, `min_pair_count`, `min_candidate_score` widget은 제거했다. 03은 더 이상 relationship candidate score를 계산하지 않는다.

### 12.2 input

```text
silver.fred_observation_versions_cleaned
silver.fred_current_observations_cleaned
```

Macrotrends와 Yahoo row는 별도 Gold branch가 아니라 current-only Silver table을 통해 들어온다.

### 12.3 as-of selection

ALFRED revision-aware row는 다음 조건을 모두 만족해야 한다.

```text
is_point_in_time_usable
quality predicate 통과
value_numeric IS NOT NULL
observation_date <= as_of_date
realtime_start <= as_of_date
realtime_end >= as_of_date
coalesce(available_at, realtime_start) <= as_of_date
```

동일 `series_id`, `observation_date`에 여러 version이 보이면 `realtime_start DESC`, `available_at DESC`, `observation_version_id DESC` 순서로 최신 visible version 하나를 선택한다.

current-only row는 ALFRED realtime window가 없으므로 다음 조건으로 포함한다.

```text
quality predicate 통과
value_numeric IS NOT NULL
observation_date <= as_of_date
```

Gold에서 다음 metadata를 붙인다.

```text
history_type=current_only
availability_basis=observation_date
is_revision_aware=false
available_at=observation_date
vintage_date=NULL
revision_number=NULL
revision_count=NULL
```

이번 변경으로 current-only row의 `frequency`, `frequency_short`, `units`, `units_short`, `seasonal_adjustment`가 더 이상 강제 null이 아니며 Silver metadata를 그대로 받는다.

### 12.4 Gold output object

| object | type | 역할 |
|---|---|---|
| `fred_asof_observations` | Delta table | as_of_date에 보이는 observation row |
| `fred_period_features_long` | Delta table | target frequency로 집계한 period value |
| `fred_transformed_features_long` | Delta table | series/period/transform long-form feature |
| `fred_series_feature_snapshot` | Delta table | series별 latest period snapshot |
| `fred_latest_feature_snapshot` | view | window/frequency/aggregation별 latest as-of snapshot |
| `fred_gold_quality_report` | Delta table | Gold quality rule result |
| `fred_gold_run_summary` | Delta table | run count와 lineage range |

### 12.5 merge key

```text
fred_asof_observations:
  as_of_date, series_id, observation_date

fred_period_features_long:
  as_of_date, target_frequency, aggregation_method, series_id, period_start

fred_transformed_features_long:
  as_of_date, target_frequency, aggregation_method, series_id, period_start, transform_type

fred_series_feature_snapshot:
  as_of_date, target_frequency, aggregation_method, series_id
```

코드는 stage view schema에 새 column이 있으면 target table에 추가하고 Delta MERGE를 수행한다. 각 실행 범위의 기존 row를 먼저 delete한 뒤 merge하여 rerun idempotency를 확보한다.

### 12.6 target frequency와 period boundary

지원 frequency:

```text
daily
weekly
monthly
quarterly
annual
native
```

weekly 지원은 최근 추가됐다. week start는 Monday, end는 Sunday로 계산한다.

```text
weekly period_start = Monday
weekly period_end   = Sunday
```

`native`는 Silver의 `period_start`, `period_end`를 우선 사용한다. 다른 target frequency는 observation_date를 target calendar period로 다시 매핑한다.

`aggregation_method=last`는 period 안 최신 observation을 사용하고, `mean`은 period 평균을 사용한다. period table은 mean/min/max/count와 quality/revision metadata도 함께 저장한다.

### 12.7 analysis window

`analysis_start_date`가 blank면 earliest available period이고, `analysis_end_date`가 blank면 as_of_date다. end는 as_of보다 클 수 없다.

이 window는 단순 display filter가 아니라 feature identity의 일부다. 다음 full-window transform에도 영향을 준다.

```text
z_score_full_sample
index_base100
```

서로 시작일이 크게 다른 series를 비교하려면 공통 analysis_start_date를 명시하는 것이 해석에 유리하다.

### 12.8 transformed feature

`fred_transformed_features_long`은 다음 9개 transform을 만든다.

| transform_type | 계산 | transformed_unit |
|---|---|---|
| `raw` | selected period value | source unit |
| `change_1` | current minus lag 1 | source unit, Percent series면 percentage_point |
| `change_12` | current minus lag 12 | source unit, Percent series면 percentage_point |
| `pct_change_1` | `100 * (current / lag1 - 1)` | percent |
| `pct_change_12` | `100 * (current / lag12 - 1)` | percent |
| `log_diff_1` | `100 * (ln current - ln lag1)` | log_percent |
| `log_diff_12` | `100 * (ln current - ln lag12)` | log_percent |
| `z_score_full_sample` | `(x - full-window mean) / full-window stddev` | standard_deviation |
| `index_base100` | `100 * x / first-window value` | index_base100 |

`log_diff_*`는 current와 lag value가 양수일 때만 가능하다. `pct_change_*`는 lag가 0이면 null이다. stage는 `transformed_value IS NOT NULL` row만 output한다.

중요한 세부사항:

1. `change_12`와 `pct_change_12`의 12는 12 calendar months가 아니라 정렬된 12개 period row다. quarterly target이면 12 quarters, annual target이면 12 years다.
2. transformed table의 `pct_change_1`은 100을 곱한 percent다.
3. `fred_series_feature_snapshot`의 `pct_change_1`은 별도 계산이며 `current / lag1 - 1` 비율이다. 같은 이름처럼 보여도 scale이 100배 다르므로 downstream에서 혼동하면 안 된다.
4. `z_score_full_sample`은 scale을 맞추지만 stationarity를 보장하지 않는다.
5. `index_base100`도 기준점 재표현일 뿐 stationarity를 보장하지 않는다.

### 12.9 source units의 Gold 전파

Gold 03 연결을 점검한 결론은 다음과 같다.

```text
01c/01d/01f metadata
  -> bronze.fred_current_observations_raw
  -> 02c silver.fred_current_observations_cleaned
  -> 03 gold_asof_observations
  -> fred_period_features_long
  -> fred_transformed_features_long
  -> fred_series_feature_snapshot
```

revision-aware ALFRED row는 01a/01b와 02a/02b의 metadata를 사용한다. current-only row는 01c 또는 bootstrap metadata를 사용한다. Gold code는 두 input column shape를 맞춰 UNION ALL하고 common output을 만든다.

### 12.10 quality report

현재 Gold quality rule:

```text
asof_not_before_observation
visible_inside_realtime_window
period_feature_value_not_null
```

current-only row에는 synthetic/placeholder realtime field가 있을 수 있으므로 quality rule을 해석할 때 history_type을 같이 본다.

### 12.11 Feature Store shape

`fred_series_feature_snapshot`은 `series_id`, `as_of_date` 중심의 time-series feature table 형태다. code에는 Unity Catalog primary key 등록 예시가 있지만 실제 constraint 실행은 자동으로 하지 않는다.

## 13. Gold `03b_gold_lagged_correlation_analysis.py` 상세 상태

이 파일은 이전 03 내부의 relationship scoring을 분리하고 의미에 맞게 이름을 바꾼 새 untracked notebook이다.

```text
TRANSFORM_NAME=build_fred_gold_lagged_correlation_analysis
TRANSFORM_VERSION=0.2.0-lagged-correlation-analysis
LOAD_TYPE=lagged_correlation_analysis
CORRELATION_SCORE_METHOD=r_squared_sample_reliability
```

### 13.1 input과 output

```text
input:
  gold.fred_transformed_features_long

outputs:
  gold.fred_lagged_correlation_scores
  gold.fred_top_lagged_correlations
  gold.fred_lagged_correlation_run_summary
```

### 13.2 widget

| widget | default |
|---|---|
| `target_series_id` | `GDPC1` |
| `comparison_series_ids` | `ALL` |
| `target_frequency` | `monthly` |
| `aggregation_method` | `last` |
| `correlation_transform_type` | `raw` |
| `max_lag_periods` | `12` |
| `min_pair_count` | `AUTO` |
| `reliable_pair_count` | `AUTO` |
| `min_correlation_score` | `0.3` |
| `optimize_tables` | `true` |

frequency별 AUTO threshold:

| frequency | min pair | reliable pair |
|---|---:|---:|
| daily | 252 | 756 |
| weekly | 104 | 260 |
| monthly | 36 | 60 |
| quarterly | 24 | 40 |
| annual | 15 | 25 |
| native | 36 | 60 |

### 13.3 lag alignment

각 target period `t`와 comparison period `t-lag`를 날짜 key로 join한다.

```text
daily: target date - lag days
weekly: target date - 7 * lag days
monthly: add_months(target, -lag)
quarterly: add_months(target, -3 * lag)
annual: add_months(target, -12 * lag)
native: add_months(target, -lag)
```

lag 0도 포함한다. target 자신은 comparison에서 제외한다. null transformed value는 source 단계에서 제외한다.

### 13.4 score

```text
pearson_corr = corr(target_value, comparison_value)
r_squared = pearson_corr ^ 2
coverage_rate = pair_count / target_period_count
sample_reliability_score = min(1, pair_count / reliable_pair_count)
coverage_weighted_score = r_squared * coverage_rate
correlation_score = r_squared * sample_reliability_score
```

`pair_count < min_pair_count`는 score stage에서 제외한다. rank는 target/run partition 안에서 correlation_score descending으로 계산한다. top view는 `correlation_score >= min_correlation_score`인 최신 row를 노출한다.

### 13.5 절대 해서는 안 되는 해석

```text
높은 score -> 원인
낮은 score -> 인과관계 없음
03b 순위 -> causal variable selection
03b 결과 -> confounding을 통제한 관계
```

03b는 다른 변수를 조건화하지 않고 forecasting improvement도 검정하지 않는 Pearson association 분석이다. nonstationary raw level, common trend, regime shift, shared measurement, common cause 때문에 높은 score가 생길 수 있다.

현재 구현은 모든 선택 series에 하나의 `correlation_transform_type`을 공통 적용한다. series별 최적 변환을 자동 선택하지 않는다. mixed macro series에서 `raw` default를 그대로 사용하는 것은 특히 위험하므로 실행자가 의도적으로 transform을 선택해야 한다.
## 14. 단위, 정상성, transform에 관한 전체 논의

이 section은 대화에서 반복해서 확인한 통계적 결론을 보존한다.

### 14.1 서로 다른 unit은 정상성 검정의 직접 문제인가

M2SL이 Billions of Dollars이고 FEDFUNDS가 Percent인 것처럼 원천 단위는 서로 다르다. 그러나 ADF, KPSS 같은 정상성 검정은 보통 각 series를 개별적으로 검정한다. 한 series를 일정한 상수로 곱해 unit scale만 바꿔도 정상성 여부 자체가 바뀌는 것은 아니다.

따라서 해결해야 할 문제는 모든 series를 동일 unit으로 바꾸는 것이 아니다. 다음을 해결해야 한다.

```text
trend 또는 stochastic trend가 있는가
seasonality가 있는가
structural break가 있는가
level, difference, growth 중 어떤 경제적 의미를 검정할 것인가
lag length와 deterministic term을 어떻게 선택할 것인가
sample period와 missingness가 결과에 어떤 영향을 주는가
```

standardization은 숫자 scale을 맞출 수 있지만 nonstationary stochastic process를 stationary하게 만들지는 않는다.

### 14.2 정상성 검정 시 주의사항

1. ADF 하나만으로 결정하지 않는다. ADF는 unit root가 null이고 KPSS는 stationarity가 null이므로 가능하면 함께 본다.
2. constant 또는 trend deterministic specification을 경제적 series 특성에 맞춘다.
3. lag length를 너무 짧게 두면 residual autocorrelation, 너무 길게 두면 power 저하가 생긴다.
4. monthly/quarterly seasonality를 점검한다. 단순 1차 차분과 seasonal difference는 다른 의미다.
5. 2008 crisis, COVID-19, policy regime change 같은 structural break는 standard unit-root test를 왜곡할 수 있다. break test 또는 subperiod robustness를 고려한다.
6. 표본이 짧으면 test power가 약하다. 특히 annual/quarterly series는 더욱 주의한다.
7. transformation 후 missing row와 시작 lag loss를 확인한다.
8. log transform은 양수 series에만 가능하다. spread나 rate가 0 이하가 될 수 있으면 log difference를 그대로 적용할 수 없다.
9. percentage point change와 percent change를 구분한다. FEDFUNDS 5%에서 6%는 +1 percentage point이며 +20 percent relative change다.
10. 정상성은 transformation 이름만 보고 가정하지 않고 실제 transformed series를 다시 검정한다.
11. 정상성 결과는 sample window에 의존한다. as_of date와 analysis_start/end가 바뀌면 결과가 바뀔 수 있다.
12. FRED revision-aware as-of series와 current reconstructed series는 과거 값의 의미가 다르므로 결과 해석에서 history_type을 확인한다.

### 14.3 unit만으로 transform을 고정할 수 있는가

unit은 좋은 초기 heuristic이지만 최종 transform은 아니다. 같은 unit이어도 경제적 의미가 다르면 transform이 달라진다.

예:

```text
FEDFUNDS, UNRATE, BAMLH0A0HYM2는 모두 Percent 계열이지만
FEDFUNDS는 policy rate level/change,
UNRATE는 labor slack level/change,
BAMLH0A0HYM2는 credit spread level/change 의미가 다르다.
```

또한 같은 Billions of Dollars라도 M2SL은 장기 추세가 강한 money stock이고 WALCL은 policy intervention과 급격한 regime shift가 있는 balance-sheet level이다. 단순히 unit이 같다고 동일 transform이 항상 최선은 아니다.

결론은 다음과 같다.

```text
series별 candidate transform을 지정하고,
실제 sample에서 정상성 검정과 해석 가능성을 비교한 뒤,
연구 질문에 맞는 transform을 결정하는 것이 바람직하다.
```

단, 현재 active 03b는 series별 transform mapping을 받지 않고 하나의 공통 transform을 사용한다. series별 변환 자동 선택 기능은 현재 구현되지 않았다.

### 14.4 현재 확인된 units

사용자가 Gold에서 확인한 units 표는 다음과 같았다.

| series_id | units | units_short |
|---|---|---|
| AWHMAN | Hours | Hours |
| BAMLH0A0HYM2 | Percent | % |
| BOGMBASE | Billions of Dollars | Bil. of $ |
| CMRMTSPL | Millions of Chained 2017 Dollars | Mil. of Chn. 2017 $ |
| FEDFUNDS | Percent | % |
| M2SL | Billions of Dollars | Bil. of $ |
| NASDAQCOM | Index Feb 5, 1971=100 | Index Feb 5, 1971=100 |
| NASDAQNQUSB45 | Index | Index |
| NEWORDER | Millions of Dollars | Mil. of $ |
| PAYEMS | Thousands of Persons | Thous. of Persons |
| PERMIT | Thousands of Units | Thous. of Units |
| SP500 | Index | Index |
| T10Y2Y | Percent | % |
| UMCSENT | Index 1966:Q1=100 | Index 1966:Q1=100 |
| UNRATE | Percent | % |
| W875RX1 | Billions of Chained 2017 Dollars | Bil. of Chn. 2017 $ |
| BOGZ1FL663067003Q | Millions of U.S. Dollars | Mil. of U.S. $ |
| CPIAUCSL | Index 1982-1984=100 | Index 1982-1984=100 |
| DGS10 | Percent | % |
| GDPC1 | Billions of Chained 2017 Dollars | Bil. of Chn. 2017 $ |
| GS10 | Percent | % |
| ICSA | Number | Number |
| INDPRO | Index 2017=100 | Index 2017=100 |
| NASDAQNQUSB40 | Index | Index |
| PCE | Billions of Dollars | Bil. of $ |
| PCEPILFE | Index 2017=100 | Index 2017=100 |
| T10Y3M | Percent | % |
| T10YFFM | Percent | % |
| WALCL | Millions of U.S. Dollars | Mil. of U.S. $ |

### 14.5 series별 candidate transform 제안

다음은 자동 규칙이 아니라 정상성 검정을 시작하기 위한 candidate set이다. 각 candidate를 실제 sample에서 검정해야 한다.

| series | 1차 candidate | 추가 candidate | 주의 |
|---|---|---|---|
| GDPC1 | `log_diff_1` | `pct_change_1`, quarterly growth에 맞춘 별도 연율화 | quarterly에서 `log_diff_12`는 12분기 |
| PCE | `log_diff_1` | `pct_change_1`, `log_diff_12` | nominal level trend |
| W875RX1 | `log_diff_1` | `pct_change_1`, `log_diff_12` | real income level trend |
| PAYEMS | `log_diff_1` | `change_1`, `pct_change_1` | employment count trend |
| UNRATE | `raw` | `change_1`, `change_12` | percent change는 해석이 어색할 수 있음 |
| ICSA | `log_diff_1` | `pct_change_1`, seasonal comparison | strictly positive 여부와 weekly seasonality 확인 |
| AWHMAN | `raw` | `change_1`, `change_12` | bounded hours level일 수 있음 |
| INDPRO | `log_diff_1` | `pct_change_1`, `log_diff_12` | index growth 해석 |
| CMRMTSPL | `log_diff_1` | `pct_change_1`, `log_diff_12` | real sales trend |
| CPIAUCSL | `log_diff_1` | `log_diff_12`, `pct_change_12` | inflation은 price index level이 아니라 growth |
| PCEPILFE | `log_diff_1` | `log_diff_12`, `pct_change_12` | core inflation 해석 |
| FEDFUNDS | `raw` | `change_1`, `change_12` | percentage point change 선호, zero 근처 pct change 주의 |
| GS10 | `raw` | `change_1` | rate level persistence 확인 |
| DGS10 | `raw` | `change_1` | daily rate, monthly aggregation 방식 영향 |
| T10Y2Y | `raw` | `change_1` | 음수 가능, log 불가 |
| T10Y3M | `raw` | `change_1` | 음수 가능, log 불가 |
| T10YFFM | `raw` | `change_1` | spread, 음수 가능 |
| M2SL | `log_diff_1` | `pct_change_1`, `log_diff_12` | 강한 level trend |
| BAMLH0A0HYM2 | `raw` | `change_1` | percent가 사실상 spread level, pct change 해석 주의 |
| SP500 | `log_diff_1` | `pct_change_1` | raw level correlation은 spurious trend 위험 |
| NASDAQCOM | `log_diff_1` | `pct_change_1` | market return 해석 |
| NASDAQNQUSB40 | `log_diff_1` | `pct_change_1` | sector index return |
| NASDAQNQUSB45 | `log_diff_1` | `pct_change_1` | sector index return |
| PERMIT | `log_diff_1` | `pct_change_1`, `change_12` | seasonality와 zero 여부 확인 |
| NEWORDER | `log_diff_1` | `pct_change_1`, `log_diff_12` | volatile orders |
| UMCSENT | `raw` | `change_1`, `change_12` | bounded sentiment index 특성 |
| WALCL | `log_diff_1` | `pct_change_1`, regime-specific `change_1` | policy regime/structural break 강함 |
| BOGMBASE | `log_diff_1` | `pct_change_1` | monetary regime break 확인 |
| BOGZ1FL663067003Q | `log_diff_1` | `pct_change_1`, `change_1` | quarterly, crisis break와 revision 확인 |

`raw`, `change`, growth 중 어느 것이 최종 선택인지는 outcome과 causal/forecasting question에 따라 달라진다.

### 14.6 서로 다른 transform을 사용한 Granger test가 가능한가

가능하다. VAR 또는 Granger test의 각 column이 반드시 동일 transform type일 필요는 없다. 예를 들어 다음과 같은 matrix를 만들 수 있다.

```text
M2SL       -> log_diff_1
FEDFUNDS   -> change_1
UNRATE     -> raw 또는 change_1
NASDAQCOM  -> log_diff_1
T10Y2Y     -> raw
```

조건은 다음과 같다.

1. 모두 같은 target frequency와 time index에 정렬돼야 한다.
2. 선택된 representation이 정상성 또는 model assumption을 충분히 만족해야 한다.
3. lag가 같은 calendar 의미를 가져야 한다.
4. coefficient와 causality statement를 transformed variable의 의미로 해석해야 한다.
5. cointegrated level series라면 무조건 difference VAR로만 처리하지 말고 VECM 가능성을 고려해야 한다.
6. mixed integration order, structural break, high-dimensional lag selection을 진단해야 한다.

과거 04 notebook은 하나의 `transform_type` widget을 전체 variable에 공통 적용하는 구조였다. 따라서 이론상 mixed transform Granger가 가능하더라도 당시 구현은 series별 transform mapping을 직접 지원하지 않았다. 이를 개선하려면 `series_id -> transform_type` mapping config와 join logic이 필요했지만, 04 전체가 방법론적 이유로 삭제되어 구현하지 않았다.

## 15. 과거 04 causal pipeline과 삭제 결정

이 section은 active 설계를 설명하는 것이 아니라, 어떤 실험을 했고 왜 폐기했는지 기록하기 위한 history다.

### 15.1 과거 흐름

대화 중 한때 Gold 다음 단계로 stationarity, Granger/conditional Granger, PCMCI/PCMCI+를 구성했다. 확인된 파일명은 다음과 같다.

```text
04b_gold_conditional_granger_var_tests.py
04c_gold_pcmci_causal_discovery.py
```

04a를 포함한 04 전체가 나중에 제거됐다. 04 파일들은 당시 Git HEAD에 추적된 상태가 아니었던 것으로 보이며 현재 active file tree에는 없다.

### 15.2 왜 삭제했는가

사용자의 최종 판단은 다음과 같다.

```text
현재 수집한 FRED/ALFRED/current-only 변수만으로
비관측 교란이 없다고 말할 수 없다.
따라서 그 가정에 기대는 causal discovery 결과를
인과관계로 제시하는 것은 정당화되지 않는다.
```

Granger causality는 forecasting precedence를 검정할 수 있지만 structural causality와 동일하지 않다. PCMCI도 관측 변수 집합, causal sufficiency 또는 관련 조건부 독립 가정, stationarity, faithfulness 등 가정의 영향을 받는다. 사용자와 프로젝트는 알고리즘 결과를 causal proof처럼 운영하지 않기로 했다.

### 15.3 correlation screening을 causal pipeline에서 제거한 이유

과거 03 또는 03b의 lagged correlation score를 04 candidate selection에 연결하려는 흐름이 있었다. 사용자는 다음 논리로 이를 중단했다.

```text
상관관계와 인과관계는 다른 개념이다.
상관관계가 없다고 인과관계가 없는 것은 아니다.
상관분석은 필요하지만 인과추론의 gate로 사용하면 안 된다.
```

따라서 correlation 분석 자체는 03b로 남기고, 이름과 table을 causal candidate가 아니라 lagged correlation으로 변경했다. 04를 삭제하고 active Gold flow를 03 -> optional 03b에서 끝내도록 했다.

## 16. 과거 PCMCI 정렬 방식과 실행 결과

### 16.1 당시 04c의 aligned sample 의미

당시 04c는 선택된 target, candidate, control series의 transformed value를 읽고 다음 방식으로 matrix를 만들었다.

```python
period_sets = [set(series_values[series_id]) for series_id in available_variable_ids]
common_periods = sorted(set.intersection(*period_sets))
```

그 다음 모든 matrix value가 finite인 row만 남겼다.

```python
finite_row_mask = np.all(np.isfinite(raw_matrix), axis=1)
aligned_periods = common_periods filtered by finite_row_mask
```

따라서 `Aligned sample: 2020-10-01 to 2026-05-01` 같은 출력은 요청한 analysis range 전체가 아니라 선택된 모든 retained variable이 동시에 non-null finite transformed value를 가진 공통 row의 첫 날짜와 마지막 날짜를 뜻한다.

이는 complete-case 교집합 분석이다. 하나의 늦게 시작한 series가 전체 시작일을 늦출 수 있고, 하나의 series가 최신 month를 아직 갖지 않으면 전체 끝일이 앞당겨질 수 있다. transform의 첫 lag row가 null이라 시작일이 추가로 밀릴 수도 있다.

중요하게도 출력된 start/end는 연속성을 보장하지 않는다. 중간 month가 하나라도 누락되면 그 row는 빠진다. 당시 Tigramite matrix에서 lag는 남은 row offset이므로, 빠진 calendar month가 있으면 lag 1이 실제 한 달보다 길어질 수 있는 잠재적 문제가 있었다.

### 16.2 사용자가 제공한 실제 04c 실행 로그

```text
Gold PCMCI target schema: fred_lakehouse.gold
Gold run_id: 20260708T073612Z
As-of date: 2026-07-07
Analysis range: NULL to 2026-07-07
Target series: NASDAQCOM
Candidate source mode: strong_granger
Candidate filter: SP500,T10Y2Y,ICSA,FEDFUNDS
Control series: DGS10,BAMLH0A0HYM2,M2SL,UNRATE,PCEPILFE
Frequency: monthly
Aggregation: last
Transform: pct_change_1
PCMCI method: pcmci
CI test: parcorr
tau_min=1
tau_max=6
pc_alpha=0.05
alpha_level=0.05
fdr=fdr_bh
max_candidates=5
min_observations=60
Selected candidates=4
Requested PCMCI variable count=10
PCMCI retained variables=10
Aligned observation count=350
Aligned sample=1997-01-01 to 2026-05-01
Dropped variables=NONE
PCMCI link rows produced=600
Significant link rows=16
Significant target link rows=0
```

10 variables x 10 destinations x 6 positive lags로 600 row가 만들어졌다. 전체 significant link는 16개였지만 NASDAQCOM을 destination으로 한 significant target link는 0개였다.

1997-01부터 2026-05까지 monthly calendar month를 단순 계산하면 350보다 약간 많을 수 있으므로 `aligned_observation_count=350`과 endpoint만 보고 완전 연속 monthly sample이라고 가정하면 안 된다. missing common periods 여부를 별도 점검해야 한다.

### 16.3 당시 04c 주요 설정

당시 default/available option에는 다음이 있었다.

```text
target_frequency=monthly
aggregation_method=last
transform_type=pct_change_1
candidate_source_mode=strong_granger|explicit|all
control_series_ids=DGS10,BAMLH0A0HYM2,M2SL,UNRATE,PCEPILFE
pcmci_method=pcmci|pcmci_plus
ci_test=parcorr|robust_parcorr
tau_min=1
tau_max=6
pc_alpha=0.05 or AUTO
alpha_level=0.05
fdr_method=fdr_bh|none
missing_policy=complete_case
```

monthly AUTO minimum observation은 60이었다. candidate는 04b `fred_strong_granger_candidates`에서 읽는 것이 default였다. 결과 table과 view는 과거에 다음 이름을 사용했다.

```text
fred_pcmci_causal_links
fred_pcmci_target_links
fred_pcmci_run_summary
```

현재 이 notebook과 output은 active design에 포함되지 않는다. Databricks에 과거 table이 남아 있다면 삭제 또는 archive 여부를 별도 결정해야 하며, handoff 문서 작성 과정에서는 실제 table을 삭제하지 않았다.
## 17. 현재 권장 실행 순서

### 17.1 initial build

```text
1. configs/build_fred_series_metadata.py
2. bronze/01a_bronze_fred_bootstrap_versions.py
3. bronze/01d_bronze_macrotrends_observations.py    필요한 BAMLH0A0HYM2 bootstrap
4. bronze/01f_bronze_yahoo_finance_observations.py 필요한 SP500 bootstrap
5. bronze/01c_bronze_fred_current_observations.py  current-only full/history refresh
6. bronze/01e_bronze_alfred_reproducibility_audit.py 선택적 점검
7. silver/02a_silver_fred_bootstrap_versions.py
8. silver/02c_silver_fred_current_observations.py
9. gold/03_gold_fred_asof_features.py
10. gold/03b_gold_lagged_correlation_analysis.py 선택적 EDA/ranking
```

01d와 01f는 bootstrap helper이므로 일반적인 daily refresh마다 반복하지 않는다.

### 17.2 refresh

```text
1. configs/build_fred_series_metadata.py            seed 또는 FRED metadata가 바뀐 경우
2. bronze/01b_bronze_fred_incremental_versions.py
3. bronze/01c_bronze_fred_current_observations.py
4. bronze/01e_bronze_alfred_reproducibility_audit.py 선택적 sample check
5. silver/02b_silver_fred_incremental_versions.py
6. silver/02c_silver_fred_current_observations.py
7. gold/03_gold_fred_asof_features.py
8. gold/03b_gold_lagged_correlation_analysis.py      ranking 갱신이 필요한 경우만
```

## 18. Bronze, Silver, Gold 확인 SQL

과거 사용자가 각 layer에서 metadata와 row coverage를 확인할 SQL을 요청했다. 다음 query는 현재 schema에 맞춘 점검용이다. 실행 전 `as_of_date`와 대상 series를 실제 값으로 바꾼다.

### 18.1 Bronze current/bootstrap metadata

```sql
SELECT
  series_id,
  data_provider,
  COUNT(*) AS row_count,
  MIN(TO_DATE(observation_date)) AS min_observation_date,
  MAX(TO_DATE(observation_date)) AS max_observation_date,
  COUNT_IF(units IS NULL OR TRIM(units) = '') AS missing_units,
  COUNT_IF(units_short IS NULL OR TRIM(units_short) = '') AS missing_units_short,
  COUNT_IF(frequency IS NULL OR TRIM(frequency) = '') AS missing_frequency,
  COUNT_IF(frequency_short IS NULL OR TRIM(frequency_short) = '') AS missing_frequency_short,
  FIRST(units, true) AS sample_units,
  FIRST(units_short, true) AS sample_units_short,
  FIRST(frequency, true) AS sample_frequency,
  FIRST(seasonal_adjustment, true) AS sample_seasonal_adjustment
FROM fred_lakehouse.bronze.fred_current_observations_raw
GROUP BY series_id, data_provider
ORDER BY series_id, data_provider;
```

provider별 duplicate key 확인:

```sql
SELECT source, series_id, observation_date, COUNT(*) AS duplicate_count
FROM fred_lakehouse.bronze.fred_current_observations_raw
GROUP BY source, series_id, observation_date
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, series_id, observation_date;
```

### 18.2 Silver current metadata

```sql
SELECT
  series_id,
  data_provider,
  history_type,
  is_point_in_time_safe,
  COUNT(*) AS row_count,
  MIN(observation_date) AS min_observation_date,
  MAX(observation_date) AS max_observation_date,
  COUNT_IF(value_numeric IS NULL) AS null_value_numeric,
  COUNT_IF(units IS NULL OR TRIM(units) = '') AS missing_units,
  COUNT_IF(frequency IS NULL OR TRIM(frequency) = '') AS missing_frequency,
  FIRST(units, true) AS sample_units,
  FIRST(units_short, true) AS sample_units_short,
  FIRST(frequency, true) AS sample_frequency,
  FIRST(frequency_short, true) AS sample_frequency_short,
  FIRST(seasonal_adjustment, true) AS sample_seasonal_adjustment
FROM fred_lakehouse.silver.fred_current_observations_cleaned
GROUP BY series_id, data_provider, history_type, is_point_in_time_safe
ORDER BY series_id, data_provider;
```

quality 확인:

```sql
SELECT series_id, quality_status, COUNT(*) AS row_count
FROM fred_lakehouse.silver.fred_current_observations_cleaned
GROUP BY series_id, quality_status
ORDER BY series_id, quality_status;
```

### 18.3 Gold unit propagation

```sql
SELECT
  series_id,
  source,
  history_type,
  source_frequency,
  source_frequency_short,
  units,
  units_short,
  seasonal_adjustment,
  COUNT(*) AS period_count,
  MIN(period_start) AS min_period_start,
  MAX(period_start) AS max_period_start
FROM fred_lakehouse.gold.fred_period_features_long
WHERE as_of_date = DATE '2026-07-07'
  AND target_frequency = 'monthly'
  AND aggregation_method = 'last'
GROUP BY
  series_id,
  source,
  history_type,
  source_frequency,
  source_frequency_short,
  units,
  units_short,
  seasonal_adjustment
ORDER BY series_id;
```

transform unit 확인:

```sql
SELECT
  series_id,
  units,
  units_short,
  transform_type,
  transformed_unit,
  COUNT(*) AS row_count,
  MIN(period_start) AS min_period_start,
  MAX(period_start) AS max_period_start
FROM fred_lakehouse.gold.fred_transformed_features_long
WHERE as_of_date = DATE '2026-07-07'
  AND target_frequency = 'monthly'
  AND aggregation_method = 'last'
GROUP BY series_id, units, units_short, transform_type, transformed_unit
ORDER BY series_id, transform_type;
```

## 19. Gold null 점검의 올바른 해석

사용자는 Gold에서 null이 없는지 확인하고 싶어 했다. Gold에서는 모든 column의 null이 오류인 것은 아니다.

예상 가능한 정상 null:

```text
analysis_start_date: widget blank면 NULL
current-only vintage_date, revision_number, revision_count
초기 period의 lag_1/3/6/12
rolling_stddev_12의 짧은 window
log transform 대상이 0 이하인 경우 해당 transformed row 자체가 제외됨
change/pct/log transform의 초기 lag row도 transformed stage에서 제외됨
```

반드시 non-null이어야 하는 핵심 key/value 위주로 점검해야 한다.

### 19.1 Gold key/value null

```sql
SELECT
  'fred_asof_observations' AS table_name,
  COUNT(*) AS total_rows,
  COUNT_IF(as_of_date IS NULL) AS null_as_of_date,
  COUNT_IF(series_id IS NULL OR TRIM(series_id) = '') AS null_series_id,
  COUNT_IF(observation_date IS NULL) AS null_observation_date,
  COUNT_IF(value_numeric IS NULL) AS null_value_numeric
FROM fred_lakehouse.gold.fred_asof_observations
UNION ALL
SELECT
  'fred_period_features_long',
  COUNT(*),
  COUNT_IF(as_of_date IS NULL),
  COUNT_IF(series_id IS NULL OR TRIM(series_id) = ''),
  COUNT_IF(period_start IS NULL),
  COUNT_IF(value_numeric IS NULL)
FROM fred_lakehouse.gold.fred_period_features_long
UNION ALL
SELECT
  'fred_transformed_features_long',
  COUNT(*),
  COUNT_IF(as_of_date IS NULL),
  COUNT_IF(series_id IS NULL OR TRIM(series_id) = ''),
  COUNT_IF(period_start IS NULL),
  COUNT_IF(transformed_value IS NULL)
FROM fred_lakehouse.gold.fred_transformed_features_long;
```

### 19.2 metadata null by series

```sql
SELECT
  series_id,
  history_type,
  COUNT(*) AS row_count,
  COUNT_IF(units IS NULL OR TRIM(units) = '') AS missing_units,
  COUNT_IF(units_short IS NULL OR TRIM(units_short) = '') AS missing_units_short,
  COUNT_IF(source_frequency IS NULL OR TRIM(source_frequency) = '') AS missing_source_frequency,
  COUNT_IF(seasonal_adjustment IS NULL OR TRIM(seasonal_adjustment) = '') AS missing_seasonal_adjustment
FROM fred_lakehouse.gold.fred_period_features_long
GROUP BY series_id, history_type
ORDER BY series_id, history_type;
```

metadata field가 모든 provider에서 항상 제공되는지 별도 확인해야 한다. `seasonal_adjustment` null은 provider metadata gap일 수 있지만 value 계산 실패와 같은 종류의 오류는 아니다.

### 19.3 transform coverage

```sql
SELECT
  series_id,
  transform_type,
  COUNT(*) AS row_count,
  MIN(period_start) AS first_period,
  MAX(period_start) AS last_period
FROM fred_lakehouse.gold.fred_transformed_features_long
WHERE as_of_date = DATE '2026-07-07'
  AND target_frequency = 'monthly'
  AND aggregation_method = 'last'
GROUP BY series_id, transform_type
ORDER BY series_id, transform_type;
```

series마다 transform row count가 다른 것은 lag loss, zero denominator, nonpositive log input 때문에 정상일 수 있다.

## 20. Schema 변경 후 table reset에 관한 기록

metadata column을 추가한 뒤 사용자는 기존 table을 삭제할 SQL을 요청했다. 현재 notebook에는 schema evolution helper가 있으므로 항상 full drop이 필수인 것은 아니다. 그러나 clean rebuild가 필요할 때는 dependency 순서에 따라 Gold -> Silver current -> Bronze current 순서로 drop한다.

다음 SQL은 파괴적이므로 대상 catalog와 복구 계획을 확인한 뒤에만 실행한다. 이 handoff 작성 과정에서는 실행하지 않았다.

```sql
-- Optional correlation outputs first
DROP VIEW IF EXISTS fred_lakehouse.gold.fred_top_lagged_correlations;
DROP TABLE IF EXISTS fred_lakehouse.gold.fred_lagged_correlation_scores;
DROP TABLE IF EXISTS fred_lakehouse.gold.fred_lagged_correlation_run_summary;

-- Active Gold 03 outputs
DROP VIEW IF EXISTS fred_lakehouse.gold.fred_latest_feature_snapshot;
DROP TABLE IF EXISTS fred_lakehouse.gold.fred_series_feature_snapshot;
DROP TABLE IF EXISTS fred_lakehouse.gold.fred_transformed_features_long;
DROP TABLE IF EXISTS fred_lakehouse.gold.fred_period_features_long;
DROP TABLE IF EXISTS fred_lakehouse.gold.fred_asof_observations;
DROP TABLE IF EXISTS fred_lakehouse.gold.fred_gold_quality_report;
DROP TABLE IF EXISTS fred_lakehouse.gold.fred_gold_run_summary;

-- Current-only Silver outputs
DROP VIEW IF EXISTS fred_lakehouse.silver.fred_current_observations_analytics_ready;
DROP TABLE IF EXISTS fred_lakehouse.silver.fred_current_observations_cleaned;
DROP TABLE IF EXISTS fred_lakehouse.silver.fred_current_series_catalog;
DROP TABLE IF EXISTS fred_lakehouse.silver.fred_current_quality_report;
DROP TABLE IF EXISTS fred_lakehouse.silver.fred_current_run_summary;

-- Current-only Bronze source shared by 01c, 01d, 01f
DROP TABLE IF EXISTS fred_lakehouse.bronze.fred_current_observations_raw;
```

ALFRED revision-aware table을 current-only metadata 변경 때문에 함께 삭제할 필요는 없다. 즉 다음은 별도 이유가 없으면 보존한다.

```text
bronze.fred_observation_versions
silver.fred_observation_versions_cleaned
```

과거 이름의 table이 Databricks에 남았을 수 있다.

```sql
DROP VIEW IF EXISTS fred_lakehouse.gold.fred_top_relationship_candidates;
DROP TABLE IF EXISTS fred_lakehouse.gold.fred_relationship_candidate_scores;

DROP VIEW IF EXISTS fred_lakehouse.gold.fred_pcmci_target_links;
DROP TABLE IF EXISTS fred_lakehouse.gold.fred_pcmci_causal_links;
DROP TABLE IF EXISTS fred_lakehouse.gold.fred_pcmci_run_summary;
```

이 legacy object drop도 사용자의 Databricks 실제 상태를 확인한 뒤 수행한다.

## 21. Bronze/Silver 변경이 Gold에 미치는 영향

사용자는 Bronze와 Silver 변경이 Gold code에 영향을 주지 않는지 물었다. 결론은 다음과 같다.

1. 새 metadata column은 additive schema change이므로 03이 해당 column을 읽도록 이미 수정된 상태에서는 연결이 맞다.
2. 02c는 old Bronze table에도 missing column을 추가하므로 schema mismatch를 완화한다.
3. 03의 current-only branch는 `frequency`, `frequency_short`, `units`, `units_short`, `seasonal_adjustment`를 Silver에서 읽는다.
4. 따라서 01c/01d/01f -> 02c -> 03 순서로 재실행해야 기존 row에도 metadata가 채워진다.
5. 기존 Bronze row가 과거 schema로 남아 있고 provider loader를 재실행하지 않으면 column은 생겨도 값은 null일 수 있다.
6. schema evolution이 code compatibility는 지켜 주지만 historical row backfill을 자동 보장하지는 않는다.
7. Gold의 revision-aware branch는 기존 01a/01b/02a/02b metadata contract에 의존하므로 current-only 변경이 그 branch를 직접 깨뜨리지는 않는다.

## 22. 현재 active flow를 화살표로 표현

```text
fred_seed_series.json
  -> build_fred_series_metadata.py
  -> fred_series_metadata.json
       -> 01d Macrotrends bootstrap ----┐
       -> 01f Yahoo bootstrap ----------┤
                                        v
01c FRED current ----------------> bronze.fred_current_observations_raw
                                        |
                                        v
                              02c Silver current cleaned
                                        |
                                        |
01a/01b ALFRED versions -> 02a/02b Silver revision-aware
                                        |
                    revision-aware -----+----- current-only
                                        v
                          03 Gold as-of feature mart
                                        |
             +--------------------------+--------------------------+
             v                          v                          v
fred_period_features_long   fred_transformed_features_long   feature snapshot
                                        |
                                        v
                    optional 03b lagged correlation EDA
```

인과추론 04로 이어지는 화살표는 현재 없다.
## 23. 논문 폴더와 연구 학습 요청

사용자는 root `논문/` 폴더에 앞으로 하고자 하는 인과추론 분야의 논문을 모아 두었고, assistant가 각 논문을 하나씩 상세히 완독하며 연구 분야를 학습하기를 요청했다.

현재 확인된 PDF는 다음과 같다.

```text
논문/ANCESTRAL GRAPH MARKOV MODELS.pdf
논문/Causal Inference in the Presence of Latent Variables and Selection.pdf
논문/Complete Causal Identification from Ancestral Graphs under Selection Bias.pdf
논문/Detecting and quantifying causal associations in large.pdf
논문/Discovering contemporaneous and lagged causal relations in autocorrelated.pdf
논문/Discovering cyclic causal models in psychological research.pdf
논문/High-recall causal discovery for autocorrelated time.pdf
논문/On Causal Discovery from Time Series Data using FCI.pdf
논문/On the completeness of orientation rules for causal discovery.pdf
논문/Pearl, “An Introduction to Causal Inference” (2010).pdf
논문/Supplementary Materials for Detecting and quantifying causal associations in large nonlinear time series datasets.pdf
```

일부 파일명은 논문 full title보다 짧게 저장되어 있다. 새 채팅은 metadata나 첫 page를 확인해 정확한 title, author, year, venue를 기록해야 한다.

### 23.1 논문 읽기에 대한 사용자 요구 수준

```text
abstract나 conclusion만 읽지 않는다.
본문을 처음부터 끝까지 완독한다.
수식의 기호와 가정을 확인한다.
algorithm과 orientation rule을 확인한다.
figure와 table을 시각적으로 확인한다.
supplement가 있으면 본문과 연결해 읽는다.
우리 dataset에 적용 가능한 가정과 적용 불가능한 가정을 구분한다.
```

새 채팅이 paper reading을 재개할 때는 논문별로 다음 결과를 남기는 것이 좋다.

```text
bibliographic metadata
research question
formal assumptions
notation glossary
main theorem/proposition
algorithm steps
identification result
latent confounding/selection bias treatment
stationarity/autocorrelation/contemporaneous edge treatment
simulation design
empirical application
limitations
우리 프로젝트에 적용 가능한 부분
우리 프로젝트에서 성립 여부를 확인할 수 없는 가정
추가로 필요한 data 또는 sensitivity analysis
```

### 23.2 계획했던 reading order

첫 논문은 Pearl 2010으로 시작하기로 했다. 이후에는 foundation에서 latent-variable graph, orientation completeness, time-series FCI와 modern time-series discovery 순으로 읽는 것이 자연스럽다.

```text
1. Pearl, An Introduction to Causal Inference (2010)
2. ANCESTRAL GRAPH MARKOV MODELS
3. Causal Inference in the Presence of Latent Variables and Selection...
4. On the completeness of orientation rules for causal discovery
5. Complete Causal Identification from Ancestral Graphs under Selection Bias
6. On Causal Discovery from Time Series Data using FCI
7. High-recall causal discovery for autocorrelated time...
8. Discovering contemporaneous and lagged causal relations in autocorrelated...
9. Detecting and quantifying causal associations in large...
10. 해당 supplementary materials
11. Discovering cyclic causal models in psychological research
```

이 순서는 필요하면 논문 citation dependency에 따라 조정할 수 있지만, correlation ranking을 causal discovery의 전제로 삼지 않는 원칙은 유지한다.

## 24. 첫 번째 논문 읽기와 PDF rendering의 실제 상태

### 24.1 무엇을 했는가

Pearl 2010 PDF에 대해 text extraction과 page rendering 환경을 준비했다. Poppler와 Tesseract를 설치 또는 확인했고 PDF pages를 PNG로 rendering했다.

확인된 tool path:

```text
Poppler pdftoppm:
C:\Users\mike1\AppData\Local\Microsoft\WinGet\Packages\oschwartz10612.Poppler_Microsoft.Winget.Source_8wekyb3d8bbwe\poppler-25.07.0\Library\bin\pdftoppm.exe

Poppler pdfinfo:
C:\Users\mike1\AppData\Local\Microsoft\WinGet\Packages\oschwartz10612.Poppler_Microsoft.Winget.Source_8wekyb3d8bbwe\poppler-25.07.0\Library\bin\pdfinfo.exe

Tesseract:
C:\Program Files\Tesseract-OCR\tesseract.exe
```

생성된 local artifact:

```text
tmp/pdfs/pearl_2010/pages/page_001.png ... page_061.png
tmp/pdfs/pearl_2010/contact_sheets/sheet_01.png ... sheet_11.png
tmp/pdfs/pearl_2010/index.html
tmp/pdfs/pearl_2010/render_manifest.json
tmp/pdf_render/pearl_2010/page_19.png
```

### 24.2 무엇을 제대로 완료하지 못했는가

`view_image`가 DPAPI error로 실패했기 때문에 rendered page의 수식, 그림, 표를 assistant가 실제로 시각 inspection하는 단계는 완료되지 않았다. OCR 또는 text extraction으로 paragraph를 읽을 수는 있었지만 그것은 간접 reading이다.

사용자는 여러 번 다음을 확인했다.

```text
첫 논문을 렌더링까지 해서 읽었는가
PDF 자체를 읽을 방법은 없는가
수식과 그림을 제대로 읽지 못한 것인가
view_image 실패 원인은 무엇인가
어떻게 해결할 수 있는가
```

정확한 답은 다음과 같다.

1. PDF를 모델이 시각적으로 읽으려면 native PDF viewer든 page rasterization이든 page image representation이 필요하다.
2. PNG rendering은 PDF 내용을 바꾸기 위한 것이 아니라 page layout을 visual input으로 만들기 위한 intermediary다.
3. 당시 text layer는 읽을 수 있었지만 visual page inspection은 실패했다.
4. 따라서 Pearl 논문을 수식과 그림까지 포함해 완독했다고 말하면 안 된다.
5. 새 채팅에서 PDF skill 또는 정상 동작하는 PDF page screenshot/view tool을 사용해 처음부터 다시 검증해야 한다.

### 24.3 새 채팅의 paper reading 기준

새 채팅에서는 첫 논문을 이미 완독했다고 가정하지 않는다. Pearl 2010부터 다시 시작한다. page count, text extraction, rendered page visual inspection을 모두 완료하고, 특히 equation, DAG figure, do-operator 설명, identification/adjustment 관련 그림을 눈으로 확인한 뒤에만 완료라고 보고한다.

`tmp/` rendering artifact는 재사용할 수 있지만, local temporary output이므로 commit하지 말고 먼저 integrity를 확인한다. `view_image`가 여전히 실패하면 실패 사실을 즉시 말하고 text-only reading을 visual reading으로 표현하지 않는다.

## 25. 대화의 시간순 결정 기록

이 section은 새 채팅이 판단 변화의 순서를 이해하도록 주요 대화 흐름을 순서대로 보존한다.

1. 사용자는 04c PCMCI의 `Aligned sample` 날짜가 무엇인지 질문했다.
2. 모든 selected variable이 동시에 값을 가진 period 교집합과 complete-case finite row로 matrix를 만든다는 점을 확인했다.
3. 사용자는 PCMCI가 교집합에 대해 실행되는지 재확인했고, 그렇다는 답과 표본 축소 위험을 논의했다.
4. 사용자는 실제 NASDAQCOM PCMCI run log를 제공했다. 10 variables, 350 monthly aligned observations, 1997-01-01부터 2026-05-01, 600 link rows, significant target link 0이었다.
5. 정상성 검정에서 M2SL Billions of Dollars와 FEDFUNDS Percent처럼 원천 단위가 다른 문제를 제기했다.
6. 단위 차이는 개별 정상성 검정의 직접 장애가 아니며 transform은 scale 통일보다 stochastic property와 경제적 해석을 위한 것이라고 정리했다.
7. 사용자는 01c, 01d, 01f가 원천 단위를 저장하지 않는 것을 확인했다.
8. 01c는 FRED에서 metadata를 직접 받을 수 있고, Macrotrends/Yahoo는 bootstrap source이므로 FRED canonical metadata를 별도로 가져오는 설계를 제안했다.
9. `fred_seed_series.json`에 generated field를 섞지 않고 별도 `fred_series_metadata.json`을 만들기로 했다.
10. `build_fred_series_metadata.py`가 추가됐고, 01d/01f가 이를 consumer로 사용하도록 변경됐다.
11. 사용자는 이를 통해 01a vintage path와 common column name을 맞출 수 있는지 물었고, metadata vocabulary는 통일하되 revision semantics는 분리해야 한다고 결론 냈다.
12. 사용자는 구현 진행을 요청했고 code change가 수행됐다.
13. schema 변경 후 기존 table reset SQL을 요청했다.
14. Bronze, Silver, Gold 각각의 확인 SQL을 요청했다.
15. 사용자는 03 Gold code가 metadata를 올바르게 연결하는지 확인해 달라고 요청했다.
16. Gold null 점검 SQL을 요청했다. 모든 null이 오류가 아니라 revision/lag/window semantics상 expected null이 있음을 구분했다.
17. Bronze/Silver 변경이 Gold에 영향을 주는지 확인했고 metadata propagation에 맞춰 Gold current branch도 수정된 상태임을 확인했다.
18. 사용자는 01c `observation_start`를 blank widget + fallback `1776-07-04`로 직접 수정했다고 알려 줬다. 최종 code를 기준으로 확인만 했다.
19. 정상성 검정 시 유의사항을 요청했다.
20. unit별 transform type을 물었고 unit heuristic만으로 결정할 수 없다고 설명했다.
21. series마다 stationarity test에 사용한 transform이 달라도 Granger test가 가능한지 물었다. 공통 time index에 stationary representation을 column별로 구성할 수 있지만 당시 code는 common transform만 지원했다고 정리했다.
22. 29개 series의 units 표를 제공하고 unit별 rule 여부를 물었다.
23. 같은 unit이어도 다른 transform이 필요할 수 있으며 series별 candidate transform set이 필요하다고 결론 냈다.
24. 사용자는 legacy `03_gold_fred_asof_features_legacy_no_analysis_window.py` 삭제를 요청했고 파일은 삭제 상태가 됐다.
25. 문서 최신화와 Gold 인과추론 문서 분리를 요청했다.
26. Gold flow를 화살표로 설명해 달라고 요청했다.
27. lagged correlation candidate screening의 의미를 다시 물었다.
28. 상관관계가 없다고 인과관계가 없는 것은 아니므로 causal candidate screening으로 쓰면 안 된다고 지적했다.
29. 상관분석 자체는 필요하지만 인과추론에 쓰면 안 된다는 원칙을 확정했다.
30. 03b가 correlation 계산 로직인지 확인했다.
31. 비관측 교란 없음 가정을 정당화할 수 없으므로 04 전체를 삭제하기로 했다.
32. 사용자는 앞으로 문서는 요청할 때만 수정하라고 명시했다.
33. 03b를 순수 correlation analysis로 최종 정의했다.
34. code/table name을 의미에 맞게 `relationship candidate`에서 `lagged correlation`으로 정리하도록 요청했다.
35. `논문/` 폴더의 causal inference 논문들을 상세히 읽고 연구 분야를 학습하도록 요청했다.
36. assistant가 정말 논문을 하나씩 자세히 읽었는지 사용자가 검증했다.
37. 읽는 순서를 먼저 설명하고 첫 논문부터 완독하라고 요청했다.
38. 필요한 PDF/OCR/rendering 환경을 설치해도 된다고 허용했다.
39. Pearl 첫 논문을 rendering까지 읽었는지 반복 확인했고, visual inspection이 실제로 완료되지 않은 점이 드러났다.
40. PDF를 직접 읽는 것과 PNG intermediary, tool 제한을 논의했다.
41. 환경이 부족하면 추가 설치하고 test하라고 요청했다.
42. text/OCR로 간접 reading은 가능하지만 수식과 figure visual reading이 미완료임을 확인했다.
43. `view_image` 실패 원인이 Windows sandbox/DPAPI임을 확인했다.
44. workspace path mismatch를 해결하기 위해 IDE를 원본 path로 다시 열고 Codex session reconnect 방법을 논의했다.
45. 실제 cwd가 `note` path로 잡힌 문제와 실제 user `mike1`을 구분했다.
46. session cwd를 영구 변경할 수 있는지 물었고 현재 session 안에서 확실히 바꾸기 어렵다는 결론이었다.
47. 새 session을 시작할 수 없어 junction workaround를 검토하고 실행했다.
48. junction 또는 copied folder에서도 DPAPI 문제가 계속돼 original path로 돌아가기로 했다.
49. `C:\Users\note` temporary tree를 제거하고 원상복구하려 했다. 현재 임시 path는 존재하지 않는다.
50. 새 채팅 전 모든 문서를 최신화하도록 요청했고 README와 layer docs가 갱신됐다.
51. 새 채팅용 `new_chat_handoff.md`를 먼저 만들었지만 초기 version은 짧은 overview였다.
52. 현재 요청에서 사용자는 길이 제한을 두지 말고 대화와 작업을 빠짐없이 담은 단일 상세 파일로 다시 작성하도록 요청했다.

## 26. 삭제와 이름 변경 history

### 26.1 삭제된 legacy Gold

```text
notebooks/databricks/gold/03_gold_fred_asof_features_legacy_no_analysis_window.py
```

사용자가 더 이상 쓰이지 않는다고 판단해 삭제를 명시적으로 요청했다. Git status에서 tracked deletion으로 남아 있다. 복구하지 않는다.

### 26.2 03 내부 relationship scoring 제거

이전 03 code는 다음 object를 만들었다.

```text
fred_relationship_candidate_scores
fred_top_relationship_candidates
```

그리고 `candidate_score = r_squared * coverage_rate`를 사용했다. 이 logic은 03에서 제거됐다.

### 26.3 새 03b naming

```text
03b_gold_lagged_correlation_analysis.py
fred_lagged_correlation_scores
fred_top_lagged_correlations
fred_lagged_correlation_run_summary
```

`candidate_series_id`는 `comparison_series_id`, `candidate_score`는 `correlation_score`, `candidate_rank`는 `correlation_rank` 의미로 바뀌었다.

### 26.4 제거된 04

04 stationarity/Granger/PCMCI 실험 파일은 active tree에 없다. 과거 Databricks output table은 자동 삭제되지 않았을 수 있으므로 table existence를 확인해야 한다.

## 27. 현재 검증 완료와 미완료

### 27.1 완료된 local 확인

```text
원본 repo path 존재 확인
임시 note path 부재 확인
현재 Git status 확인
active file 목록 확인
fred_seed_series.json UTF-8 JSON parse PASS
active docs에서 stale relationship/causal candidate naming 검색
new metadata propagation code diff 확인
03b score formula와 widget 확인
fred_series_metadata.json local 부재 확인
```

### 27.2 완료되지 않은 것

```text
Databricks cluster에서 전체 pipeline end-to-end rerun
FRED API를 호출해 fred_series_metadata.json 실제 생성
01d/01f bootstrap reingestion 후 모든 historical row metadata backfill
02c와 03 rerun 후 production Delta schema/data validation
03b 실제 rerun과 score table validation
과거 legacy/04 Delta object의 실제 존재 및 삭제 확인
모든 active notebook의 Databricks runtime integration test
논문 11개 시각 완독
Pearl 2010 수식/figure/table visual verification
view_image DPAPI 문제 해결
untracked tmp/와 논문/의 commit policy 결정
현재 dirty changes의 commit/PR
```

새 채팅은 이 미완료 항목을 완료된 것으로 가정하면 안 된다.
## 28. Pairwise correlation과 과거 PCMCI 교집합의 차이

새 채팅이 두 종류의 alignment를 혼동하지 않도록 구분한다.

과거 PCMCI 04c:

```text
선택된 모든 variable의 period set 전체 교집합
-> 하나의 complete-case multivariate matrix
-> 한 variable의 짧은 history가 전체 sample을 줄임
```

현재 03b correlation:

```text
target와 각 comparison을 lag별로 pairwise date join
-> comparison/lag마다 pair_count가 다를 수 있음
-> 한 comparison의 짧은 history가 다른 comparison score sample을 줄이지 않음
```

03b는 pair_count가 적은 관계를 `sample_reliability_score`로 낮추고 `min_pair_count` 미만은 제외한다. 그러나 이것도 missingness bias나 selection bias를 제거하는 방법은 아니다.

## 29. 향후 causal research를 재개할 때 필요한 설계

04 code를 단순 복구하지 않는다. 먼저 구체적인 연구 protocol을 만든다.

예를 들어 다음 질문에 답해야 한다.

```text
정확히 무엇의 무엇에 대한 causal effect를 알고 싶은가
예: policy rate shock이 NASDAQ return에 미치는 효과인가
단순 rate level 변화인가, 외생적 monetary policy shock인가
outcome horizon은 1개월, 3개월, 12개월 중 무엇인가
announcement timing과 monthly aggregation은 어떻게 연결하는가
available-at와 revision을 어떻게 처리하는가
어떤 pre-treatment covariate를 조정하는가
post-treatment variable을 잘못 조정하지 않는가
latent common cause를 어떻게 다루는가
selection bias와 measurement error가 있는가
identification이 point identification인지 partial identification인지
negative control, placebo, sensitivity analysis는 무엇인가
```

가능한 방법 이름을 나열하기 전에 DAG 또는 temporal causal graph와 data-generating assumptions를 문서화한다. FCI/PAG 같은 latent-confounder-aware discovery도 assumption-free가 아니다. causal sufficiency를 완화하더라도 Markov, faithfulness, sampling, measurement, stationarity/temporal assumptions를 검토해야 한다.

논문 학습의 목적은 어떤 algorithm을 바로 Gold notebook에 추가하는 것이 아니라 이 assumption boundary를 정확히 이해하는 것이다.

## 30. 새 채팅에서 추천하는 첫 작업 순서

새 채팅의 assistant는 다음 순서로 시작한다.

1. 이 파일 `docs/new_chat_handoff.md`를 끝까지 읽는다.
2. 실제 cwd와 원본 repository path를 확인한다.
3. `git status --short`를 확인하되 기존 변경을 건드리지 않는다.
4. `README.md`, `docs/bronze_layer.md`, `docs/silver_layer.md`, `docs/gold_layer.md`를 읽는다.
5. 작업 요청과 관련된 실제 notebook을 다시 읽는다.
6. generated `fred_series_metadata.json`이 필요한 작업이면 먼저 존재 여부와 API key/deployment path를 확인한다.
7. Databricks 실행을 요청받으면 initial/refresh 순서를 지키고 각 layer validation query를 실행한다.
8. 03b를 설명할 때 causal이라는 말을 붙이지 않는다.
9. causal research를 요청받으면 `docs/causality_scope.md`와 논문 reading 상태를 먼저 확인한다.
10. PDF reading을 재개하면 Pearl 2010부터 visual verification을 포함해 다시 읽는다.

## 31. 새 채팅 assistant에게 주는 작업 규칙

```text
문서는 사용자의 명시적 요청 없이 수정하지 않는다.
현재 dirty worktree의 변경을 되돌리지 않는다.
원본 경로만 사용한다.
임시 note 경로를 재생성하지 않는다.
상관관계와 인과관계를 엄격히 구분한다.
03b를 causal candidate screening이라고 부르지 않는다.
04를 사용자의 새 방법론 결정 없이 복구하지 않는다.
unit metadata와 numeric scale transformation을 혼동하지 않는다.
series별 transform은 unit만으로 자동 확정하지 않는다.
ADF/KPSS 결과 없이 stationary라고 단정하지 않는다.
current-only history를 ALFRED point-in-time-safe history라고 부르지 않는다.
논문은 text extraction만으로 visual 완독했다고 하지 않는다.
도구 오류와 미검증 상태를 숨기지 않는다.
```

## 32. 작업별 실제 source file 우선순위

metadata config 작업:

```text
notebooks/databricks/configs/fred_seed_series.json
notebooks/databricks/configs/build_fred_series_metadata.py
docs/fred_series_metadata_config.md
```

current-only Bronze 작업:

```text
notebooks/databricks/bronze/01c_bronze_fred_current_observations.py
notebooks/databricks/bronze/01d_bronze_macrotrends_observations.py
notebooks/databricks/bronze/01f_bronze_yahoo_finance_observations.py
```

Silver current 작업:

```text
notebooks/databricks/silver/02c_silver_fred_current_observations.py
```

Gold feature 작업:

```text
notebooks/databricks/gold/03_gold_fred_asof_features.py
docs/gold_layer.md
```

correlation 작업:

```text
notebooks/databricks/gold/03b_gold_lagged_correlation_analysis.py
docs/gold_lagged_correlation_analysis.md
```

causal research 작업:

```text
docs/causality_scope.md
논문/*.pdf
```

## 33. 현재 코드의 주의할 설계 세부사항

1. 01c/01d/01f 모두 `fred_current_observations_raw`를 공유하고 key는 `source, series_id, observation_date`다. provider가 달라도 `source=fred`이므로 같은 series/date를 bootstrap row와 FRED row가 공유하면 later merge가 metadata/value를 update할 수 있다. 실제 provider 이력은 `data_provider`와 `source_metadata_json`을 통해 확인한다.
2. 01d/01f external bootstrap은 actual historical vintage가 아니다. realtime field가 있더라도 ALFRED revision semantics로 해석하지 않는다.
3. 02c가 schema column을 추가해도 과거 row value를 자동 채우지 않는다. loader rerun이 필요하다.
4. Gold 03은 seed routing상 동일 series가 revision-aware와 current-only 두 Silver input에 동시에 나오지 않는다는 전제가 있다. routing이 깨지면 stage duplicate와 merge ambiguity를 점검해야 한다.
5. Gold full-sample z-score는 as-of/window 전체를 사용하므로 forecasting feature로 사용할 때 training leakage 관점에서 rolling/expanding alternative가 필요할 수 있다. 현재 이름에 `full_sample`이 명시돼 있다.
6. `index_base100`의 base는 analysis window 첫 period다. window가 바뀌면 값이 바뀐다.
7. 03b raw-level Pearson은 trend로 인한 spurious correlation 위험이 크다.
8. 03b correlation score는 p-value나 confidence interval이 아니다. 현재 main rank formula는 effect magnitude와 sample reliability heuristic이다.
9. 03b r-squared는 sign을 제거한다. direction은 `pearson_corr` column을 별도로 봐야 한다.
10. `min_correlation_score=0.3`은 statistical significance threshold가 아니라 top view용 heuristic threshold다.
11. daily/weekly/monthly aggregation에서 source release timing과 publication lag는 separate issue다. as-of selection이 data visibility를 다루지만 current-only provider는 true historical availability를 보장하지 않는다.
12. as_of_date가 오늘보다 미래이거나 data latest period보다 늦어도 code가 자동으로 future data를 만들지는 않는다. latest common/available observation에서 끝난다.

## 34. 아직 결정이 필요한 운영 사항

다음은 사용자 결정이 없으므로 새 채팅이 임의로 결정하지 않는다.

```text
fred_series_metadata.json을 Git에 commit할지 deployment에서 생성할지
tmp/ PDF render artifact를 삭제할지 보존할지
논문/ PDF를 Git에 포함할지 local research asset으로 둘지
현재 dirty changes를 어떤 commit 단위로 나눌지
Databricks의 과거 relationship/PCMCI table을 실제 drop할지 archive할지
03b default correlation_transform_type을 raw에서 바꿀지
03b에 series별 transform mapping을 추가할지
Gold transformed feature의 12-period naming을 frequency-aware alias로 보강할지
snapshot pct_change_1과 transformed pct_change_1의 scale 차이를 column name으로 명확히 할지
current-only source의 historical availability limitation을 dashboard에서 어떻게 표시할지
causal research의 첫 구체적 question을 무엇으로 정할지
```

## 35. 이 문서의 사용 방법

새 채팅 시작 prompt에서 다음처럼 요청하면 된다.

```text
C:\Users\mike1\OneDrive\Desktop\Data_Lakehouse\docs\new_chat_handoff.md를
처음부터 끝까지 읽고, 실제 Git 상태와 관련 source file을 확인한 뒤
이전 대화를 이어서 작업해줘.
```

새 assistant는 이 파일을 읽었다는 이유만으로 code 실행이나 paper reading이 완료됐다고 가정하면 안 된다. 이 파일은 지금까지의 맥락과 현재 상태를 복원하기 위한 기록이며, 실제 작업은 source와 environment를 다시 확인하면서 이어간다.

## 36. 최종 상태 한 문장

현재 프로젝트는 FRED/ALFRED와 외부 bootstrap history를 canonical FRED metadata와 함께 Bronze/Silver/Gold로 연결하는 as-of feature lakehouse이며, lagged correlation은 순수 EDA로만 남겨 두었고, 비관측 교란 가정을 정당화할 수 없는 04 causal pipeline은 제거했으며, 다음 큰 연구 작업은 PDF를 수식과 그림까지 실제로 완독해 latent-confounding-aware causal research design을 세우는 것이다.