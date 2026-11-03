# PROJECT_CONTEXT — FRED/ALFRED 금융·거시 시계열 Lakehouse 인수인계

소스 분석 기준일: 2026-11-02. 문서 교정·저장 완료일: 2026-11-03, Asia/Seoul.

## 0. 문서의 근거와 경로부터 확인할 것

이 문서는 프로젝트 소개가 아니라 다음 Codex 세션의 개발 인수인계 문서다. 실제 파일·설정·Git 상태, 기존 인수인계 문서, 현재 대화에서 확인되는 논의를 구분해 기록했다. **코드가 존재한다는 사실과 Databricks에서 성공적으로 실행됐다는 사실은 다르다.** 이번 작업은 읽기와 문서 생성이며, API 수집이나 Databricks Job 실행은 하지 않았다.

### 0.1 현재 세션과 실제 저장소가 서로 다른 위치에 있음

| 구분 | 경로 및 확인 결과 |
|---|---|
| 현재 Codex 세션 루트 | `C:\Users\mike1\OneDrive\Documents\ChatGPT\(상원) DataLakehouse` — 작업 시작 당시 비어 있었고 Git 저장소가 아니었음 |
| 실제 분석한 소스 저장소 | `C:\Users\mike1\OneDrive\Desktop\Data_Lakehouse` — `.git`, `README.md`, `docs`, `notebooks`, `논문`, `tmp`, `학연생.png` 존재 |
| 문서 저장 위치 | 실제 소스 저장소 루트의 `PROJECT_CONTEXT.md`. 현재 세션 루트에도 같은 내용의 전달본을 보관 |

이하 상대 경로는 **실제 소스 저장소**를 기준으로 한다. 현재 세션 루트에 코드가 복사되어 있다고 가정하지 않는다. 코드 작업을 재개할 때 실제 저장소를 작업 폴더로 열거나 명시적 `workdir`를 사용한다. 과거 `C:\Users\note\Desktop\데이터레이크하우스` 경로는 폐기된 경로이므로 재생성하지 않는다.

원본 파일은 읽을 수 있었지만 일반 권한의 Git 호출은 `Permission denied`가 발생했다. 읽기 전용 Git 명령을 승인된 추가 권한으로 재실행해 상태를 확인했다. 파일 손상이나 저장소 소실로 해석할 문제가 아니었다.

### 0.2 근거의 우선순위

1. 이번 확인한 실제 코드·설정·Git 상태: 현재 구현 사실.
2. `학연생.png`: 연구과제의 목적과 목표. 이번 작업에서 직접 이미지를 확인했다.
3. `docs/new_chat_handoff.md`: 2026-08-05 작성된 과거 작업·사용자 결정·실행 로그의 기록. 기록 시점을 붙여 사용한다.
4. 현재 대화: 논문 읽기, 인과설계, 정상성·계절성·추세 진단에 대한 논의. 제안과 구현 완료를 구분한다.
5. Databricks 실행 결과·외부 서비스 상태: 이번 작업에서는 접속하지 않았으므로 현재 상태 미검증.

기존 문서의 모든 설명이 현재 코드와 일치하지는 않는다. 특히 Gold 분석 구간 저장 키와 시간 lag 의미는 아래 5장에서 코드 기준으로 정정했다. 논문 PDF는 이번 문서 작성 작업에서 다시 완독하지 않았고 파일 목록과 이전 대화의 독서 기록을 구분했다.

## 1. 프로젝트 개요

### 1.1 연구 목적과 최종 목표

`학연생.png`의 1차년도(2026) 과제명은 **인과 분석을 위한 불규칙 혼합빈도 시계열 데이터 레이크하우스 구축**이다.

과제가 다루는 핵심 문제는 사회경제 시계열의 불규칙성, 혼합빈도, 결측, 사후 개정 이력, 지표 정의 변경이다. 시점별 원본 재현, 정제 과정에서의 인과적 의미 보존, 데이터 계보 추적을 지원하는 Medallion 아키텍처를 구축하는 것이 목적이다.

과제 이미지에 명시된 목표:

- 수천 개 이상의 불규칙 혼합빈도 시계열을 다루는 Lakehouse.
- 시간 정합성을 고려한 자동 수집과 원본 보존.
- 데이터 계보를 추적하는 인과 보존형 정제 파이프라인.
- 다양한 빈도·차원의 Gold 집계 및 서빙.
- 인과 변수 탐색과 정상성·독립성·인과 검정, 계절성·추세 분해 등 표준 연산 커널.
- 에이전트에서 호출 가능한 표준 연산 커널 15종 이상.
- 장기 데이터 소스 예시: Fed, 국가통계포털, 통계청, OECD, Bloomberg 등.

이는 연구 목표이며 현재 구현 완료 범위가 아니다. 현재는 FRED/ALFRED 중심의 29개 시리즈와 Macrotrends/Yahoo 보조 적재를 구현했다. 다기관 수천 개 시리즈, 15개 커널, 에이전트 호출 API가 완료됐다는 근거는 없다.

### 1.2 현재 활성 제품 범위

```text
FRED/ALFRED 및 외부 bootstrap
  → Bronze: 수집 원형·revision·metadata·lineage
  → Silver: 타입 정제·품질 태깅·revision/current-only 분리
  → Gold 03: 단일 as-of 재구성·빈도 집계·9개 transform·snapshot
  → Gold 03b: 선택적 lagged Pearson correlation EDA
```

현재 활성 코드에는 causal discovery 또는 causal effect estimation이 없다. 과거 04 계열은 방법론적 이유로 제거됐다. `03b`는 인과 변수 선택의 필터가 아니며, 높은 상관을 원인으로 또는 낮은 상관을 인과 부재로 해석하지 않는다.

### 1.3 핵심 요구사항

- 원본 값과 개정 이력을 보존하고 요청·수집·정제·산출물의 lineage를 추적한다.
- ALFRED revision-aware와 current-only 재구성 이력의 시간 의미를 구분한다.
- 원천 단위, 빈도, 계절조정 정보를 downstream까지 전달한다.
- 기간·as-of·집계·변환 기준을 명시해 재실행 가능하게 한다.
- 연구 질문과 식별 가정을 먼저 정의한 후 인과분석을 설계한다.
- 변환 이름이나 상관점수로 정상성·인과성을 자동 확정하지 않는다.

## 2. 프로젝트 구조와 기술 스택

### 2.1 주요 파일 구성

```text
Data_Lakehouse/
├─ README.md
├─ .gitignore
├─ 학연생.png
├─ PROJECT_CONTEXT.md                         이번 인수인계 문서
├─ docs/
│  ├─ bronze_layer.md
│  ├─ silver_layer.md
│  ├─ gold_layer.md
│  ├─ gold_lagged_correlation_analysis.md
│  ├─ fred_series_metadata_config.md
│  ├─ causality_scope.md
│  └─ new_chat_handoff.md                     2026-08-05 과거 상세 기록
├─ notebooks/databricks/
│  ├─ configs/
│  │  ├─ fred_seed_series.json
│  │  ├─ build_fred_series_metadata.py
│  │  └─ fred_series_metadata.json            생성 대상; 현재 로컬에는 없음
│  ├─ macrotrends/
│  │  └─ BAMLH0A0HYM2_chart_20260702T065042.csv
│  ├─ bronze/
│  │  ├─ 01a_bronze_fred_bootstrap_versions.py
│  │  ├─ 01b_bronze_fred_incremental_versions.py
│  │  ├─ 01c_bronze_fred_current_observations.py
│  │  ├─ 01d_bronze_macrotrends_observations.py
│  │  ├─ 01e_bronze_alfred_reproducibility_audit.py
│  │  └─ 01f_bronze_yahoo_finance_observations.py
│  ├─ silver/
│  │  ├─ 02a_silver_fred_bootstrap_versions.py
│  │  ├─ 02b_silver_fred_incremental_versions.py
│  │  └─ 02c_silver_fred_current_observations.py
│  └─ gold/
│     ├─ 03_gold_fred_asof_features.py
│     └─ 03b_gold_lagged_correlation_analysis.py
├─ 논문/                                      12개 PDF; supplementary 포함
└─ tmp/                                       PDF 렌더링 등 임시 연구 자료
```

`__pycache__`에 삭제된 04 또는 옛 03b의 `.pyc`가 남아 있다. 이를 활성 구현 또는 복구해야 할 소스로 간주하지 않는다. 기존 파일 삭제·정리는 이번 작업의 범위가 아니다.

### 2.2 실행 환경

- Python으로 작성된 Databricks source notebook: `# Databricks notebook source`, `# COMMAND ----------`, `# MAGIC` 형식.
- Spark/PySpark, Spark SQL, Delta Lake, Unity Catalog.
- `spark`, `dbutils.widgets`, `dbutils.secrets`, `display`는 Databricks가 제공하는 실행 객체.
- API 호출은 주로 Python 표준 라이브러리 `urllib`; JSON, CSV, datetime, pathlib, hashlib 사용.
- Delta `MERGE`, schema column 추가, append run log, Change Data Feed와 optimize 속성 설정.
- Gold는 SQL window, `LAG`, `STACK`, `ROW_NUMBER`, `corr` 등을 사용한다.
- 기본 catalog: `fred_lakehouse`; schema: `bronze`, `silver`, `gold`.
- 비밀정보: Databricks scope `fred-lakehouse`, key `fred_api_key`. 실제 secret 값은 문서나 Git에 기록하지 않는다.
- metadata builder만 `FRED_API_KEY` 등 환경변수를 통한 로컬 실행 경로가 있다. 모든 notebook을 일반 Python 스크립트처럼 실행할 수 있는 것은 아니다.
- Python 타입 표기에 `str | None`이 사용된다. 정확한 Databricks Runtime·Spark·라이브러리 버전은 저장소에서 고정되어 있지 않아 배포 환경 확인이 필요하다.

활성 파일 목록에서 패키지 lockfile, 테스트 suite, CI workflow, Lakeflow Job 배포 JSON/YAML은 확인되지 않았다. Lakeflow 실행 흐름은 코드 주석에 권장 구조로 쓰여 있으나 실제 스케줄 배포를 뜻하지 않는다.

## 3. 현재까지 구현한 작업

### 3.1 수집 대상과 metadata 설정

`fred_seed_series.json`은 사람이 관리하는 routing 설정이다. 이번에 UTF-8 JSON 파싱을 확인했고, 총 29개 중 revision-aware 경로 23개, current-only 경로 6개다. 아래 분류는 **현재 seed 설정**이며 FRED의 최신 서비스 가용성을 이번에 조회한 결과는 아니다.

| 빈도 | ALFRED 경로 (`alfred_available=true`) | Current-only 경로 (`false`) |
|---|---|---|
| 분기 | GDPC1, BOGZ1FL663067003Q | 없음 |
| 월 | PCE, PAYEMS, UNRATE, AWHMAN, INDPRO, CPIAUCSL, PCEPILFE, FEDFUNDS, GS10, M2SL, PERMIT, NEWORDER, UMCSENT, BOGMBASE | W875RX1, CMRMTSPL, T10YFFM |
| 주 | ICSA, WALCL | 없음 |
| 일 | DGS10, T10Y2Y, T10Y3M, NASDAQNQUSB40, NASDAQNQUSB45 | BAMLH0A0HYM2, SP500, NASDAQCOM |

최근 미커밋 seed 변경으로 `NASDAQNQUSB40`(경기민감 소비재)과 `NASDAQNQUSB45`(필수소비재)가 추가됐다.

`build_fred_series_metadata.py`는 seed와 FRED `/fred/series` 응답을 합쳐 canonical metadata JSON을 만든다.

- 입력: seed, FRED API key, 선택 series.
- 출력: `generated_at_utc`, provider/endpoint, seed 경로, `series_count`, series ID별 metadata 사전.
- 필드: title, units/units_short, frequency/frequency_short, seasonal_adjustment/short, observation 범위, last_updated, notes, domain, priority 등.
- 모든 선택 series 요청이 성공해야 파일을 기록한다. 실패 시 partial 결과를 정상 출력으로 남기지 않는다.
- `series_ids`를 subset으로 실행하면 출력도 subset이다. 전체 consumer를 위해서는 필요한 모든 series가 포함됐는지 확인한다.
- 상대 output path는 해결된 seed 파일의 부모 경로를 기준으로 한다.
- 생성 파일은 이번 확인에도 로컬에 없었다. 01d/01f 실행 전 생성·배포가 필요하다.

### 3.2 Bronze

| 모듈 | 구현 역할 및 핵심 설정 |
|---|---|
| `01a` | ALFRED 초기 revision history 수집. `include_vintages=true`, realtime 범위 `1776-07-04`~`9999-12-31`, `output_type=1`. 큰 요청은 관측연도 5년 및 vintage 1500개 기본 chunk fallback |
| `01b` | vintage date와 watermark 기반 증분 수집. fallback lookback 30일, overlap 3일. 동일 값·기존 realtime 구간으로 덮이는 응답은 기존 observation version과 연결하여 조회 창 변화로 불필요한 version이 생기는 것을 줄임 |
| `01c` | current-only FRED 관측치와 `/fred/series` metadata 수집. 빈 `observation_start`는 `1776-07-04`로 처리 |
| `01d` | 수동 Macrotrends CSV bootstrap. 기본 BAMLH0A0HYM2, 날짜 형식 `%m/%d/%Y,%Y-%m-%d`, generated metadata 사용 |
| `01e` | 내부 Bronze as-of 복원과 live ALFRED `vintage_dates` 응답 비교. 기본 GDPC1, 불일치 표시, `fail_on_mismatch=false`; 별도 영구 audit 테이블은 만들지 않음 |
| `01f` | Yahoo chart API bootstrap. 기본 `SP500 → ^GSPC`, `interval=1d`, `price_field=close`, 시작 1900-01-01. generated FRED metadata와 Yahoo 보조 metadata 연결 |

Bronze 주요 테이블:

```text
fred_raw_response_payloads       원천 FRED/ALFRED JSON 응답
fred_ingestion_runs              요청별 실행 이력
fred_series_metadata_versions    versioned 경로의 지표 metadata
fred_observation_versions        revision-aware 관측 버전
fred_vintage_dates_seen          확인한 vintage 날짜
fred_incremental_watermarks      증분 수집 위치
fred_current_observations_raw     FRED current + 외부 bootstrap 공유
fred_run_summary                 실행 요약
```

01c/01d/01f는 `source, series_id, observation_date`를 공통 merge key로 사용한다. `source=fred`는 논리적 호환용 명칭이며 실제 제공자는 `data_provider=fred_api|macrotrends|yahoo_finance`로 구분한다. 같은 날짜에 provider가 겹치면 나중 실행의 merge가 값·metadata를 갱신할 수 있다. 이 테이블을 provider별 불변 역사 테이블로 해석하지 않는다.

최근 구현에서는 값뿐 아니라 units, frequency, seasonal adjustment, domain, priority, period 추론, provider 등의 변경도 update 조건에 반영한다. 단위 metadata를 붙이는 것은 숫자의 scale을 실제 변환하는 것과 다르다.

### 3.3 Silver

| 모듈 | 역할 |
|---|---|
| `02a` | Bronze ALFRED 전체 bootstrap 정제. `clean_fred_versioned_observations`, version `0.3.0` |
| `02b` | 초기 Silver 존재를 전제로, Bronze run 또는 이전 Silver watermark 이후 변경된 series를 재처리. revision 순서·품질 계산에 해당 series 이력을 사용 |
| `02c` | current-only Bronze 정제와 metadata 전달. version `0.3.0-current-common-observation-fields` |

공통 작업: 문자열 값·날짜 typing, 결측·parse 오류·잘못된 realtime 범위 확인, 중복 처리, 품질 상태와 lineage 기록. 기본 이상치 임계값은 6.0, missing 보존 옵션 기본값은 true다.

MAD 기반 robust score를 사용한다. `02c`의 최종 outlier 판정은 관측 차분 기반 score이며 level score도 보조 컬럼으로 저장한다. revision-aware 정제는 revision 변화 관련 score도 계산한다. 이상치 표시가 자동 제거·보간이나 인과 보존의 수학적 보증을 뜻하지 않는다.

Revision-aware 핵심 필드:

```text
value_numeric, observation_date, period_start, period_end
available_at, realtime_start, realtime_end, vintage_date
revision_number, revision_count, is_current_version
is_point_in_time_usable, quality_status, quality_issues
is_outlier, outlier_score, Bronze/Silver lineage
```

Current-only는 `history_type=current_only`, `is_point_in_time_safe=false`다. `02c`는 Bronze metadata를 우선 사용하고 필요한 경우 seed와 observation date로 fallback한다. 새 컬럼을 추가하는 schema evolution과 기존 행의 metadata backfill은 다른 작업이다.

Silver 주요 테이블·뷰:

```text
fred_observation_versions_cleaned
fred_version_series_catalog
fred_version_quality_report
fred_version_lineage_events
fred_version_run_summary
fred_observations_asof_ready                 view
fred_observations_current                    view
fred_current_observations_cleaned
fred_current_series_catalog
fred_current_quality_report
fred_current_run_summary
fred_current_observations_analytics_ready     view
```

### 3.4 Gold 03: as-of feature mart

구현 version은 `1.0.1-asof-integrated-source-units`다. 두 Silver cleaned 테이블을 요구하고 합친다. 한쪽 series만 분석하더라도 입력 테이블 존재 검사는 두 테이블 모두에 적용된다.

주요 widget 기본값:

| 설정 | 기본값 |
|---|---|
| `catalog` | fred_lakehouse |
| `as_of_date` | 빈 값 → UTC 오늘 |
| `analysis_start_date` | 빈 값 → 사용 가능한 첫 period |
| `analysis_end_date` | 빈 값 → as_of_date |
| `series_ids` | ALL |
| `target_frequency` | monthly; daily/weekly/monthly/quarterly/annual/native 지원 |
| `aggregation_method` | last; mean도 지원 |
| `include_quality_warnings` | true |
| `optimize_tables` | true; 환경 미지원 시 메시지 후 계속 |

Revision-aware 선택 조건은 usable·품질·non-null 값에 더해 `observation_date <= as_of_date`, `realtime_start <= as_of_date <= realtime_end`, `coalesce(available_at,realtime_start) <= as_of_date`다. 중복 visible version은 realtime_start, available_at, observation_version_id 내림차순으로 하나를 선택한다.

Current-only는 observation_date까지만 제한하고 `availability_basis=observation_date`, `is_revision_aware=false`, Gold의 `available_at=observation_date`를 붙인다. 이것은 과거 실제 발표일 확인 결과가 아니다.

월·분기·연·주 집계는 observation_date를 기준으로 한다. 주간 구간은 월요일~일요일이다. `native`는 Silver period 경계를 이용한다. `last`는 해당 구간의 마지막 관측, `mean`은 평균이다. 빈 달 채우기, 발표시점 패널, 분기 GDP를 실제 월별 GDP로 변환하는 기능은 없다.

Gold 산출물:

| 테이블/뷰 | 역할 |
|---|---|
| `fred_asof_observations` | 단일 as-of에서 선택한 관측치 |
| `fred_period_features_long` | target 빈도 대표값·집계 통계·단위·품질·lineage |
| `fred_transformed_features_long` | 9개 변환의 long-form 값; 주요 분석 입력 |
| `fred_series_feature_snapshot` | series별 마지막 period 및 lag/rolling 값 |
| `fred_latest_feature_snapshot` | 분석 구간·빈도·집계·series별 최신 as-of 뷰 |
| `fred_gold_quality_report` | 세 가지 품질 규칙의 결과 |
| `fred_gold_run_summary` | 실행 설정과 행 수·lineage 요약 |

9개 변환에서 `x`는 집계한 period 값, `lag(k)`는 **정렬된 k행 전** 값이다.

| transform_type | 실제 계산 | 주의 |
|---|---|---|
| raw | x | 원천 단위 |
| change_1 | x − lag(1) | Percent 원천은 percentage point 의미 |
| change_12 | x − lag(12) | 12개월 보장 아님 |
| pct_change_1 | 100 × (x / lag(1) − 1) | 분모 0이면 null |
| pct_change_12 | 100 × (x / lag(12) − 1) | 동일 |
| log_diff_1 | 100 × (ln x − ln lag(1)) | 두 값 모두 양수 필요 |
| log_diff_12 | 100 × (ln x − ln lag(12)) | 양수 및 기간 간격 확인 |
| z_score_full_sample | (x − 해당 구간 평균) / 표본 표준편차 | 정상성을 만들지 않음 |
| index_base100 | 100 × x / 해당 구간 첫 값 | 첫 값이 0이면 null |

`transformed_value IS NOT NULL`만 저장한다. 따라서 series마다 9개 transform의 행 수가 같지 않으며, 초기 lag 손실·0 분모·비양수 로그를 전부 적재 오류로 취급하면 안 된다.

Snapshot의 lag는 1/3/6/12, rolling mean은 3/6/12, rolling stddev는 12행 창이다. 초기 부분 창에서 계산될 수 있어 이름만 보고 완전한 12기간 표본을 보장한다고 생각하지 않는다. Feature Store 등록은 실행 코드가 아니라 안내문이며, 실제 등록·PK·training lookup은 검증되지 않았다.

### 3.5 Gold 03b: lagged correlation

구현 version: `0.2.0-lagged-correlation-analysis`. 입력은 `fred_transformed_features_long`이며 as-of, analysis start/end, target frequency, aggregation, transform이 정확히 일치해야 한다.

- 기본 target `GDPC1`, comparison `ALL`, transform `raw`, lag 0~12.
- target 자신은 comparison에서 제외한다.
- 한 실행에서 모든 series에 같은 transform을 적용한다. series별 mapping은 미구현.
- target 시점 t와 comparison 시점 t−lag를 pairwise 날짜 join한다.
- daily lag는 달력 일수, weekly는 7일, monthly/quarterly/annual은 각각 1/3/12개월 배수다.
- `native`도 현재 구현은 월 단위 `add_months`를 사용한다. 모든 원천 빈도에 일반적으로 맞는 동작은 아니다.

```text
r = corr(target, comparison)
r_squared = r²
coverage_rate = pair_count / target_period_count
sample_reliability_score = min(1, pair_count / reliable_pair_count)
correlation_score = r² × sample_reliability_score
coverage_weighted_score = r² × coverage_rate
```

AUTO의 최소/신뢰 표본 수: daily 252/756, weekly 104/260, monthly 36/60, quarterly 24/40, annual 15/25, native 36/60. 이는 구현된 휴리스틱이며 통계적 검정력 보장이 아니다. 기본 top score threshold 0.3 역시 p-value가 아니다. 부호는 `pearson_corr`를 봐야 한다.

출력은 `fred_lagged_correlation_scores`, `fred_top_lagged_correlations` 뷰, `fred_lagged_correlation_run_summary`다. 인과성, 조건부 관계, 예측 개선을 검정하지 않는다.

## 4. 주요 의사결정과 변경 이력

### 4.1 확정된 설계 방향

| 결정 | 이유 및 근거 |
|---|---|
| Medallion + Databricks/Delta/UC | 과제의 원본 보존·계보·서빙 분리 목표와 현재 notebook 구현. 다른 기술과의 정량 비교 기록은 없음 |
| revision-aware/current-only 분리 | 과거 시점에 실제 보인 값과 현재 복원된 과거값의 의미를 혼동하지 않기 위해 |
| seed와 generated metadata 분리 | 수동 routing과 외부 canonical metadata의 관리 책임 분리; 기존 handoff에 사용자 결정으로 기록 |
| 외부 bootstrap에 FRED metadata 연결 | Macrotrends/Yahoo의 단위·빈도 정보 부족 보완. 제공자가 같다는 뜻은 아님 |
| 03과 03b 분리 | feature mart와 상관 EDA의 목적 분리 |
| correlation screening 폐기 | 상관의 유무가 인과의 유무를 보장하지 않으므로 causal variable gate로 쓰지 않음 |
| 과거 04 제거 | 관측된 제한적 macro 변수만으로 causal sufficiency 등을 정당화할 수 없었음 |
| 문서는 명시적 요청 때 변경 | 기존 사용자 운영 원칙. 이번 문서 생성은 명시적으로 요청됨 |

### 4.2 실제 미커밋 개선 내용

- 01c/01d/01f에 canonical metadata, provider, period 정보 전달과 metadata 변경 merge 처리 추가.
- metadata builder 신규 추가, 01d/01f가 generated JSON을 읽도록 변경. `series_units_json`은 override 용도.
- 02c의 Bronze/target schema 보강, seed fallback, catalog/view까지 metadata 전파.
- Gold current branch가 units/frequency/seasonal_adjustment를 null 처리하던 부분 개선.
- Gold weekly aggregation 지원 및 correlation logic 분리.
- 03b 명칭·테이블을 lagged correlation으로 정리; score는 표본 신뢰도 가중 r² 사용.
- 두 Nasdaq sector seed 추가.
- legacy `03_gold_fred_asof_features_legacy_no_analysis_window.py` 삭제.
- README와 Bronze/Silver/Gold 문서 갱신, causal scope 및 metadata/03b 문서 신규 작성.

01c의 observation_start 빈 값 fallback은 현재 `1776-07-04`다. 예전 2010 시작 설정이나 잘못 전달된 변수명 변경 설명보다 실제 코드를 따른다.

### 4.3 과거 실험 기록의 해석

`docs/new_chat_handoff.md`에는 2026-07-08 PCMCI 실험 로그가 보존되어 있다: target NASDAQCOM, 변수 10개, 정렬 관측치 350개, 1997-01~2026-05, lag 1~6, 링크 600개 중 유의 링크 16개, target 링크 0개. 이는 **삭제된 구현의 과거 로그**이며 현재 모델 성능 또는 인과 부재의 증거가 아니다.

당시 complete-case 교집합은 표본을 줄였고 중간 월이 빠지면 행 lag와 달력 lag가 달라질 수 있었다. 03b는 pair별 join이므로 한 series의 짧은 이력이 모든 pair의 표본을 일괄 축소하지 않는다.

이전 세션에서 PDF 이미지 도구의 DPAPI 오류와 작업 경로 혼선이 있었다. 기존 handoff에는 Pearl 시각 검수가 미완료로 기록돼 있다. 현재 작업에서는 `학연생.png`를 실제로 열었으므로 과거 도구 오류가 지금도 모든 이미지에 발생한다고 단정하지 않는다.

## 5. 현재 상태와 반드시 알아야 할 제약

### 5.1 무엇을 완료로 볼 수 있는가

| 상태 | 내용 |
|---|---|
| 코드 구현 확인 | Bronze 6개, Silver 3개, Gold 2개, metadata builder 1개 |
| 설정 확인 | seed UTF-8 JSON 파싱 성공, 29개/23+6 routing |
| 문서·목표 확인 | README, 7개 docs, 과제 이미지, Git 상태와 주요 코드 대조 |
| 생성물 누락 확인 | 로컬 `fred_series_metadata.json` 없음 |
| 현재 실행 미검증 | FRED API 수집, Databricks E2E, Delta 실제 schema/행 수, 최신 audit, 대시보드, 스케줄 |
| 구현되지 않음 | 정상성·계절성·추세 진단 계층, transform 선택표, diagonal real-time panel, 현재 인과분석 모듈, 15개 커널/에이전트 API |

이번에는 notebook을 실행하지 않았다. “정상 작동하는 기능”으로 자신 있게 말할 수 있는 범위는 파일 접근·설정 파싱과 확인된 코드 경로이며 운영 성공을 보증하지 않는다. 런타임 오류가 없다는 결론도 낼 수 없다.

### 5.2 코드로 확인한 의미 불일치와 잠재적 결함

**1. 분석 구간 저장 identity 불일치 — 우선 확인 필요**

03의 `analysis_start_date/end_date`는 출력 컬럼과 hash ID에는 포함되지만 다음 merge key와 선행 delete에는 포함되지 않는다.

```text
asof observations: as_of_date, series_id, observation_date
period features:   as_of_date, target_frequency, aggregation_method, series_id, period_start
transformed:       위 period key + transform_type
snapshot:          as_of_date, target_frequency, aggregation_method, series_id
```

같은 as-of·빈도·집계·series로 다른 analysis window를 실행하면 period/transformed/snapshot의 이전 window 결과가 교체된다. 기존 문서의 “window가 feature identity”라는 표현을 여러 window 동시 보존 보장으로 읽으면 안 된다. 03b는 window를 key와 delete에 포함하므로 원천 Gold가 교체된 뒤에도 과거 correlation이 남을 수 있다. 여러 구간 보존 또는 최신 구간만 유지 중 운영 의도를 확정한 후 수정해야 한다.

**2. 행 lag와 달력 lag가 다름**

03은 calendar spine 없이 SQL `LAG`를 쓴다. 월별 결측이 있으면 lag 1이 한 달이 아닐 수 있다. 분기 GDP를 monthly로 집계해도 없는 달이 채워지지 않으므로 12행 lag가 12개월이라는 보장이 없다. 03b는 날짜 join이므로 두 모듈의 lag 의미가 자동 일치하지 않는다. 데이터 진단 전에 먼저 확인한다.

**3. 동일 이름 증가율의 100배 차이**

transformed의 `pct_change_1`은 `100 × (ratio−1)`, snapshot의 `pct_change_1`은 `(ratio−1)`다. 1% 변화를 각각 1과 0.01로 나타낸다. 이번 작업에서는 수정하지 않았다.

**4. 단일 as-of와 역사적 정보집합은 다름**

03은 선택된 as-of에서 과거 전체를 복원한다. 각 과거 의사결정 시점의 가용 정보를 자동 연결하는 diagonal panel은 아니다. ALFRED의 available_at도 현재 코드에서는 realtime/vintage 날짜에서 유래하며 초·분 단위 공식 발표시각을 보장하지 않는다.

**5. Current-only에 붙인 시간과 품질 결과**

Gold current branch의 available_at은 observation_date다. 원천에 남은 realtime 범위가 요청 시점을 반영할 수 있는데 Gold의 `visible_inside_realtime_window` 품질 규칙은 모든 history_type에 적용된다. 따라서 과거 as-of current-only 행이 이 규칙에서 실패할 가능성이 있다. 실제 실행 결과는 미검증이며 history_type별 판정을 검토할 필요가 있다.

**6. Latest view의 의미**

snapshot latest 뷰는 analysis_end_date까지 partition한다. 기본 end=as_of로 여러 날 실행하면 end가 매일 달라져 series당 전역 최신 한 행만 남는 뷰가 아니다. 03b top 뷰는 threshold 필터를 적용한 뒤 latest를 선택하므로 고정 window에서는 최신 결과가 threshold 미만일 때 과거 통과 행을 노출할 수 있다. 뷰의 실제 목적과 소비자 기대를 점검한다.

**7. Feature Store와 pipeline 원자성**

안내된 PK `(series_id, as_of_date)`는 여러 frequency/aggregation 행이 공존하면 충분하지 않을 수 있다. 실제 등록 전 유일성을 검증해야 한다. Gold의 delete 후 merge, 여러 테이블 갱신은 하나의 전역 트랜잭션이 아니므로 중간 실패 후 재실행·복구 검증이 필요하다. quality/run log는 append이므로 “재실행 멱등성”이 모든 log row 중복 부재를 뜻하지 않는다.

**8. 품질 통계의 시점 일관성**

Silver MAD/분포 통계가 처리 대상 이력에서 계산된다. 이를 과거 as-of의 필터로 사용할 때 미래 이력의 정보가 품질 선택에 영향을 줄 수 있는지 별도 점검해야 한다. Gold의 기본값은 warning 포함이지만 warning 제외 옵션과 outlier를 feature로 사용하는 경우 영향이 달라진다. 원본 vintage 복원이 모든 정제 판단의 실시간 재현까지 보장하지 않는다.

### 5.3 추가 확인 사항

- generated metadata 생성 후 01d/01f 경로·coverage와 역사행 backfill.
- provider 전환 시 원천 값·단위·close/adj_close 의미의 일치. metadata 부착만으로 값의 동등성이 검증되지는 않음.
- 두 Silver 경로에 동일 series/date가 겹칠 때 UNION ALL/merge 중복 위험.
- 운영 Delta에 과거 relationship/04 테이블이 남아 있는지 여부. 코드 삭제는 테이블 삭제가 아님.
- 수천 series 확장 시 API 호출량·driver collect·메모리·merge 성능. 현재 소규모 구현을 대규모 검증 완료로 간주하지 않음.
- 원 발행기관(BEA/BLS 등)과의 cross-source validation. 01e는 ALFRED와의 비교까지만 수행.
- 지표 정의·단위 변경의 historical semantics를 실제 분석 시점별로 보존하는지.

## 6. 최신 대화에서 이어갈 연구 맥락

### 6.1 사용자의 최근 관심

사용자는 정상성·추세·계절성을 어떻게 진단하고 제거 또는 분리할지 질문했고, 이미 Gold에 9개 transform이 저장된다는 점을 강조했다. 따라서 바로 다음 개발 후보는 **기존 transformed table을 입력으로 하는 진단과 연구별 표현 선택**이다. 변환을 다시 만드는 코드나 인과모형 구현은 아직 요청·완료되지 않았다.

대화에서 제안된 이름은 `fred_time_series_diagnostics`, `fred_causal_transform_selection`이다. 두 테이블은 현재 존재하는 구현이 아니라 설계 제안이다. 통화정책 surprise, 이벤트 연구, LP/IV, LPCMCI, real-time panel과 04a~04e 명칭도 제안일 뿐 채택·개발된 상태가 아니다.

### 6.2 대화의 통계적 표현을 그대로 구현하지 않을 것

앞선 설명 중 단정적인 표현은 다음처럼 제한해서 이해한다.

- 필요한 것은 선택한 방법의 가정을 진단하는 것이다. 모든 인과방법에 ADF/KPSS 통과가 일률적인 필수 관문인 것은 아니다.
- 정상성은 제거 대상이 아니다. 비정상성 원인을 처리하거나 적절한 모형으로 표현한다.
- ADF/KPSS의 귀무가설·상수/추세 사양·lag·표본 범위를 함께 저장한다. 기각 실패는 가설의 증명이 아니다.
- 계절성은 반드시 차분으로 제거해야 하는 것이 아니다. metadata 확인, 결정적 계절 통제, 계절 단위근 처리 등을 구분한다.
- `*_12`는 연속 월별 표본일 때만 1년 전 비교가 된다. 주별 52, 일별 252 같은 수를 calendar 정의 없이 일반 규칙으로 고정하지 않는다.
- z-score와 base100은 scale 재표현이며 단위근·추세를 제거하지 않는다.
- 공적분은 여러 수준계열의 장기 관계다. 단변량 진단 테이블만으로 확정할 수 없다.
- 차분·계절필터·평활화는 변수의 경제적 의미와 시간 의존성을 바꿀 수 있다. 잔차에 원래 수준변수의 causal graph를 그대로 적용하지 않는다.
- point-in-time 정보집합은 역사적 의사결정·예측 분석에 중요하다. 모든 회고적 인과분석에서 최종 수정 결과값 사용이 금지되는 것은 아니다. 정보변수와 결과 측정치를 구분해 vintage 정책을 정한다.
- 외부 surprise/IV는 가능한 식별전략이다. 모든 관측자료 인과효과 추정에 반드시 외생 충격 데이터가 필요하다는 뜻은 아니다.
- LPCMCI의 PAG 방향은 알고리즘 가정하의 ancestral 정보이며 곧바로 직접효과 크기가 아니다. bidirected 관계를 하나의 직접 숨은 원인으로 단순화하지 않는다.
- 정상성 통계·간선 안정성만으로 causal stationarity, Markov, faithfulness, 식별 가정을 증명할 수 없다.

### 6.3 진단 계층의 다음 설계 후보

입력 범위 식별에는 `as_of_date`, `analysis_start_date/end_date`, `target_frequency`, `aggregation_method`, `series_id`, `transform_type`, source Gold run/version을 포함한다.

권장 진단 순서:

1. 단위·원천 빈도·계절조정·history_type·available_at 의미 확인.
2. 일정한 시간축인지, calendar gap·중복·constant series·결측·변환 제외행이 있는지 점검.
3. 연구 해석에 맞는 후보 변환 지정. 모든 9개 중 p-value가 가장 좋은 것을 자동 선택하지 않음.
4. ADF/KPSS의 사양과 lag, 실제 사용 관측 수, 결과와 실패 이유를 저장.
5. 계절 ACF·계절 패턴·추세·rolling 분포·구조변화 진단을 함께 검토.
6. 공적분이 문제인 경우 관련 수준계열의 다변량 분석을 별도 수행.
7. 진단 결과를 `통과/실패` 하나로 축약하지 않고 `불확실/표본부족/미실행/오류` 상태도 보존.
8. 연구별 transform 선택 이유와 추정대상의 의미를 기록. 처리·결과·통제 역할을 구분.

후보: 주가지수는 log_diff_1, 가격/생산/고용 수준은 log_diff_1 또는 질문에 맞는 장기 모형, 금리·spread·실업률은 raw/change_1, UMCSENT/AWHMAN은 raw부터 검토. 이는 확정 rule이 아니다. 인과 변수의 포함·제외를 03b 점수로 결정하지 않는다.

### 6.4 논문 자료와 읽기 상태

현재 `논문/` 파일 12개:

1. `Pearl, “An Introduction to Causal Inference” (2010).pdf`
2. `ANCESTRAL GRAPH MARKOV MODELS.pdf`
3. `Causal Inference in the Presence of Latent Variables and Selection.pdf`
4. `On the completeness of orientation rules for causal discovery.pdf`
5. `Complete Causal Identification from Ancestral Graphs under Selection Bias.pdf`
6. `On Causal Discovery from Time Series Data using FCI.pdf`
7. `High-recall causal discovery for autocorrelated time.pdf`
8. `Discovering contemporaneous and lagged causal relations in autocorrelated.pdf`
9. `Detecting and quantifying causal associations in large.pdf`
10. `Supplementary Materials for Detecting and quantifying causal associations in large nonlinear time series datasets.pdf`
11. `Discovering cyclic causal models in psychological research.pdf`
12. `Causal_Inference_for_Time_Series_NREE.pdf`

이는 파일 목록이며 12개의 독립 연구 논문을 의미하지 않는다. supplementary가 포함된다. 앞의 순서는 기존 handoff의 읽기 계획에 신규 NREE를 더한 것으로 실제 전체 완료순서를 확정하는 기록은 아니다.

현재 대화에는 순차 완독 요청이 여러 번 남아 있으나 모든 논문에 대해 페이지별 완료 근거가 같은 수준으로 남아 있지는 않다. 기존 handoff의 Pearl 미완료 기록과 이후 독서 진행을 구분해야 한다. 다음 세션은 “모든 논문을 이미 학습했다”고 주장하지 말고 필요한 원문을 확인한다.

NREE는 현재 대화에서 46쪽 accepted manuscript 전체, 수식 1~7, Box 1~2, Table 1~2, Figure 1~4, 참고문헌 목록 157개, 공식 사례 노트북의 셀과 텍스트 출력을 읽었다고 보고됐다. 참고문헌 목록 확인은 인용된 157개 문헌 자체의 완독을 뜻하지 않는다. 공식 코드의 실행·수치 재현도 별도다.

NREE에서 이어갈 핵심 맥락: Question–Assumptions–Data(QAD), discovery와 effect estimation의 구별, 식별 가능성 먼저 판단, 조정집합과 매개변수 구분, 동일 자료의 그래프 선택 후 효과 추정에 따른 post-selection uncertainty, 시간 집계·숨은 교란·체제변화 검토.

이전 대화에서 확인한 출판 정보: Runge et al., Nature Reviews Earth & Environment 4, 487–505 (2023), DOI `10.1038/s43017-023-00431-y`. 정정문 `10.1038/s43017-023-00471-4`는 Figure 4i의 MLP-causal 곡선과 식 (2) 뒤 독립 기호 표기에 관한 것이다. accepted manuscript를 단순히 “정정 전”이라고 단정할 근거는 없다.

공식 재현 자료 주소: `https://github.com/jakobrunge/tigramite/tree/master/tutorials/case_studies`. 사용 시 현재 코드를 다시 검토하고 버전을 고정한다. 이 문서 작성 시 외부 자료를 다시 조회하거나 모델을 실행하지 않았다.

## 7. Git 상태와 작업 보존

이번 확인한 branch: `codex/integrated-asof-gold`.

HEAD: `ac882483d20259d73ca67755718dc4a92dcb553a`.

Origin: `https://github.com/sangwuny/Data_Lakehouse.git`.

최근 커밋:

| hash | 날짜 | 메시지 |
|---|---|---|
| ac88248 | 2026-07-06 | Complete integrated Gold layer implementation |
| 3c85301 | 2026-07-02 | add market risk indicators |
| 3426a9d | 2026-07-02 | integrate as-of gold feature mart |
| 4eed230 | 2026-06-30 | Add ALFRED reproducibility audit |
| 4383ee8 | 2026-06-30 | Initial commit |

문서 생성 전 working tree는 dirty 상태였다. tracked 차분은 11개 파일, 926줄 추가/1813줄 삭제다. 이는 기존 작업의 변경이며 이번 문서 작업의 구현량이 아니다.

변경 파일: README, docs의 Bronze/Silver/Gold 문서, 01c/01d/01f, seed, 03, 02c.

삭제 파일: `03_gold_fred_asof_features_legacy_no_analysis_window.py`.

미추적 파일: causality_scope/metadata/03b/new_chat_handoff의 4개 문서, metadata builder, 03b notebook, `tmp/`, `논문/`. 이번 `PROJECT_CONTEXT.md`는 신규 문서로 추가된다.

과거 04는 활성 .py 파일 목록에 없다. 미추적 상태에서 삭제됐다는 과거 기록이 있으므로 Git 삭제 차분이 없다고 활성 파일로 판단하지 않는다. 남은 .pyc를 근거로 복구하지 않는다.

.gitignore에는 .env, __pycache__/, *.pyc, .pytest_cache/, .ipynb_checkpoints/, data/가 포함된다. tmp/와 논문/는 일괄 제외되지 않는다. commit 대상은 별도로 확인한다. remote 최신 상태, push 범위, PR 상태는 이번에 확인하지 않았다.

## 8. 운영 재개 절차

### 8.1 초기 구축

1. Databricks Runtime, UC 권한, secret scope/key, 외부 API 연결, notebook/config 배치를 확인한다.
2. metadata builder를 실행해 필요한 모든 series의 JSON을 생성·배포한다.
3. Bronze 01a로 revision history를 구축한다.
4. 필요한 장기 이력을 01d(Macrotrends), 01f(Yahoo)로 bootstrap한다.
5. 01c로 current-only를 갱신한다.
6. 01e로 대표 series와 여러 as-of를 감사한다.
7. Silver 02a와 02c를 실행한다.
8. Gold 03을 명시한 as-of/window/frequency/aggregation으로 실행한다.
9. 필요하면 같은 설정으로 03b를 실행한다.

### 8.2 지속 갱신

metadata 변경 시 builder → 01b/01c → 필요한 01e 감사 → 02b/02c → 03 → 선택적 03b.

01d/01f는 bootstrap 도구이며 매번 실행하는 전제가 아니다. metadata 컬럼 추가만으로 역사행이 채워지지 않으므로 해당 provider loader → 02c → 03 재실행을 확인한다. 전체 table drop을 자동 복구 절차로 삼지 않는다.

### 8.3 운영 검증

기존 handoff 18~19장에 구체적인 SQL이 있다. 예제 날짜 2026-07-07은 실제 대상 날짜로 바꾼다.

- Bronze current의 키 중복, provider별 범위, units/frequency 누락.
- Silver quality_status별 건수, revision usability와 current-only 안전성 표기.
- Gold key/value null, metadata 전파, transform별 행 수와 최초·최종 period.
- 01e의 실제 일치/불일치와 점검 범위.
- 03b pair_count, 날짜 정렬, raw-level 추세, window 일치.
- 실패 시 run ID, 설정, 실제 범위, 예외 기록.

모든 컬럼에 non-null을 요구하지 않는다. 빈 analysis_start, current-only revision 번호, 초기 lag 등은 정상적으로 null일 수 있다.

## 9. 우선순위별 TODO

다음은 권장 순서이며 모든 항목이 이미 사용자에게 구현 승인됐다는 뜻은 아니다.

### P0: 작업 환경과 데이터 계약

- [ ] 실제 소스 저장소를 workspace로 설정하고 기존 dirty 변경을 보존한다.
- [ ] generated metadata 생성 환경·배포 정책을 확인한다.
- [ ] Databricks에서 최신 코드의 E2E 및 metadata backfill을 확인한다.
- [ ] Gold window 저장 키와 delete 범위의 의도를 확정한다.
- [ ] calendar gap, 혼합빈도, missing/partial period를 감사한다.
- [ ] pct_change scale 차이와 Feature Store 키를 소비자 요구와 대조한다.

### P1: 최신 논의에 해당하는 진단 계층

- [ ] 기존 transformed table을 입력으로 하는 진단 스키마를 확정한다.
- [ ] 결측·불규칙성·constant·표본 부족을 먼저 판정한다.
- [ ] ADF/KPSS, 계절성, 추세, 구조변화 진단 사양을 정한다.
- [ ] 통계량뿐 아니라 귀무가설, deterministic term, lag, 실효 표본, 오류, 버전을 기록한다.
- [ ] 후보 변환과 연구별 선택 이유를 관리한다. 최소 p-value 자동 선택을 피한다.
- [ ] series별 transform mapping 필요성을 검토한다. 03b의 공통 transform과 구분한다.
- [ ] 합성 시계열과 알려진 사례로 진단·시간 정렬을 검증한다.

### P2: 인과 연구 설계

- [ ] 질문, 처치, 결과, horizon, 정보시점, 결과 vintage, 조정집합, 잠재 교란을 정의한다.
- [ ] QAD와 temporal DAG/ADMG/PAG를 바탕으로 식별 가능성을 판단한다.
- [ ] 필요한 경우 real-time panel, 외부 policy surprise, 발표 timestamp를 추가한다.
- [ ] LPCMCI 탐색, domain graph 기반 조정, IV/LP, 이벤트 연구 등을 질문에 맞게 선택한다.
- [ ] placebo, 사전 반응, 체제별 비교, lag/변환 감도, 선택 후 추론을 계획한다.
- [ ] 동일 변수의 raw/차분/성장률, 구성 금리와 spread의 결정적 중복을 검토한다.

### P3: 연구 목표 확장과 운영

- [ ] 여러 기관·수천 series로 확장할 수집 성능과 metadata 관리를 설계한다.
- [ ] 지표 정의 변경, 원 발행기관 대조, 시점별 품질 판단을 정비한다.
- [ ] kernel registry, 에이전트 호출 계약, 15종 이상의 평가 기준을 정의한다.
- [ ] Runtime/의존 버전, 의미 있는 테스트, Job 배포 설정, CI를 관리한다.
- [ ] 논문/tmp/generated JSON의 Git 정책과 기존 변경의 commit 단위를 정한다.
- [ ] 과거 Delta object의 archive/drop은 실제 존재와 의존성을 확인해 별도로 판단한다.

## 10. Codex 작업 인수인계

먼저 이 문서의 0·5·6장을 읽고 README, docs/causality_scope.md, 관련 layer 문서, 실제 코드를 확인한다. 과거 작업 순서가 필요하면 docs/new_chat_handoff.md를 읽는다. 문서와 코드가 다르면 실제 동작을 확인하고 차이를 보고한다.

최신 대화를 이어갈 경우 첫 작업 후보는 기존 9개 변환의 진단 사양과 입력 시간축 감사다. 진단 테이블·인과모형은 아직 구현되지 않았으며, 이 문서는 구현 요청을 대신하지 않는다.

유지해야 할 원칙:

1. 기존 dirty 변경을 reset/checkout/clean하지 않는다.
2. 03b를 causal screening이라고 부르지 않고 순위를 인과 변수 선택의 근거로 삼지 않는다.
3. 폐기한 04를 새로운 연구 설계 없이 복구하지 않는다.
4. current-only를 ALFRED의 역사적 point-in-time 안전성과 동일시하지 않는다.
5. 단위 metadata, scale 변환, 정상성, 인과 식별을 혼동하지 않는다.
6. 문서는 명시적 요청이 있을 때 수정한다. 이번 요청은 이 문서 생성에 해당한다.
7. 논문 파일 존재, 텍스트 추출, 시각 검수, 재현 실행을 구별한다.
8. API/Databricks/추정을 실행하지 않았다면 정상 동작·검정 완료·효과 확인이라고 보고하지 않는다.
9. 이 문서에 절차가 있다는 이유만으로 과거 테이블 삭제, 외부 변경, Git 공개를 수행하지 않는다.

새 세션 시작문 예시:

```text
PROJECT_CONTEXT.md를 읽고 실제 소스 저장소와 현재 workspace를 구분해 주세요.
기존 미커밋 변경을 보존하고 README, causality_scope, Gold 03 코드를 확인해 주세요.
현재는 기존 Gold의 9개 변환을 이용한 정상성·추세·계절성 진단 설계를 논의 중이며,
진단 테이블이나 인과모형은 아직 구현하지 않았습니다.
analysis window 저장 키, 행 lag와 달력 lag, pct_change scale 차이를 고려해 이어가 주세요.
```

## 11. 이번 확인 범위

- 실제 파일 목록, README와 docs, seed, metadata builder, Gold 처리와 Bronze/Silver의 관련 설정·핵심 처리를 확인했다.
- 학연생.png를 직접 열어 연구 목표와 현재 구현 범위를 구분했다.
- Git branch/HEAD/status/log/diff, seed JSON 파싱, generated metadata 부재, 활성 진단·인과 코드 부재를 확인했다.
- PDF/CSV 전체 재독, 외부 API 접속, Databricks 실행, 통계 검정, CI 실행은 이번 문서 작업에서 수행하지 않았다.
- 기존 코드·설정·README·docs 수정, 데이터 삭제, commit/push는 수행하지 않았다.

이 문서는 확인 시점의 기록이다. 다음 작업에서는 현재 Git 상태와 대상 코드 변경분을 다시 확인한다.
