# Bronze 계층 문서

이 문서는 FRED/ALFRED 데이터 Lakehouse의 Bronze 계층 설계를 설명한다. 기준 언어는 한국어이며, 현재 저장소의 Databricks notebook, Delta Lake, Unity Catalog 기반 구조만 다룬다.

## 1. Bronze 계층의 목적

Bronze 계층은 외부 소스인 FRED/ALFRED API에서 받은 데이터를 Lakehouse 내부에 가장 원본에 가깝게 보존하는 계층이다. 이 프로젝트의 Bronze는 단순 최신값 저장소가 아니라, 경제지표의 발표와 사후 개정 이력을 보존하기 위한 revision-aware 원천 계층이다.

Bronze에서 보존하는 핵심 정보는 다음과 같다.

- API endpoint, 요청 파라미터, 응답 payload
- 관측값의 원본 문자열 값
- 관측값이 유효했던 `realtime_start`, `realtime_end`
- ALFRED `vintage_date`
- 수집 run ID, 수집 날짜/시간, request hash, payload hash
- API 호출 성공, 실패, skipped 이력
- 동일 payload, metadata, observation version의 재확인 횟수

핵심 목표는 다음과 같다.

```text
1. 원본 API 응답을 재현 가능하게 보존한다.
2. observation version을 중복 없이 관리한다.
3. 수집 실행 이력을 append-only로 남긴다.
4. Silver/Gold에서 point-in-time 분석을 수행할 수 있는 기반을 제공한다.
```

## 2. 현재 설계 방향

초기에는 매 calendar date마다 전체 관측값 snapshot을 저장하는 physical daily snapshot 구조를 실험했다. 하지만 이 방식은 2010년 이후 모든 날짜와 모든 series를 조회해야 하므로 API 호출 횟수, 실행 시간, 저장 비용이 빠르게 커진다.

현재 구조는 physical daily snapshot이 아니라 **vintage-date 기반 revision-aware Bronze**이다.

- ALFRED revision history가 있는 series는 `realtime_start`, `realtime_end`, `vintage_date`를 보존하는 observation version table에 저장한다.
- 같은 observation version이 다시 수집되면 새 row를 만들지 않고 `seen_count`, `last_seen_bronze_run_id`, `last_collected_at_utc`만 갱신한다.
- incremental 적재는 watermark와 ALFRED vintage date를 이용해 새 개정이 있을 가능성이 있는 구간만 조회한다.
- ALFRED revision history가 없는 FRED-only series는 version table에 섞지 않고 별도 current table에 저장한다.

이 설계는 daily snapshot보다 훨씬 적은 API 호출로 revision-aware 분석 기반을 만든다.

## 3. Repository 구조

Databricks 관련 파일은 Medallion 계층별로 관리한다.

```text
notebooks/databricks/
  configs/
    fred_seed_series.json
  bronze/
    01a_bronze_fred_bootstrap_versions.py
    01b_bronze_fred_incremental_versions.py
    01c_bronze_fred_current_observations.py
    01e_bronze_alfred_reproducibility_audit.py
  silver/
    02a_silver_fred_bootstrap_versions.py
    02b_silver_fred_incremental_versions.py
    02c_silver_fred_current_observations.py
  gold/
    03a_gold_fred_bootstrap_causal_features.py
    03b_gold_fred_incremental_causal_features.py
    03c_gold_fred_current_indicators.py
```

현재 운영의 중심은 `bronze/01a`, `bronze/01b`, `bronze/01c`, `bronze/01e`, `silver/02a`, `silver/02b`, `silver/02c`, `gold/03a`, `gold/03b`, `gold/03c`이다. 이전 로컬 Python 실행 코드, 과거 snapshot 실험 코드, 예전 단일 Bronze/Silver/Gold notebook은 더 이상 현재 운영 기준이 아니다.

## 4. Seed catalog

Seed catalog는 다음 위치에 있다.

```text
notebooks/databricks/configs/fred_seed_series.json
```

현재 seed catalog는 연구 목적상 핵심 macro/financial 지표 20개로 압축되어 있다.

주요 필드는 다음과 같다.

| 필드 | 설명 |
|---|---|
| `rank` | 현재 프로젝트 내 우선순위 |
| `series_id` | FRED/ALFRED series ID |
| `domain` | 경제 영역. 예: labor, inflation, rates |
| `priority` | 수집 우선순위. 현재 20개는 모두 `core` |
| `expected_frequency` | 기대 빈도. 예: daily, weekly, monthly, quarterly |
| `description` | 지표명 |
| `role` | 프로젝트 안에서의 핵심 역할 |
| `alfred_available` | ALFRED revision history 사용 가능 여부 |

`alfred_available` 라우팅 규칙은 다음과 같다.

```text
alfred_available = true  -> bronze/01a, bronze/01b
alfred_available = false -> bronze/01c
```

`series_ids = ALL`로 실행하면 `01a`와 `01b`는 ALFRED-capable series만 선택하고, `01c`는 FRED-only series만 선택한다. FRED-only series를 `01a` 또는 `01b`에 명시적으로 넘기면 ALFRED API 오류가 나기 전에 명확한 에러를 발생시킨다.

현재 20개 seed series와 Bronze 적재 경로는 다음과 같다.

| 우선순위 | Series ID | 빈도 | 핵심 역할 | Bronze 적재 경로 |
|---:|---|---|---|---|
| 1 | `GDPC1` | quarterly | 실질 경제성장 | ALFRED version |
| 2 | `PCE` | monthly | 소비 수요 | ALFRED version |
| 3 | `W875RX1` | monthly | 실질 소득 | FRED current |
| 4 | `PAYEMS` | monthly | 고용 규모 | ALFRED version |
| 5 | `UNRATE` | monthly | 노동시장 상태 | ALFRED version |
| 6 | `ICSA` | weekly | 노동시장 선행 신호 | ALFRED version |
| 7 | `AWHMAN` | monthly | 제조업 경기 선행 | ALFRED version |
| 8 | `INDPRO` | monthly | 생산 활동 | ALFRED version |
| 9 | `CMRMTSPL` | monthly | 실물 판매 | FRED current |
| 10 | `CPIAUCSL` | monthly | 대표 물가 | ALFRED version |
| 11 | `PCEPILFE` | monthly | 기조 인플레이션 | ALFRED version |
| 12 | `FEDFUNDS` | monthly | 통화정책 | ALFRED version |
| 13 | `GS10` | monthly | 장기금리 | ALFRED version |
| 14 | `T10YFFM` | monthly | 장단기 금리 spread | FRED current |
| 15 | `M2SL` | monthly | 통화량 | ALFRED version |
| 16 | `BAMLH0A0HYM2` | daily | 신용위험 | FRED current |
| 17 | `SP500` | daily | 금융시장 / 자산가격 | FRED current |
| 18 | `PERMIT` | monthly | 주택 경기 선행 | ALFRED version |
| 19 | `NEWORDER` | monthly | 기업투자 선행 | ALFRED version |
| 20 | `UMCSENT` | monthly | 소비자 심리 | ALFRED version |

FRED-only series는 현재 다음 5개이다.

```text
W875RX1, CMRMTSPL, T10YFFM, BAMLH0A0HYM2, SP500
```

## 5. 저장 위치와 테이블 구조

기본 저장 위치는 다음과 같다.

```text
Catalog: fred_lakehouse
Schema : bronze
Format : Delta table
```

Bronze 주요 테이블은 다음과 같다.

```text
fred_lakehouse.bronze
├── fred_raw_response_payloads
├── fred_ingestion_runs
├── fred_series_metadata_versions
├── fred_observation_versions
├── fred_vintage_dates_seen
├── fred_incremental_watermarks
├── fred_current_observations_raw
└── fred_run_summary
```

## 6. Bronze notebook 역할

### 6.1 `bronze/01a_bronze_fred_bootstrap_versions.py`

ALFRED-capable series의 최초 전체 revision history를 적재한다. 한 번의 초기 구축 작업 또는 큰 범위 재구축 작업에 사용한다.

권장 실행 파라미터는 다음과 같다.

```text
catalog = fred_lakehouse
series_ids = ALL
include_vintages = true
realtime_start = 1776-07-04
realtime_end = 9999-12-31
output_type = 1
seed_catalog_path = ../configs/fred_seed_series.json
```

주요 동작은 다음과 같다.

- seed catalog에서 ALFRED-capable series만 선택
- series metadata 조회
- ALFRED vintage dates 조회
- observation API 조회
- observation version 후보 생성
- Delta `MERGE`로 중복 없는 version table 적재
- raw response, metadata, vintage dates, ingestion run, run summary 기록

### 6.2 `bronze/01b_bronze_fred_incremental_versions.py`

ALFRED-capable series의 증분 revision history를 적재한다. Lakeflow Jobs에서 주기적으로 실행하는 Bronze 증분 작업이다.

권장 실행 파라미터는 다음과 같다.

```text
catalog = fred_lakehouse
series_ids = ALL
lookback_days = 30
overlap_days = 3
output_type = 1
seed_catalog_path = ../configs/fred_seed_series.json
```

주요 동작은 다음과 같다.

- `fred_incremental_watermarks`에서 series별 마지막 성공 real-time end 조회
- overlap을 둔 다음 request window 계산
- `/fred/series/vintagedates`로 새 vintage date 존재 여부 확인
- 새 vintage date가 없으면 observation 재조회 없이 skipped 처리하고 watermark 갱신
- 새 vintage date가 있으면 observation API 호출 후 새 observation version만 `MERGE`

### 6.3 `bronze/01c_bronze_fred_current_observations.py`

ALFRED revision history가 없는 FRED-only series를 적재한다. 현재 seed catalog 기준으로 `W875RX1`, `CMRMTSPL`, `T10YFFM`, `BAMLH0A0HYM2`, `SP500`가 이 경로를 사용한다.

권장 실행 파라미터는 다음과 같다.

```text
catalog = fred_lakehouse
bronze_schema = bronze
series_ids = ALL
observation_start = 2010-01-01
observation_end =
sleep_seconds = 0
retry_sleep_seconds = 0.05,0.1,0.5
seed_catalog_path = ../configs/fred_seed_series.json
```

이 notebook은 point-in-time version을 만들지 않는다. FRED에서 현재 제공하는 관측값을 `fred_current_observations_raw`에 저장한다. 중복 key는 다음과 같다.

```text
source, series_id, observation_date
```

따라서 같은 observation date가 이미 저장되어 있으면 반복 실행해도 같은 값을 계속 append하지 않는다. 다만 FRED current 값 또는 FRED 응답의 real-time 범위가 바뀐 경우에는 동일 key row를 최신 값으로 update한다.

### 6.4 `bronze/01e_bronze_alfred_reproducibility_audit.py`

내부 point-in-time 복원값이 ALFRED의 특정 vintage 응답과 일치하는지 외부 대조한다. 이 notebook은 ALFRED API에 `vintage_dates = as_of_date`를 넣어 직접 받은 값과, Bronze `fred_observation_versions`에서 `realtime_start <= as_of_date <= realtime_end` 조건으로 복원한 값을 비교한다.

권장 실행 파라미터 예시는 다음과 같다.

```text
catalog = fred_lakehouse
bronze_schema = bronze
series_ids = GDPC1,UNRATE,CPIAUCSL
as_of_dates = 2015-01-01,2020-04-01,2024-01-01
observation_start = 2010-01-01
observation_end =
max_observations_per_series = 0
fail_on_mismatch = false
```

결과는 별도 Delta table에 저장하지 않고 notebook 화면에 summary와 mismatch row로 표시하며, 마지막에 JSON summary를 반환한다. `match_status <> 'match'`가 존재하면 내부 재현 로직과 ALFRED 응답이 일치하지 않는다는 의미이므로 원인을 확인해야 한다.

## 7. Databricks 기능 사용 방식

Bronze 계층은 Databricks-native 기능을 중심으로 구성한다.

| 구분 | 사용 기능 | 사용 이유 |
|---|---|---|
| Catalog / Schema | `CREATE CATALOG`, `CREATE SCHEMA`, `USE CATALOG`, `USE SCHEMA` | `fred_lakehouse.bronze` 네임스페이스에서 테이블을 일관되게 관리 |
| Delta Lake | `CREATE TABLE ... USING DELTA` | ACID transaction, schema 관리, SQL 조회, MERGE 지원 |
| Delta MERGE | `MERGE INTO` | 동일 observation version, payload, metadata 중복 방지 |
| Append write | `.write.format("delta").mode("append")` | 실행 로그와 run summary를 append-only로 보존 |
| Spark SQL | `spark.sql(...)` | 테이블 생성, MERGE, 검증 쿼리 수행 |
| Spark DataFrame | `spark.createDataFrame(...)` | API 응답을 구조화된 row로 변환 |
| Temporary View | `createOrReplaceTempView(...)` | Python row batch를 SQL MERGE source로 사용 |
| Databricks Widgets | `dbutils.widgets.*` | notebook 파라미터화, Jobs 실행 지원 |
| Databricks Secrets | `dbutils.secrets.get(...)` | FRED API key 보호 |
| Lakeflow Jobs | Jobs workflow | Bronze/Silver/Gold 순차 실행 자동화 |

Bronze에서 가장 중요한 기능은 Delta Lake와 Delta MERGE이다. 이 둘을 사용하기 때문에 같은 데이터를 다시 확인하더라도 observation version row가 무한히 증가하지 않는다.

## 8. 테이블별 역할

### 8.1 `fred_observation_versions`

ALFRED-capable series의 canonical observation version table이다.

하나의 row는 특정 `series_id`, `observation_date`, `value_raw`, `realtime_start`, `realtime_end` 조합으로 정의되는 하나의 값 버전을 의미한다.

대표 컬럼은 다음과 같다.

| 컬럼 | 설명 |
|---|---|
| `observation_version_id` | observation version 고유 ID |
| `value_hash` | 값 버전 식별 hash |
| `series_id` | FRED/ALFRED series ID |
| `domain` | seed catalog의 경제 영역 |
| `priority` | seed catalog의 수집 우선순위 |
| `observation_date` | 관측 대상 날짜 |
| `period_start_inferred` | 빈도 기반 추정 기간 시작일 |
| `period_end_inferred` | 빈도 기반 추정 기간 종료일 |
| `period_inference_basis` | 기간 추정 근거 |
| `value_raw` | API에서 받은 원본 값 |
| `realtime_start` | 해당 값이 알려지기 시작한 real-time date |
| `realtime_end` | 해당 값이 유효한 마지막 real-time date |
| `vintage_date` | ALFRED vintage date |
| `available_at` | 분석 시점에서 값이 사용 가능해진 날짜 |
| `frequency`, `frequency_short` | FRED metadata의 빈도 |
| `units`, `units_short` | FRED metadata의 단위 |
| `seasonal_adjustment` | 계절조정 여부 |
| `request_params_hash` | API 요청 파라미터 hash |
| `first_seen_bronze_run_id` | 처음 발견된 Bronze run ID |
| `first_collected_at_utc` | 처음 수집된 UTC 시각 |
| `last_seen_bronze_run_id` | 마지막으로 재확인된 Bronze run ID |
| `last_collected_at_utc` | 마지막으로 재확인된 UTC 시각 |
| `seen_count` | 같은 version이 확인된 횟수 |

병합 key는 `observation_version_id`이다.

### 8.2 `fred_raw_response_payloads`

FRED/ALFRED API의 원본 JSON 응답을 보존한다. 분석용 정규화 테이블이 아니라, 재현성과 감사 목적의 raw payload archive이다.

병합 key는 `payload_hash`이다. 동일 payload가 다시 수집되면 `seen_count`와 마지막 확인 메타데이터만 갱신한다.

### 8.3 `fred_ingestion_runs`

API 호출 단위의 실행 로그이다. 요청 endpoint, 파라미터, 응답 수, 상태, 오류 메시지, 시작/종료 시각을 기록한다.

이 테이블은 실행 이력 자체가 중요하므로 append-only로 관리한다.

### 8.4 `fred_series_metadata_versions`

series metadata의 version table이다. 제목, 빈도, 단위, 계절조정 여부, 관측 시작/종료일, notes 등을 저장한다.

병합 key는 `metadata_hash`이다. 동일 metadata가 다시 수집되면 `seen_count`와 마지막 확인 메타데이터만 갱신한다.

### 8.5 `fred_vintage_dates_seen`

series별 ALFRED vintage date 목록을 저장한다. bootstrap에서 전체 vintage date를 확인하고, incremental에서는 새 vintage date가 있는지 판단하는 근거가 된다.

병합 key는 다음과 같다.

```text
source, series_id, vintage_date
```

### 8.6 `fred_incremental_watermarks`

incremental 수집 상태를 저장한다. series별 마지막 성공 `realtime_end`를 기록하여 다음 실행에서 전체 과거를 다시 스캔하지 않도록 한다.

병합 key는 다음과 같다.

```text
source, series_id
```

### 8.7 `fred_current_observations_raw`

ALFRED revision history가 없는 FRED-only series의 현재 관측값을 저장한다. 이 테이블은 point-in-time safe하지 않으며, revision-aware observation version table과 의도적으로 분리한다.

주요 컬럼은 다음과 같다.

| 컬럼 | 설명 |
|---|---|
| `realtime_start`, `realtime_end` | FRED 응답에 포함된 real-time 범위. FRED-only current table에서는 개정 이력 판단용으로 사용하지 않는다. |
| `observation_date` | 관측 대상 날짜 |
| `value_raw` | API에서 받은 원본 값 |
| `source` | 현재 `fred` |
| `series_id` | FRED series ID |
| `endpoint` | 호출 endpoint |
| `request_params_json` | API 요청 파라미터 JSON |
| `request_params_hash` | 요청 파라미터 hash |
| `redacted_url` | API key를 숨긴 요청 URL |
| `bronze_run_id` | 수집 run ID |
| `collection_date` | 수집 날짜 |
| `collected_at_utc` | 수집 UTC 시각 |
| `load_type` | 현재 `fred_current_observations` |
| `attempts` | API request 재시도 후 성공 attempt 수 |

병합 key는 다음과 같다. 이 테이블은 FRED current 최신 상태 mirror로 사용하므로, 같은 key에서 `value_raw`, `realtime_start`, `realtime_end`가 달라지면 기존 row를 update한다.

```text
source, series_id, observation_date
```

### 8.8 `fred_run_summary`

notebook 실행 단위 요약 테이블이다. 처리 대상 series 수, 성공/실패 수, 반환 observation 수, 시작/종료 시각 등을 기록한다.

이 테이블도 실행 이력 보존 목적이므로 append-only로 관리한다.

## 9. Point-in-time 분석과 Bronze

이 프로젝트의 핵심은 특정 시점에 실제로 알 수 있었던 정보만 사용해 분석하는 것이다. 예를 들어 2023-01-01에 분석을 수행한다면, 2024년에 개정된 GDP 값을 사용하면 미래 정보 누수가 발생한다.

Bronze는 이를 막기 위해 다음 필드를 보존한다.

```text
observation_date: 어떤 기간의 값인가?
realtime_start  : 언제부터 이 값이 알려졌는가?
realtime_end    : 언제까지 이 값이 유효했는가?
vintage_date    : 어떤 발표/개정 날짜에 해당하는가?
available_at    : 분석 시점에서 사용 가능해진 날짜는 언제인가?
```

Silver/Gold에서는 일반적으로 다음 조건으로 특정 `as_of_date`에서 사용 가능했던 값을 복원한다.

```sql
realtime_start <= as_of_date
AND realtime_end >= as_of_date
AND observation_date <= as_of_date
AND available_at <= as_of_date
```

이 구조 덕분에 Lakehouse는 단순한 경제지표 저장소가 아니라, 인과 분석 재현성을 고려한 revision-aware 데이터 기반이 된다.

단, `fred_current_observations_raw`는 ALFRED revision history가 없는 FRED-only current 데이터이므로 point-in-time safe한 revision table로 해석하지 않는다. Gold에서 엄격한 인과 분석에 사용할 때는 이 차이를 명시적으로 고려해야 한다.

## 10. 중복 방지와 재실행 안정성

Bronze는 재실행을 전제로 설계한다. 같은 notebook을 다시 실행하더라도 핵심 version table이 무한히 늘어나지 않도록 Delta `MERGE`를 사용한다.

| 테이블 | 중복 방지 방식 |
|---|---|
| `fred_raw_response_payloads` | `payload_hash` 기준 MERGE |
| `fred_series_metadata_versions` | `metadata_hash` 기준 MERGE |
| `fred_observation_versions` | `observation_version_id` 기준 MERGE |
| `fred_vintage_dates_seen` | `source`, `series_id`, `vintage_date` 기준 MERGE |
| `fred_incremental_watermarks` | `source`, `series_id` 기준 MERGE |
| `fred_current_observations_raw` | `source`, `series_id`, `observation_date` 기준 MERGE, current 값 변경 시 UPDATE |
| `fred_ingestion_runs` | append-only |
| `fred_run_summary` | append-only |

`MERGE` 대상 테이블은 동일 데이터가 다시 들어오면 `seen_count`와 마지막 확인 관련 메타데이터를 갱신한다. `fred_current_observations_raw`는 `seen_count`가 없는 current mirror이므로 동일 key의 current 값이 바뀐 경우 최신 row로 갱신한다. 로그성 테이블은 실행 이력 자체가 의미 있으므로 append-only로 남긴다.

## 11. API 호출 속도와 retry 전략

FRED API 호출은 너무 촘촘하면 일시적 HTTP 오류나 네트워크 오류가 발생할 수 있다. Bronze notebook은 안정성과 실행 시간을 균형 있게 가져간다.

- `01a`, `01b`는 `sleep_seconds`로 series 간 호출 간격을 조절한다.
- `01c`는 기본 `sleep_seconds = 0`으로 빠르게 실행하되, 실패 시 `retry_sleep_seconds = 0.05,0.1,0.5` 순서로 더 긴 대기 시간을 적용한다.
- API 호출 자체도 `fred_get` 내부에서 재시도한다.

대량 bootstrap에서는 안정성을 위해 `sleep_seconds`를 너무 공격적으로 낮추지 않는 것이 좋고, FRED-only current 적재처럼 호출량이 상대적으로 작으면 `sleep_seconds = 0`으로 시작해도 된다.

## 12. Bronze 재현성 검증 기준

교수님 피드백의 핵심은 단순히 Bronze table이 잘 생성되었는지가 아니라, 우리가 구현한 point-in-time 재현 로직이 실제 ALFRED 특정 시점 응답을 정확히 복원하는지 확인하는 것이다. 이 프로젝트에서는 `bronze/01e_bronze_alfred_reproducibility_audit.py`를 Bronze 재현성 검증 단계로 둔다.

검증 관점은 다음과 같다.

| 관점 | 확인 내용 |
|---|---|
| 내부 복원값 | `fred_observation_versions`에서 `realtime_start <= as_of_date <= realtime_end`, `available_at <= as_of_date` 조건으로 특정 시점 값을 복원 |
| 외부 기준값 | ALFRED API에 `vintage_dates = as_of_date`를 넣어 같은 시점의 값을 직접 조회 |
| row-level 대조 | `series_id + as_of_date + observation_date` 단위로 내부값과 외부값 비교 |
| mismatch 판정 | `value_mismatch`, `missing_internal`, `missing_external` 여부 확인 |

검증 결과는 별도 table에 저장하지 않고 notebook output과 JSON summary로 반환한다.

따라서 Bronze 적재 후에는 사용자가 선택한 `series_ids`, `as_of_dates`에 대해 `01e`를 실행하고 summary의 `values_match = true`, `mismatch_count = 0`인지 확인해야 한다.

### 12.1 ALFRED 재현성 대조와 원 기관 대조의 차이

`01e`는 ALFRED API를 기준 truth로 삼는다. 즉 “ALFRED에서 특정 날짜의 vintage 값을 다시 요청했을 때”와 “우리 Bronze table의 `realtime_start`, `realtime_end`, `available_at` 조건으로 복원한 값”이 일치하는지를 검증한다.

원 기관 발표값과의 직접 대조는 별도 검증 단계가 필요하다. 예를 들어 `GDPC1`은 BEA, `CPIAUCSL`과 `UNRATE`는 BLS, `GS10`과 `M2SL`은 Federal Reserve 계열 원천을 확인해야 한다. 기관별 API, series code, release table, 단위, 계절조정 방식이 서로 다르므로 다음과 같은 별도 mapping이 있어야 한다.

```text
fred_series_id -> official_source -> official_dataset/table/series_code -> unit/frequency adjustment rule
```

현재 Bronze의 재현성 검증 범위는 ALFRED/FRED가 제공한 vintage history를 정확히 보존하고 복원하는지까지이다. 원 기관 직접 대조는 FRED/ALFRED 외부의 cross-source validation으로 분리해서 구현하는 것이 맞다.

## 13. 운영 순서

권장 실행 순서는 다음과 같다.

```text
1. bronze/01a를 한 번 실행해 ALFRED-capable 전체 history를 적재한다.
2. bronze/01c를 실행해 FRED-only current series를 별도로 적재한다.
3. bronze/01e를 실행해 대표 series/as_of_date의 ALFRED 재현성을 대조한다.
4. silver/02a를 한 번 실행해 Silver versioned table을 만든다.
5. silver/02c를 실행해 FRED-only current table을 정제한다.
6. gold/03a로 초기 분석용 feature mart를 생성한다.
7. gold/03c로 최신 통합 dashboard mart를 생성한다.
8. 이후 bronze/01b를 Lakeflow Jobs로 주기 실행한다.
9. 필요 시 bronze/01e로 표본 as-of date 재현성을 대조한다.
10. silver/02b를 실행해 변경된 ALFRED series만 정제한다.
11. bronze/01c와 silver/02c로 FRED-only current series를 갱신한다.
12. gold/03b로 증분 feature mart를 갱신한다.
13. gold/03c로 최신 통합 dashboard mart를 갱신한다.
```

일반적인 daily workflow는 다음과 같이 구성할 수 있다.

```text
bronze/01b_bronze_fred_incremental_versions.py
-> bronze/01e_bronze_alfred_reproducibility_audit.py
-> silver/02b_silver_fred_incremental_versions.py
-> gold/03b_gold_fred_incremental_causal_features.py

bronze/01c_bronze_fred_current_observations.py
-> silver/02c_silver_fred_current_observations.py
-> gold/03c_gold_fred_current_indicators.py
```

FRED-only current series는 별도 workflow 또는 같은 Job의 별도 task로 `bronze/01c`, `silver/02c`, `gold/03c`를 순서대로 실행하면 된다.

## 14. 요약

Bronze 계층은 다음 역할을 수행한다.

| 역할 | 설명 |
|---|---|
| 원본 보존 | API response JSON과 raw value를 보존 |
| 실행 기록 | 요청 파라미터, 응답 수, 실패 메시지, 재시도 횟수 기록 |
| 개정 이력 저장 | ALFRED-capable observation version을 real-time range 기준으로 저장 |
| 중복 방지 | 동일 payload, metadata, observation version은 `MERGE`로 갱신 |
| 증분 수집 지원 | series별 watermark와 vintage date 확인으로 효율적 증분 적재 지원 |
| FRED-only 분리 | ALFRED revision history가 없는 series는 별도 current table에 저장 |
| 계보 추적 기반 제공 | Silver/Gold에서 어떤 Bronze run과 API 요청에서 온 데이터인지 추적 가능 |
| 재현성 대조 | 내부 PIT 복원값과 ALFRED 특정 vintage 응답값의 일치 여부 검증 |

Bronze는 이후 Silver 계층의 정제와 Gold 계층의 인과 후보 탐색이 신뢰 가능하도록 만드는 가장 기초적인 저장 계층이다.
