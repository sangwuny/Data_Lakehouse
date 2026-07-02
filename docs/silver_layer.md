# Silver 계층 문서

이 문서는 FRED/ALFRED 데이터 Lakehouse의 Silver 계층 설계를 설명한다. 기준 언어는 한국어이며, 현재 저장소의 Databricks notebook, Delta Lake, Unity Catalog 기반 구조만 다룬다.

## 1. Silver 계층의 목적

Silver 계층은 Bronze 계층의 원본성 높은 데이터를 분석 가능한 형태로 정제하고 표준화하는 계층이다. Bronze가 API 응답과 observation version을 최대한 원형에 가깝게 보존한다면, Silver는 그 데이터를 “just-enough” 수준으로 cleansed, conformed, quality-tagged 상태로 만든다.

이 프로젝트에서 Silver의 핵심 목적은 다음과 같다.

```text
1. Bronze의 문자열 기반 원본 값을 분석 가능한 숫자/날짜 타입으로 변환한다.
2. 결측, 파싱 오류, real-time range 오류, 이상치를 품질 상태로 표시한다.
3. ALFRED revision-aware 데이터는 point-in-time 분석에 사용할 수 있게 정제한다.
4. FRED current-only 데이터는 ALFRED와 분리해 최신 current mirror로 정제한다.
5. Gold 계층이 as-of feature mart 또는 current dashboard mart를 안정적으로 만들 수 있게 한다.
```

Silver는 Gold처럼 목적별 feature를 과하게 만들지 않는다. 대신 여러 분석 프로젝트가 공통으로 사용할 수 있는 정제된 enterprise view를 제공한다.

## 2. 현재 설계 범위

현재 Silver 계층은 시간 의미가 다른 두 경로를 분리해서 정제한다.

```text
ALFRED revision-aware input : fred_lakehouse.bronze.fred_observation_versions
ALFRED Silver output       : fred_lakehouse.silver.fred_observation_versions_cleaned

FRED current-only input    : fred_lakehouse.bronze.fred_current_observations_raw
FRED current Silver output : fred_lakehouse.silver.fred_current_observations_cleaned
```

`fred_observation_versions_cleaned`는 point-in-time 재현이 가능한 revision-aware table이다. 반면 `fred_current_observations_cleaned`는 ALFRED revision history가 없는 FRED-only series의 최신 current mirror를 정제한 table이며, `is_point_in_time_safe = false`를 명시적으로 보존한다.

## 3. Repository 구조

Silver 관련 notebook은 다음 위치에 있다.

```text
notebooks/databricks/silver/
  02a_silver_fred_bootstrap_versions.py
  02b_silver_fred_incremental_versions.py
  02c_silver_fred_current_observations.py
```

- `02a`는 ALFRED revision-aware 최초 전체 Silver cleaning을 수행한다.
- `02b`는 ALFRED Bronze 변경분이 있는 series만 다시 정제한다.
- `02c`는 FRED current-only Bronze table을 별도 Silver current table로 정제한다.

## 4. 입력과 출력

기본 저장 위치는 다음과 같다.

```text
Catalog: fred_lakehouse
Schema : silver
Format : Delta table / View
```

Silver 주요 테이블과 뷰는 다음과 같다.

```text
fred_lakehouse.silver
├── fred_observation_versions_cleaned
├── fred_version_series_catalog
├── fred_version_quality_report
├── fred_version_lineage_events
├── fred_version_run_summary
├── fred_observations_asof_ready              (view)
├── fred_observations_current                 (view)
├── fred_current_observations_cleaned
├── fred_current_series_catalog
├── fred_current_quality_report
├── fred_current_run_summary
└── fred_current_observations_analytics_ready (view)
```

## 5. Silver notebook 역할

### 5.1 `silver/02a_silver_fred_bootstrap_versions.py`

Bronze에 적재된 전체 ALFRED observation version을 정제한다. 최초 구축 또는 전체 재구축에 사용한다.

주요 파라미터는 다음과 같다.

```text
catalog = fred_lakehouse
bronze_schema = bronze
silver_schema = silver
series_ids = ALL
outlier_threshold = 6.0
include_missing_in_silver = true
```

주요 동작은 다음과 같다.

- `bronze.fred_observation_versions`에서 대상 series 선택
- `value_raw`를 `value_numeric`으로 변환
- 날짜 문자열을 date 타입으로 변환
- 결측과 파싱 오류 식별
- real-time range 오류 식별
- observation date별 revision 순서 계산
- robust z-score 기반 이상치 점수 계산
- 품질 상태와 품질 이슈 문자열 생성
- `fred_observation_versions_cleaned`에 Delta `MERGE`
- 품질 리포트, series catalog, lineage, run summary 기록

### 5.2 `silver/02b_silver_fred_incremental_versions.py`

Bronze incremental 이후 변경된 series만 다시 정제한다. 변경 감지는 두 방식으로 가능하다.

```text
1. bronze_run_id를 명시하면 해당 Bronze run에서 새로 발견된 observation version을 기준으로 처리한다.
2. bronze_run_id를 비우면 이전 Silver watermark 이후 수집된 Bronze version을 기준으로 처리한다.
```

증분 처리에서 중요한 점은 row 단위만 부분 계산하지 않고, 변경된 series 전체를 다시 정제한다는 것이다. revision count, revision number, outlier score처럼 series 전체 문맥이 필요한 값들이 있기 때문이다.

### 5.3 `silver/02c_silver_fred_current_observations.py`

Bronze `fred_current_observations_raw`에 저장된 FRED-only current 데이터를 정제한다. 이 경로는 ALFRED revision-aware table과 섞지 않는다.

주요 파라미터는 다음과 같다.

```text
catalog = fred_lakehouse
bronze_schema = bronze
silver_schema = silver
seed_catalog_path = ../configs/fred_seed_series.json
series_ids = ALL
outlier_threshold = 6.0
include_missing_in_silver = true
```

주요 동작은 다음과 같다.

- `bronze.fred_current_observations_raw`에서 대상 series 선택
- seed catalog를 조인해 domain, priority, expected frequency를 보강
- `value_raw`를 `value_numeric`으로 변환
- 결측과 파싱 오류 식별
- expected frequency 기준 `period_start`, `period_end` 추론
- current-only diff 기반 robust z-score 이상치 점수 계산
- `history_type = current_only`, `is_point_in_time_safe = false` 명시
- `fred_current_observations_cleaned`에 Delta `MERGE`
- current quality report, series catalog, run summary 기록

## 6. 핵심 정제 규칙

### 6.1 타입 표준화

Bronze의 `value_raw`는 FRED API 응답을 보존하기 위해 문자열이다. Silver에서는 다음 값을 추가한다.

| 컬럼 | 설명 |
|---|---|
| `value_text` | trim 처리된 문자열 값 |
| `value_numeric` | `try_cast(value_raw AS DOUBLE)` 결과 |
| `observation_date` | date 타입 관측일 |
| `period_start`, `period_end` | 관측 기간을 date 타입으로 추론 |
| `realtime_start`, `realtime_end` | date 타입 real-time range |
| `available_at` | 사용 가능일. ALFRED는 vintage/real-time 기반, FRED current는 collection date 기반 |

### 6.2 결측과 파싱 오류

FRED는 결측값을 `.`으로 표현하는 경우가 있다. Silver는 이를 명시적으로 표시한다.

| 컬럼 | 설명 |
|---|---|
| `is_missing` | 값이 null, 빈 문자열, `.`, 숫자 변환 불가인지 여부 |
| `missing_reason` | `null`, `empty_string`, `fred_dot`, `numeric_parse_error` |
| `is_numeric_parse_error` | 값이 존재하지만 숫자로 변환할 수 없는 경우 |

`include_missing_in_silver = true`이면 결측 row도 Silver에 보존한다. 이 설정은 원본 추적성과 품질 리포팅에 유리하다.

### 6.3 time semantics

ALFRED와 FRED current는 시간 의미가 다르다.

| 구분 | ALFRED revision-aware | FRED current-only |
|---|---|---|
| 핵심 테이블 | `fred_observation_versions_cleaned` | `fred_current_observations_cleaned` |
| history type | revision history | current only |
| point-in-time safe | true | false |
| as-of 복원 | 가능 | 불가 |
| 사용 목적 | 재현성, backtest, causal feature | 최신 대시보드, current indicator |

FRED current-only row는 `is_point_in_time_safe = false`로 저장된다. 이것은 품질 오류가 아니라 데이터 의미에 대한 명시적 표시이다.

### 6.4 revision feature

ALFRED Silver는 같은 `series_id`, `observation_date`에 여러 version이 있으면 사후 개정 이력을 계산한다.

| 컬럼 | 설명 |
|---|---|
| `revision_number` | observation date 안에서 시간순 revision 순번 |
| `revision_count` | 해당 observation date의 총 revision 수 |
| `is_observation_date_revised` | revision count가 2 이상인지 여부 |
| `previous_revision_value_numeric` | 직전 revision 값 |
| `revision_delta_value` | 현재 revision 값과 직전 revision 값의 차이 |

FRED current-only Silver는 revision feature를 만들지 않는다. 대신 current observation의 변화량인 `observation_diff_value`를 계산해 이상치 탐지에 사용한다.

### 6.5 이상치 점수

Silver는 robust z-score와 MAD를 사용해 series별 이상치를 탐지한다.

| 컬럼 | ALFRED | FRED current |
|---|---|---|
| `outlier_level_score` | 값 수준 robust z-score | 값 수준 robust z-score |
| `outlier_diff_score` | 현재 version 기준 관측값 변화량 | current observation 변화량 |
| `outlier_revision_score` | revision delta 기준 | null |
| `outlier_score` | diff/revision score 기준 | diff score 기준 |
| `outlier_threshold` | 기본 `6.0` | 기본 `6.0` |
| `is_outlier` | threshold 초과 여부 | threshold 초과 여부 |

이상치는 즉시 제거하지 않고 품질 상태로 표시한다. 제거 여부는 Gold나 분석 목적에 따라 결정한다.

### 6.6 품질 상태

Silver는 row별로 `quality_status`와 `quality_issues`를 부여한다.

| 상태 | 의미 |
|---|---|
| `valid` | 분석 사용에 큰 문제가 없는 row |
| `warning` | 결측이나 이상치처럼 주의가 필요한 row |
| `error` | numeric parse error, real-time range error, duplicate version 등 분석에 부적절한 row |

Gold notebook은 기본적으로 `quality_status <> 'error'`인 row를 사용할 수 있고, `include_quality_warnings` 파라미터로 warning 포함 여부를 조절한다.

## 7. 테이블별 역할

### 7.1 `fred_observation_versions_cleaned`

Silver의 ALFRED canonical table이다. Bronze observation version 하나를 정제된 observation version 하나로 매핑한다.

병합 key는 다음과 같다.

```text
observation_version_id
```

주요 사용자는 Gold 계층의 as-of snapshot, period feature, causal candidate scoring이다.

### 7.2 `fred_version_series_catalog`

ALFRED Silver 기준 series catalog이다. series별 source, domain, priority, frequency, units, 관측 기간, Silver 처리 시각 등을 요약한다.

### 7.3 `fred_version_quality_report`

ALFRED Silver run별 품질 집계 테이블이다. series별 전체 row 수, 결측 수, parse error 수, real-time range error 수, 이상치 수 등을 기록한다.

### 7.4 `fred_version_lineage_events`

ALFRED Silver run이 어떤 Bronze table에서 어떤 Silver table을 만들었는지 기록한다. 변환 이름, 변환 버전, 처리 규칙 요약을 포함한다.

### 7.5 `fred_version_run_summary`

ALFRED Silver notebook 실행 단위 요약 테이블이다. 처리 row 수, series 수, 품질 이슈 수, source watermark 등을 기록한다.

### 7.6 `fred_observations_asof_ready`

Gold에서 point-in-time 복원에 사용할 수 있는 ALFRED row만 노출하는 view이다.

```text
Source table: fred_observation_versions_cleaned
Filter      : is_point_in_time_usable
```

### 7.7 `fred_observations_current`

ALFRED revision-aware table에서 현재 유효한 version만 노출하는 view이다.

```text
Source view: fred_observations_asof_ready
Filter     : is_current_version
```

이 view는 “최신값 조회”에는 편리하지만, 과거 as-of 분석에는 `fred_observations_asof_ready` 또는 Gold의 as-of table을 사용해야 한다.

### 7.8 `fred_current_observations_cleaned`

FRED current-only 데이터의 Silver canonical table이다. Bronze current row 하나를 정제된 current observation 하나로 매핑한다.

병합 key는 다음과 같다.

```text
source, series_id, observation_date
```

이 테이블은 `history_type = current_only`, `is_point_in_time_safe = false`를 보존한다.

### 7.9 `fred_current_series_catalog`

FRED current-only series별 catalog이다. seed catalog의 domain, priority, expected frequency와 정제 row 수, 관측 기간, 품질 이슈 수를 요약한다.

### 7.10 `fred_current_quality_report`

FRED current-only Silver run별 품질 집계 테이블이다. 결측, parse error, real-time range error, 이상치 수를 series별로 기록한다.

### 7.11 `fred_current_run_summary`

FRED current-only Silver notebook 실행 단위 요약 테이블이다.

### 7.12 `fred_current_observations_analytics_ready`

FRED current-only cleaned table에서 `quality_status <> 'error'`인 row만 노출하는 view이다. 최신 대시보드나 current indicator mart의 입력으로 사용할 수 있지만, point-in-time 재현 분석에는 사용하지 않는다.

## 8. Incremental 처리 원칙

ALFRED incremental은 효율성과 정확성 사이의 균형을 잡는다.

- 변경된 Bronze observation version을 찾는다.
- 변경이 발생한 series 목록을 만든다.
- 해당 series의 전체 Bronze version을 다시 읽어 revision 순서와 이상치 점수를 재계산한다.
- 정제 결과를 `observation_version_id` 기준으로 `MERGE`한다.
- 품질 리포트, lineage, run summary는 append-only로 기록한다.

FRED current-only Silver는 Bronze current mirror를 다시 읽어 `source, series_id, observation_date` 기준으로 upsert한다. FRED current-only에는 revision history가 없으므로 ALFRED와 같은 revision count 계산은 수행하지 않는다.

## 9. Gold와의 관계

Gold 계층은 목적에 따라 Silver table을 다르게 사용한다.

```text
재현성 / point-in-time / causal feature mart
-> silver.fred_observation_versions_cleaned

최신 대시보드 / current indicator mart
-> silver.fred_observations_current
   + silver.fred_current_observations_analytics_ready
```

따라서 Silver에서 가장 중요한 보증은 다음이다.

```text
1. value_numeric이 안정적으로 계산되어 있다.
2. error row와 warning row가 구분되어 있다.
3. ALFRED와 FRED current의 time semantics가 섞이지 않는다.
4. FRED current-only는 is_point_in_time_safe = false로 명시된다.
```

## 10. 운영 순서

권장 실행 순서는 다음과 같다.

```text
초기 구축:
bronze/01a_bronze_fred_bootstrap_versions.py
-> silver/02a_silver_fred_bootstrap_versions.py
-> gold/03a_gold_fred_bootstrap_causal_features.py

bronze/01c_bronze_fred_current_observations.py
-> silver/02c_silver_fred_current_observations.py

일일 증분:
bronze/01b_bronze_fred_incremental_versions.py
-> silver/02b_silver_fred_incremental_versions.py
-> gold/03b_gold_fred_incremental_causal_features.py

bronze/01c_bronze_fred_current_observations.py
-> silver/02c_silver_fred_current_observations.py
```

## 11. 요약

Silver 계층은 Bronze의 원본성과 Gold의 분석 편의성 사이에 있는 정제 계층이다.

| 역할 | 설명 |
|---|---|
| 타입 정제 | 문자열 value와 날짜를 분석 가능한 타입으로 변환 |
| 품질 표시 | 결측, parse error, real-time 오류, 이상치를 명시적으로 태깅 |
| revision 정렬 | ALFRED observation date별 revision number와 revision count 계산 |
| current 정제 | FRED-only current 데이터를 별도 Silver table로 정제 |
| point-in-time 준비 | ALFRED as-of 복원에 사용할 수 있는 정제 version 제공 |
| semantic 분리 | revision-aware와 current-only 데이터를 섞지 않고 명시적으로 구분 |

Silver는 복잡한 모델링이나 목적별 feature 생성보다, 여러 Gold 프로젝트가 공통으로 신뢰할 수 있는 정제된 economic data를 제공하는 데 초점을 둔다.
