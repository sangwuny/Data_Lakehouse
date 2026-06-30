# Gold 계층 문서

이 문서는 FRED/ALFRED 데이터 Lakehouse의 Gold 계층 설계를 설명한다. 기준 언어는 한국어이며, 현재 저장소의 Databricks notebook, Delta Lake, Unity Catalog 기반 serving mart 구조를 다룬다.

## 1. Gold 계층의 목적

Gold 계층은 Silver 계층의 정제된 데이터를 분석 목적에 맞게 재구성한 serving 계층이다. Bronze와 Silver가 원천 보존과 공통 정제에 초점을 둔다면, Gold는 대시보드, 리포팅, ML, causal candidate screening에 바로 사용할 수 있는 형태를 제공한다.

현재 Gold 계층의 핵심 목적은 다음과 같다.

```text
1. 특정 as_of_date에 실제로 알 수 있었던 observation version만 선택한다.
2. daily, monthly, quarterly, annual, native 기준 period feature를 만든다.
3. 단위가 다른 경제지표를 비교할 수 있도록 공통 long-format 변환값을 생성한다.
4. target series와 candidate series 간 lag correlation을 계산한다.
5. Feature Store 또는 downstream ML에서 사용할 수 있는 snapshot table을 제공한다.
```

Gold는 최종 인과 모델 자체가 아니라, 인과 분석과 예측 실험을 시작하기 위한 serving mart이다.

## 2. 입력과 출력

현재 Gold 계층은 Silver의 revision-aware cleaned table을 입력으로 사용한다.

```text
Input : fred_lakehouse.silver.fred_observation_versions_cleaned
Output: fred_lakehouse.gold.*
```

`bronze/01c`에서 적재한 FRED-only current data는 현재 Gold causal feature mart에 자동으로 합쳐지지 않는다. 엄격한 point-in-time 분석에서는 revision-aware Silver table을 기준으로 하는 것이 현재 설계 원칙이다.

## 3. Repository 구조

Gold 관련 notebook은 다음 위치에 있다.

```text
notebooks/databricks/gold/
  03a_gold_fred_bootstrap_causal_features.py
  03b_gold_fred_incremental_causal_features.py
```

- `03a`는 최초 전체 Gold feature mart를 구축한다.
- `03b`는 Silver 변경분을 기준으로 Gold mart를 증분 갱신한다.

## 4. Gold 테이블과 뷰

기본 저장 위치는 다음과 같다.

```text
Catalog: fred_lakehouse
Schema : gold
Format : Delta table / View
```

Gold 주요 테이블과 뷰는 다음과 같다.

```text
fred_lakehouse.gold
├── fred_asof_observations
├── fred_period_features_long
├── fred_transformed_features_long
├── fred_series_feature_snapshot
├── fred_causal_candidate_scores
├── fred_gold_quality_report
├── fred_gold_run_summary
├── fred_latest_feature_snapshot       (view)
└── fred_top_causal_candidates         (view)
```

## 5. 주요 파라미터

공통 파라미터는 다음과 같다.

```text
catalog = fred_lakehouse
silver_schema = silver
gold_schema = gold
as_of_date =                  # blank이면 UTC today
series_ids = ALL
candidate_series_ids = ALL
target_series_id = GDPC1
target_frequency = monthly
aggregation_method = last
causal_transform_type = raw
max_lag_periods = 12
min_pair_count = 24
include_quality_warnings = true
optimize_tables = true
```

`causal_transform_type`은 causal candidate score를 어떤 값으로 계산할지 고르는 파라미터다. 기본값 `raw`는 기존처럼 period 대표값을 그대로 사용한다.

지원 transform type은 다음과 같다.

| Transform type | 의미 | 단위 |
|---|---|---|
| `raw` | period 대표 원값 | 원천 단위 |
| `change_1` | 전 period 대비 변화량 | 원천 단위 또는 percentage point |
| `change_12` | 12 period 전 대비 변화량 | 원천 단위 또는 percentage point |
| `pct_change_1` | 전 period 대비 증가율 | percent |
| `pct_change_12` | 12 period 전 대비 증가율 | percent |
| `log_diff_1` | 전 period 대비 로그 차분 | log percent |
| `log_diff_12` | 12 period 전 대비 로그 차분 | log percent |
| `z_score_full_sample` | as-of 시점 표본 내 표준화 값 | standard deviation |
| `index_base100` | as-of 시점 첫 period를 100으로 둔 지수 | index base 100 |

## 6. 처리 흐름

Gold notebook은 크게 다섯 단계로 동작한다.

```text
1. as-of observation 선택
2. period feature 생성
3. transformed feature 생성
4. latest feature snapshot 생성
5. causal candidate score 계산
```

### 6.1 As-of observation 선택

Silver table에서 특정 `as_of_date`에 실제로 볼 수 있었던 version만 선택한다.

```sql
is_point_in_time_usable
AND value_numeric IS NOT NULL
AND observation_date <= as_of_date
AND realtime_start <= as_of_date
AND realtime_end >= as_of_date
AND coalesce(available_at, realtime_start) <= as_of_date
```

동일 `series_id`, `observation_date`에 여러 version이 보이면 가장 최신 real-time version을 선택한다.

```text
ORDER BY realtime_start DESC, available_at DESC, observation_version_id DESC
```

결과는 `fred_asof_observations`에 저장된다.

### 6.2 Period feature 생성

As-of observation을 target frequency 기준으로 정렬하고 집계한다.

지원 frequency는 다음과 같다.

```text
daily, monthly, quarterly, annual, native
```

지원 aggregation method는 다음과 같다.

```text
last, mean
```

period별로 `mean`, `min`, `max`, `last`, 품질 warning 수, outlier 수, revision 수, 마지막 observation lineage를 계산한다.

결과는 `fred_period_features_long`에 저장된다.

### 6.3 Transformed feature 생성

`fred_period_features_long`의 `value_numeric`을 기반으로 분석 목적별 변환값을 만든다.

이 테이블은 서로 단위가 다른 경제지표를 함께 비교하기 위한 핵심 long-format table이다.

```text
series_id | period_start | transform_type | transformed_value | transformed_unit
```

예를 들어 같은 `series_id = CPIAUCSL`이라도 다음 row들이 함께 존재할 수 있다.

```text
raw
pct_change_1
pct_change_12
z_score_full_sample
index_base100
```

결과는 `fred_transformed_features_long`에 저장된다.

### 6.4 Feature snapshot 생성

각 `as_of_date`, target frequency, aggregation method, series별 최신 period feature를 snapshot 형태로 만든다.

생성되는 대표 feature는 다음과 같다.

| Feature | 설명 |
|---|---|
| `value_numeric` | 최신 period의 대표 값 |
| `lag_1_value_numeric` | 1 period lag |
| `lag_3_value_numeric` | 3 period lag |
| `lag_6_value_numeric` | 6 period lag |
| `lag_12_value_numeric` | 12 period lag |
| `diff_1_value_numeric` | 현재 값과 1 period lag의 차이 |
| `pct_change_1` | 1 period percentage change |
| `rolling_mean_3` | 최근 3 period 평균 |
| `rolling_mean_6` | 최근 6 period 평균 |
| `rolling_mean_12` | 최근 12 period 평균 |
| `rolling_stddev_12` | 최근 12 period 표준편차 |

결과는 `fred_series_feature_snapshot`에 저장된다. 이 테이블은 Feature Store 또는 dashboard summary에 쓰기 좋다.

### 6.5 Causal candidate score 계산

Gold는 target series와 candidate series를 target frequency 기준으로 정렬한 뒤 lag별 correlation을 계산한다.

중요한 점은 candidate score가 이제 `value_numeric`만이 아니라 `causal_transform_type`으로 선택된 `transformed_value`를 기준으로 계산된다는 것이다.

```text
causal_transform_type = raw            # 원값 기준
causal_transform_type = pct_change_12  # 전년동기 대비 증가율 기준
causal_transform_type = z_score_full_sample
```

계산 흐름은 다음과 같다.

```text
1. target series의 transform_type별 period 값을 선택한다.
2. lag_periods = 0 .. max_lag_periods를 생성한다.
3. candidate series를 lag만큼 과거로 이동해 target period와 맞춘다.
4. pair_count와 pearson correlation을 계산한다.
5. coverage_rate와 candidate_score를 계산한다.
6. score 기준으로 candidate_rank를 부여한다.
```

현재 score는 다음 개념을 사용한다.

```text
candidate_score = abs(pearson_corr) * coverage_rate
coverage_rate   = pair_count / target_period_count
```

결과는 `fred_causal_candidate_scores`에 저장된다. 이 점수는 인과성을 증명하는 결과가 아니라, 추가 검토할 후보를 줄여주는 screening signal이다.

## 7. 테이블별 역할과 key

### 7.1 `fred_asof_observations`

특정 `as_of_date`에 볼 수 있었던 observation version만 저장한다.

```text
Key: as_of_date, series_id, observation_date
```

### 7.2 `fred_period_features_long`

As-of observation을 target frequency와 aggregation method 기준으로 정렬한 period-level feature table이다.

```text
Key: as_of_date, target_frequency, aggregation_method, series_id, period_start
```

### 7.3 `fred_transformed_features_long`

분석 목적별 변환값을 long-format으로 저장한다. 서로 단위가 다른 series를 비교하거나 dashboard에서 여러 series를 겹쳐 볼 때 가장 중요하다.

```text
Key: as_of_date, target_frequency, aggregation_method, series_id, period_start, transform_type
```

주요 컬럼은 다음과 같다.

| Column | 의미 |
|---|---|
| `transform_type` | 변환 종류 |
| `transformed_value` | 변환된 분석용 값 |
| `transformed_unit` | 변환 후 단위 |
| `base_value_numeric` | 변환 전 period 대표값 |
| `comparison_lag_periods` | 비교 기준 lag |
| `lookback_periods` | 계산에 사용한 lookback period |
| `calculation_method` | 계산 방식 설명 |

### 7.4 `fred_series_feature_snapshot`

각 series별 최신 feature snapshot이다.

```text
Key: as_of_date, target_frequency, aggregation_method, series_id
```

### 7.5 `fred_causal_candidate_scores`

target series와 candidate series 간 transform type, lag별 screening score를 저장한다.

```text
Key: as_of_date, target_frequency, aggregation_method, transform_type,
     target_series_id, candidate_series_id, lag_periods
```

### 7.6 `fred_gold_quality_report`

Gold run 단위 품질 검증 결과를 append-only로 기록한다.

### 7.7 `fred_gold_run_summary`

Gold run 단위 요약 테이블이다. `causal_transform_type`, 처리 mode, target frequency, target series, row 수, quality rule 실패 수 등을 기록한다.

Incremental Gold는 이 테이블의 `processed_at_utc`를 다음 실행의 watermark로 사용한다.

### 7.8 `fred_latest_feature_snapshot`

`fred_series_feature_snapshot`에서 frequency, aggregation method, series별 최신 as-of row만 보여주는 view이다.

### 7.9 `fred_top_causal_candidates`

`fred_causal_candidate_scores`에서 최신 as-of 기준 상위 candidate만 보여주는 view이다. 현재 `candidate_rank <= 20` 조건을 사용한다.

## 8. Dashboard에서 주로 볼 테이블

대시보드 목적별 권장 테이블은 다음과 같다.

| 목적 | 권장 테이블 |
|---|---|
| 두 개 이상 series의 증가율, z-score, 지수화 비교 | `fred_transformed_features_long` |
| 최신 series 상태 요약 | `fred_latest_feature_snapshot` |
| target에 대한 선행 후보 확인 | `fred_top_causal_candidates` |
| 특정 as-of 기준 원천 관측 version 확인 | `fred_asof_observations` |
| Gold 실행 품질/row 수 확인 | `fred_gold_run_summary`, `fred_gold_quality_report` |

서로 단위가 다른 경제지표를 시각적으로 비교할 때는 `fred_transformed_features_long`에서 같은 `transform_type`만 필터링해서 보는 것이 좋다.

예를 들어 증가율 비교는 다음 조건을 권장한다.

```sql
WHERE transform_type IN ('pct_change_1', 'pct_change_12')
```

## 9. Incremental 처리 원칙

Gold incremental은 Silver 변경량과 현재 transform score 존재 여부에 따라 재계산 범위를 조절한다.

| 상황 | 처리 방식 |
|---|---|
| 이전 Gold watermark 없음 | bootstrap처럼 전체 selected series 처리 |
| target series 변경 | 전체 candidate 재계산 |
| candidate만 변경 | 변경 candidate와 target만 재계산 |
| as-of snapshot 없음 + 보강 옵션 true | complete as-of snapshot 생성 |
| 선택한 `causal_transform_type`의 score 없음 | 전체 selected series 처리 |
| Silver 변경 없음 | notebook exit |

Gold는 각 대상 범위의 기존 row를 먼저 삭제한 뒤 `MERGE`한다. 따라서 같은 as-of date와 같은 파라미터로 재실행해도 결과가 중복되지 않는다.

## 10. 품질과 최적화

Gold table에는 가능한 경우 다음 Delta table properties를 적용한다.

```text
delta.enableChangeDataFeed = true
delta.autoOptimize.optimizeWrite = true
delta.autoOptimize.autoCompact = true
lakehouse.layer = gold
lakehouse.source = fred
```

`optimize_tables = true`이면 주요 table에 대해 `OPTIMIZE ... ZORDER BY`를 시도한다.

| 테이블 | ZORDER 기준 |
|---|---|
| `fred_asof_observations` | `as_of_date`, `series_id` |
| `fred_period_features_long` | `as_of_date`, `series_id` |
| `fred_transformed_features_long` | `as_of_date`, `series_id`, `transform_type` |
| `fred_series_feature_snapshot` | `as_of_date`, `series_id` |
| `fred_causal_candidate_scores` | `as_of_date`, `target_series_id` |

서버리스 또는 권한 제한 환경에서 `OPTIMIZE`가 실패할 수 있으므로 notebook은 실패 시 skip 메시지만 출력한다.

## 11. 운영 파라미터 권장값

현재 20개 seed catalog 기준으로 실질 GDP를 target으로 삼는다면 다음 설정을 권장한다.

```text
target_series_id = GDPC1
target_frequency = monthly
aggregation_method = last
causal_transform_type = raw
max_lag_periods = 12
min_pair_count = 24
include_quality_warnings = true
```

단위가 다른 지표 간 변동률 관계를 보고 싶다면 다음도 자주 쓸 수 있다.

```text
causal_transform_type = pct_change_12
```

분기 GDP를 target으로 직접 맞추고 싶다면 `target_frequency = quarterly`도 가능하다. 다만 다른 월별/일별 candidate와의 정렬 방식이 달라지므로 해석에 주의해야 한다.

## 12. 운영 순서

권장 실행 순서는 다음과 같다.

```text
초기 구축:
bronze/01a_bronze_fred_bootstrap_versions.py
-> silver/02a_silver_fred_bootstrap_versions.py
-> gold/03a_gold_fred_bootstrap_causal_features.py

일일 증분:
bronze/01b_bronze_fred_incremental_versions.py
-> silver/02b_silver_fred_incremental_versions.py
-> gold/03b_gold_fred_incremental_causal_features.py
```

Gold는 Silver의 정제 결과에 의존하므로 Silver가 먼저 성공해야 한다.

## 13. 해석상 주의점

`fred_causal_candidate_scores`의 correlation 기반 score는 인과관계를 확정하지 않는다. 이 값은 다음 질문을 빠르게 좁히는 screening 결과로 보는 것이 안전하다.

```text
어떤 candidate series가 target series보다 몇 period 앞서 움직이는 경향이 있는가?
그 관계가 충분한 관측 pair에서 반복되는가?
품질 warning이나 outlier가 score를 왜곡하고 있지는 않은가?
원값 기준 관계와 증가율 기준 관계가 서로 일관적인가?
```

최종 인과 판단은 추가적인 경제 이론, 시차 구조 검토, backtesting, causal model, robustness check를 거쳐야 한다.

## 14. 요약

Gold 계층은 Silver의 정제된 revision-aware 데이터를 분석과 ML에 바로 사용할 수 있는 형태로 만든다.

| 역할 | 설명 |
|---|---|
| as-of 복원 | 특정 시점에 실제로 볼 수 있었던 observation version 선택 |
| 기간 정렬 | daily, monthly, quarterly, annual, native 기준 period feature 생성 |
| 공통 변환 | raw, 변화량, 증가율, 로그 차분, z-score, 기준시점 100 지수 생성 |
| 후보 탐색 | 선택한 transform type 기준 lag correlation screening score 계산 |
| serving table | dashboard, ML, Feature Store가 사용할 수 있는 snapshot 제공 |
| 품질 관리 | Gold rule 결과와 run summary를 append-only로 기록 |

Gold는 project-specific 분석 요구가 반영되는 계층이므로, 이후 연구 질문이 구체화될수록 feature와 score 계산 방식이 확장될 수 있다.