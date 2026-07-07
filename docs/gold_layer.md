# Gold 계층 문서

이 문서는 현재 운영 기준의 Databricks Gold 계층을 설명한다. Gold 계층은 Silver에서 정제된 ALFRED revision-aware 데이터와 FRED current-only 데이터를 통합하여 특정 as_of_date 기준의 분석용 feature mart와 관계 후보 점수를 만든다.

## 1. 목적

Gold 계층의 목적은 다음과 같다.

    1. as_of_date 시점에 관측 가능했던 데이터만 선택한다.
    2. ALFRED revision-aware series와 FRED current-only series를 하나의 분석 mart로 통합한다.
    3. daily, monthly, quarterly, annual, native 기준 period feature를 생성한다.
    4. 단위가 다른 경제 지표를 비교할 수 있도록 공통 변환값을 long format으로 저장한다.
    5. target series와 candidate series 사이의 lag별 관계 점수를 계산한다.
    6. Databricks SQL, AI BI Dashboard, ML 분석에서 사용할 수 있는 serving table을 제공한다.

Gold는 원천 수집 계층이 아니라 분석과 서빙 계층이다. 원자료 보존은 Bronze와 Silver가 담당하고, Gold는 as-of 재현, 변환, 비교, 관계 탐색에 집중한다.

## 2. 운영 Notebook

현재 Gold 계층의 운영 notebook은 다음 파일이다.

    notebooks/databricks/gold/03_gold_fred_asof_features.py

입력 Silver table은 다음 두 개다.

    fred_lakehouse.silver.fred_observation_versions_cleaned
    fred_lakehouse.silver.fred_current_observations_cleaned

Macrotrends 또는 Yahoo Finance에서 bootstrap으로 보강된 값도 Bronze와 Silver에서 FRED current-only 구조로 정규화되므로, Gold에서는 별도 provider 분기 없이 함께 처리한다.

## 3. 주요 파라미터

| 파라미터 | 기본값 | 설명 |
|---|---:|---|
| catalog | fred_lakehouse | Unity Catalog 이름 |
| silver_schema | silver | 입력 Silver schema |
| gold_schema | gold | 출력 Gold schema |
| as_of_date | blank | blank이면 UTC today |
| analysis_start_date | blank | 분석 시작일. blank이면 가능한 가장 이른 period부터 사용 |
| analysis_end_date | blank | 분석 종료일. blank이면 as_of_date |
| series_ids | ALL | 처리할 series 목록 |
| candidate_series_ids | ALL | 관계 후보로 볼 candidate 목록 |
| target_series_id | GDPC1 | 관계 점수 계산의 target series |
| target_frequency | monthly | daily, monthly, quarterly, annual, native |
| aggregation_method | last | last 또는 mean |
| relationship_transform_type | raw | 관계 점수에 사용할 변환값 |
| max_lag_periods | 12 | 후보 lag 탐색 범위 |
| min_pair_count | 24 | 관계 점수를 저장하기 위한 최소 pair 수 |
| min_candidate_score | 0.3 | top relationship view에 표시할 최소 점수 |
| include_quality_warnings | true | Silver warning row 포함 여부 |
| optimize_tables | true | Delta OPTIMIZE ZORDER 실행 시도 여부 |

analysis_start_date와 analysis_end_date는 변환값과 관계 점수 계산에 직접 영향을 준다. series별 시작일이 다를 때는 공통 분석 구간을 명시하는 것이 좋다. 예를 들어 NASDAQCOM처럼 관측 시작이 늦은 지표를 비교할 때는 2000-01-01 또는 2010-01-01 같은 시작일을 줄 수 있다.

analysis_start_date가 비어 있으면 NULL로 저장된다. 관계 점수 계산에서는 null-safe join을 사용하므로 시작일을 비워도 score가 생성된다.

## 4. 출력 테이블과 View

| 이름 | 유형 | 역할 |
|---|---|---|
| fred_asof_observations | Delta table | as_of_date 기준 관측 가능 row |
| fred_period_features_long | Delta table | target frequency로 정렬된 period-level feature |
| fred_transformed_features_long | Delta table | raw, 변화량, 증가율, z-score, 지수화 값의 long-format table |
| fred_series_feature_snapshot | Delta table | series별 최신 period feature snapshot |
| fred_relationship_candidate_scores | Delta table | target, candidate, lag별 관계 점수 |
| fred_gold_quality_report | Delta table | Gold 품질 규칙 결과 |
| fred_gold_run_summary | Delta table | Gold 실행 요약 |
| fred_latest_feature_snapshot | View | 최신 as-of snapshot view |
| fred_top_relationship_candidates | View | 최소 score 이상 relationship candidate view |

대시보드에서 여러 지표를 비교할 때 가장 자주 쓰는 테이블은 fred_transformed_features_long이다. target과 candidate 관계를 볼 때는 fred_relationship_candidate_scores 또는 fred_top_relationship_candidates를 사용한다.

## 5. 처리 흐름

    1. as-of observation stage 생성
    2. period feature stage 생성
    3. transformed feature stage 생성
    4. latest feature snapshot 생성
    5. relationship candidate score 계산
    6. latest view와 top relationship view 생성
    7. quality report와 run summary 저장
    8. Delta table property 및 OPTIMIZE 시도

## 6. As-of Observation

ALFRED revision-aware 데이터는 real-time window 기준으로 as_of_date에 실제로 볼 수 있었던 version만 선택한다.

    is_point_in_time_usable
    value_numeric IS NOT NULL
    observation_date <= as_of_date
    realtime_start <= as_of_date
    realtime_end >= as_of_date
    coalesce available_at realtime_start <= as_of_date

같은 series_id와 observation_date에 여러 version이 있으면 realtime_start, available_at, observation_version_id 기준으로 가장 최신 row를 선택한다.

FRED current-only 데이터는 revision-aware 재현 대상이 아니므로 observation_date가 as_of_date 이하인 row를 포함한다. Gold 결과에는 history_type, availability_basis, is_revision_aware 컬럼이 있어 revision-aware row와 current-only row를 구분할 수 있다.

## 7. Period Feature

period feature는 target_frequency와 aggregation_method에 따라 생성된다.

    지원 frequency: daily, monthly, quarterly, annual, native
    지원 aggregation: last, mean

analysis_start_date와 analysis_end_date가 지정되면 해당 구간 안의 period_start만 이후 변환과 관계 점수 계산에 사용된다.

## 8. Transformed Feature

fred_transformed_features_long은 단위가 다른 경제 지표를 같은 방식으로 비교하기 위한 핵심 테이블이다.

| transform_type | 의미 | 단위 |
|---|---|---|
| raw | period 대표값 | 원천 단위 |
| change_1 | 1 period 전 대비 변화량 | 원천 단위 또는 percentage point |
| change_12 | 12 period 전 대비 변화량 | 원천 단위 또는 percentage point |
| pct_change_1 | 1 period 전 대비 증가율 | percent |
| pct_change_12 | 12 period 전 대비 증가율 | percent |
| log_diff_1 | 1 period 로그 차분 | log percent |
| log_diff_12 | 12 period 로그 차분 | log percent |
| z_score_full_sample | 선택된 분석 구간 내 full-sample z-score | standard deviation |
| index_base100 | 선택된 분석 구간의 첫 period를 100으로 둔 지수 | index base 100 |

권장 사용 방식은 다음과 같다.

    수준값 비교: raw, index_base100
    증가율 비교: pct_change_1, pct_change_12, log_diff_1, log_diff_12
    금리와 실업률 변화 비교: change_1, change_12
    한 그래프 스케일 비교: z_score_full_sample

z_score_full_sample과 index_base100은 선택된 분석 구간을 기준으로 계산된다. 따라서 analysis_start_date를 바꾸면 값도 바뀐다.

## 9. Feature Snapshot

fred_series_feature_snapshot은 각 series의 최신 period만 뽑아 feature snapshot으로 저장한다.

주요 feature는 다음과 같다.

    value_numeric
    lag_1_value_numeric
    lag_3_value_numeric
    lag_6_value_numeric
    lag_12_value_numeric
    diff_1_value_numeric
    pct_change_1
    rolling_mean_3
    rolling_mean_6
    rolling_mean_12
    rolling_stddev_12

## 10. Relationship Candidate Scores

관계 후보 점수는 target series와 candidate series를 같은 relationship_transform_type 기준으로 맞춘 뒤 lag별 Pearson correlation을 계산한다.

계산 흐름은 다음과 같다.

    1. target_series_id의 transformed_value를 선택한다.
    2. lag_periods = 0 .. max_lag_periods를 생성한다.
    3. candidate period를 lag만큼 과거로 이동해 target period와 맞춘다.
    4. target_value_numeric, candidate_value_numeric이 모두 null이 아닌 pair만 사용한다.
    5. pair_count >= min_pair_count인 후보만 저장한다.
    6. pearson_corr, r_squared, coverage_rate, candidate_score를 계산한다.

현재 점수 공식은 다음과 같다.

    r_squared       = pearson_corr squared
    coverage_rate   = pair_count / target_period_count
    candidate_score = r_squared * coverage_rate

candidate_score만 보지 말고 r_squared, coverage_rate, pair_count, target_period_count를 함께 봐야 한다. coverage_rate는 데이터 겹침 정도를 반영하므로, 시작 시점이 늦은 지표는 score가 낮게 보일 수 있다. 이런 경우 analysis_start_date로 공통 분석 구간을 맞추는 것이 좋다.

fred_top_relationship_candidates view는 candidate_score >= min_candidate_score 조건을 만족하는 최신 후보를 보여준다.

## 11. 테이블 Key

| 테이블 | Merge key |
|---|---|
| fred_asof_observations | as_of_date, series_id, observation_date |
| fred_period_features_long | as_of_date, target_frequency, aggregation_method, series_id, period_start |
| fred_transformed_features_long | as_of_date, target_frequency, aggregation_method, series_id, period_start, transform_type |
| fred_series_feature_snapshot | as_of_date, target_frequency, aggregation_method, series_id |
| fred_relationship_candidate_scores | as_of_date, analysis_start_date, analysis_end_date, target_frequency, aggregation_method, transform_type, target_series_id, candidate_series_id, lag_periods |

현재 notebook은 실행 시 같은 as_of_date, target_frequency, aggregation_method, series_id 범위의 period, transformed, snapshot row를 삭제 후 다시 merge한다. 따라서 여러 analysis_start_date 조합을 동시에 장기 보존하는 용도로는 아직 완전히 분리되어 있지 않다. 분석 구간별 결과를 장기 보존하려면 delete 조건과 merge key를 더 확장하는 개선이 필요하다.

## 12. 품질 리포트와 실행 요약

fred_gold_quality_report는 다음 규칙 결과를 기록한다.

    asof_not_before_observation
    visible_inside_realtime_window
    period_feature_value_not_null
    candidate_scores_meet_min_pair_count

fred_gold_run_summary는 실행 단위로 다음 정보를 기록한다.

    gold_run_id
    as_of_date
    analysis_start_date
    analysis_end_date
    target_frequency
    aggregation_method
    relationship_transform_type
    target_series_id
    processing_series_count
    asof_observation_count
    period_feature_count
    transformed_feature_count
    feature_snapshot_count
    relationship_candidate_score_count
    failed_quality_rule_count

## 13. Dashboard 권장 사용

| 목적 | 권장 테이블 또는 View |
|---|---|
| 여러 series의 raw, 증가율, z-score 비교 | fred_transformed_features_long |
| 최신 series 상태 요약 | fred_latest_feature_snapshot |
| target과 관계가 높은 후보 확인 | fred_relationship_candidate_scores |
| score threshold가 적용된 후보 확인 | fred_top_relationship_candidates |
| as-of 기준 원천 관측 row 확인 | fred_asof_observations |
| 실행 품질과 row 수 확인 | fred_gold_run_summary, fred_gold_quality_report |

서로 단위가 다른 지표를 비교할 때는 raw를 바로 겹쳐 그리기보다 pct_change_1, pct_change_12, log_diff_1, log_diff_12, z_score_full_sample, index_base100 중 목적에 맞는 transform_type을 선택하는 것이 좋다.

## 14. 운영 권장값

일반적인 월별 비교는 다음 설정을 권장한다.

    target_frequency = monthly
    aggregation_method = last
    relationship_transform_type = raw
    max_lag_periods = 12
    min_pair_count = 24
    min_candidate_score = 0.3
    include_quality_warnings = true

주가지수와 거시지표 비교는 다음처럼 공통 분석 구간을 지정하는 것이 좋다.

    target_series_id = NASDAQCOM 또는 SP500
    analysis_start_date = 2000-01-01 또는 2010-01-01
    target_frequency = monthly
    relationship_transform_type = pct_change_1 또는 pct_change_12
    min_pair_count = 24

데이터 시작 시점이 서로 크게 다르면 analysis_start_date를 명시해 공통 비교 구간을 잡아야 한다. 그렇지 않으면 coverage_rate가 낮아져 실제 상관관계가 있어도 candidate_score가 낮게 나올 수 있다.

## 15. 해석 주의사항

fred_relationship_candidate_scores는 인과관계를 증명하는 테이블이 아니다. target과 candidate 사이의 lag별 선형 관계를 빠르게 탐색하기 위한 screening 결과다.

해석 시 반드시 함께 확인할 값은 다음과 같다.

    r_squared
    coverage_rate
    pair_count
    target_period_count
    lag_periods
    transform_type
    quality_warning_count
    outlier_count

특히 pair_count가 작거나 coverage_rate가 낮거나 target과 candidate의 관측 시작 시점이 크게 다르면 점수 해석에 주의해야 한다. 최종 판단에는 경제 이론, 시차 구조 검토, out-of-sample backtest, robustness check가 필요하다.
