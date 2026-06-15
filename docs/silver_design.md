# Silver Layer Design

Silver 계층은 Bronze의 FRED 원본 보존 구조를 유지하면서 분석 가능한 정제 테이블을 만든다. 핵심 원칙은 값을 임의로 삭제하거나 덮어쓰지 않고, 결측치, 중복, 이상치 여부를 명시적인 flag로 남기는 것이다.

## 입력 구조

```text
data/bronze/fred/tables/
  series_id=GDP/
    collection_date=2026-06-15/
      run_id=20260615T120000Z/
        observations.jsonl
        metadata.jsonl
        vintage_dates.jsonl
```

## 출력 구조

```text
data/silver/fred/
  tables/
    series_id=GDP/
      collection_date=2026-06-15/
        run_id=20260615T120000Z/
          observations.jsonl
          quality_report.json
          lineage.json
  catalog/
    series_catalog.jsonl
  logs/
    lineage_events.jsonl
    silver_run_summary_<silver_run_id>.json
```

## 정제 원칙

1. `value_raw`는 보존하고, 숫자 변환 결과는 `value_numeric`에 저장한다.
2. FRED의 `"."`, 빈 문자열, null, 숫자 변환 실패는 결측 flag로 남긴다.
3. 결측치는 Silver에서 보간하지 않는다. 보간은 Gold 또는 분석 단계에서 선택한다.
4. 이상치는 삭제하지 않고 변화량(diff)의 robust z-score 기반 후보 flag로 남긴다. 장기 추세 자체를 이상치로 오판하지 않기 위해 level score는 참고값으로만 보존한다.
5. 같은 observation date라도 realtime 또는 값이 다르면 revision/vintage 가능성이 있으므로 삭제하지 않는다.
6. 완전 중복은 `is_duplicate`, `duplicate_group_id`, `duplicate_keep_candidate`로 표시한다.
7. Bronze raw path, manifest path, bronze run id, silver run id, transform version, lineage id를 보존한다.

## 주요 필드

```text
silver_run_id
bronze_run_id
collection_date
lineage_id
series_id
observation_date
period_start_inferred
period_end_inferred
realtime_start
realtime_end
value_raw
value_numeric
is_missing
missing_reason
is_duplicate
duplicate_type
is_observation_date_repeated
is_outlier
outlier_score
observations_raw_path
metadata_raw_path
manifest_path
transform_version
```

## Data Lineage

각 series마다 `lineage.json`을 생성하고, 전체 이벤트는 `logs/lineage_events.jsonl`에 append한다. lineage에는 Bronze 입력 경로, Silver 출력 경로, 변환 규칙, row count, 품질 요약이 포함된다.

## 실행 예시

대상 series 확인:

```bash
python -m src.pipelines.silver --dry-run --series GDP UNRATE
```

일부 series 처리:

```bash
python -m src.pipelines.silver --series GDP UNRATE
```

전체 처리:

```bash
python -m src.pipelines.silver
```

특정 Bronze run 기준 처리:

```bash
python -m src.pipelines.silver --run-id 20260615T091020Z
```
