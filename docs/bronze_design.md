# Bronze Layer Design

이 Bronze 계층은 FRED 원본 데이터를 손상 없이 보존하고, 이후 Silver/Gold 계층에서 인과 분석용 데이터셋을 만들 수 있도록 시간 맥락을 함께 남기는 것을 목표로 한다.

## 핵심 원칙

1. 원본 API 응답은 수정하지 않고 JSON으로 저장한다.
2. API key는 `.env`에만 저장하고, 로그와 manifest에는 기록하지 않는다.
3. 같은 series를 다시 수집해도 덮어쓰지 않고 `run_id`와 `collected_at` 기준으로 새 버전을 만든다.
4. `date`, `realtime_start`, `realtime_end`, `frequency`, `collection_time`을 함께 보존한다.
5. 실패한 요청도 collection log에 남겨 대량 수집의 재시작과 품질 점검이 가능하게 한다.
6. optional vintage date 수집이 실패해도 metadata와 observations 원본은 저장하고, vintage 실패 사유는 manifest와 collection log에 남긴다.

## 저장 구조

```text
data/bronze/fred/
  raw/
    source=fred/
      series_id=GDP/
        collected_at=20260615T120000Z/
          metadata.json
          observations.json
          vintages.json
          request_manifest.json
  tables/
    series_id=GDP/
      observations.jsonl
      metadata.jsonl
      vintage_dates.jsonl
  logs/
    collection_log.jsonl
    run_summary_<run_id>.json
```

## 시간 정합성 필드

`tables/series_id=<SERIES_ID>/observations.jsonl`에는 다음 필드를 둔다.

```text
series_id
observation_date
period_start_inferred
period_end_inferred
period_inference_basis
value_raw
realtime_start
realtime_end
frequency
frequency_short
units
seasonal_adjustment
collected_at_utc
run_id
observations_raw_path
metadata_raw_path
manifest_path
request_params_hash
```

여기서 `observation_date`는 값이 가리키는 경제 시점이고, `collected_at_utc`는 우리가 실제로 FRED에서 받은 시점이다. `realtime_start`와 `realtime_end`는 vintage 또는 revision-aware 분석으로 확장하기 위한 FRED의 실시간 기준 필드다.

`period_start_inferred`와 `period_end_inferred`는 월별, 분기별, 연간 데이터의 기간 경계를 분석 단계에서 명시적으로 다루기 위한 보조 필드다. Bronze에서는 값을 바꾸지 않고, FRED의 observation date와 frequency metadata를 기준으로 추정 경계만 남긴다.

통합 `tables/observations.jsonl`은 만들지 않는다. 수천 개 이상의 시계열을 다룰 때 단일 JSONL 파일이 너무 커져 탐색과 디버깅이 어려워지기 때문이다. 대신 series 단위로 partition된 `tables/series_id=<SERIES_ID>/...`를 Silver 계층의 입력으로 사용한다.

## 실행 예시

네트워크 호출 없이 대상 series만 확인:

```bash
python -m src.pipelines.bronze --dry-run --priority core --limit 5
```

핵심 series 5개 수집:

```bash
python -m src.pipelines.bronze --priority core --limit 5
```

vintage date metadata까지 포함:

```bash
python -m src.pipelines.bronze --priority core --limit 5 --include-vintages
```

특정 series만 수집:

```bash
python -m src.pipelines.bronze --series GDP UNRATE FEDFUNDS
```
