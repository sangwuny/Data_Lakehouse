# Causal-Aware Time Series Lakehouse

FRED API 기반의 인과 분석용 불규칙 혼합빈도 시계열 데이터 레이크하우스 프로젝트다.

현재 구현 범위는 Bronze 계층이다.

## Bronze 계층

Bronze는 FRED API 원본 응답과 수집 맥락을 append-only 방식으로 저장한다.

주요 기능:

- `.env` 기반 FRED API key 관리
- 대표 경제 시계열 seed catalog 관리
- FRED metadata, observations, optional vintage dates 수집
- raw JSON 원본 저장
- Silver 입력용 series별 normalized JSONL 생성
- collection log와 run summary 생성
- request manifest에서 API key 자동 redaction
- 월별/분기별/연간 observation date의 추정 기간 경계 기록
- optional vintage date table 생성

## 준비

`.env` 파일에 다음 값을 둔다.

```env
FRED_API_KEY=your_fred_api_key_here
```

## 실행

대상 series 확인:

```bash
python -m src.pipelines.bronze --dry-run --priority core --limit 5
```

Bronze 수집:

```bash
python -m src.pipelines.bronze --priority core --limit 5
```

vintage date metadata 포함:

```bash
python -m src.pipelines.bronze --priority core --limit 5 --include-vintages
```
