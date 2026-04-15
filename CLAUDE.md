# Claude Code 진입점

이 저장소에서 작업하기 전에 **반드시** 아래 파일을 읽는다:

```
AGENT.md
```

AGENT.md 에는 다음 내용이 포함되어 있다:
- 팀 구성 및 파일 소유권 규칙 (위반 시 빌드 파괴)
- 아키텍처 및 모듈 의존 관계
- 핵심 인터페이스 계약 (bptree.h, index_manager.h, interface.h)
- 필수 코딩 규칙 (binary 파일 모드, 타이밍 측정, stderr/stdout 분리)
- 현재 구현 상태 (완료 vs 스텁)
- 미완료 작업 목록

AGENT.md 를 읽기 전에는 어떤 파일도 수정하지 않는다.
