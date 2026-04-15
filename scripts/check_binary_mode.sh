#!/usr/bin/env bash
# =============================================================
# check_binary_mode.sh — fopen binary 모드 규칙 검증
#
# 검증 대상:
#   src/executor/executor.c
#   src/index/index_manager.c
#
# 규칙:
#   .dat 파일에 접근하는 fopen() 은 반드시 binary 모드를 사용해야 한다.
#   - INSERT 쓰기: "ab" (binary append)
#   - SELECT 읽기: "rb" (binary read)
#   - text 모드 ("r", "a", "w") 는 Windows에서 ftell 오프셋 오류를 일으킨다.
#
# 종료 코드:
#   0 = 통과
#   1 = 위반 발견
# =============================================================

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1

CRITICAL_FILES=(
    "src/executor/executor.c"
    "src/index/index_manager.c"
)

echo "[BINARY_MODE] fopen() 모드 검증 중..."

FAILED=0

for f in "${CRITICAL_FILES[@]}"; do
    if [ ! -f "$f" ]; then
        echo "  [SKIP] $f (파일 없음)"
        continue
    fi

    # text 모드 패턴 탐지: `, "r"`, `, "a"`, `, "w"` (b 없는 단독 모드)
    # 주석 행(//로 시작)은 제외
    VIOLATIONS=$(grep -n 'fopen' "$f" \
        | grep -v '^\s*//' \
        | grep -E ',\s*"[raw]"' \
        | grep -vE ',\s*"[raw][b+]')

    if [ -n "$VIOLATIONS" ]; then
        echo "  [FAIL] $f — text mode fopen 감지:"
        echo "$VIOLATIONS" | while IFS= read -r line; do
            echo "         $line"
        done
        echo "         → \"rb\" / \"ab\" / \"wb\" 로 변경하세요 (AGENT.md 섹션 6-1 참조)"
        FAILED=$((FAILED + 1))
    else
        echo "  [OK]   $f"
    fi
done

echo ""
if [ $FAILED -eq 0 ]; then
    echo "[BINARY_MODE] PASSED"
    exit 0
else
    echo "[BINARY_MODE] FAILED — $FAILED 개 파일에 text mode fopen 있음"
    exit 1
fi
