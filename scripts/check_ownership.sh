#!/usr/bin/env bash
# =============================================================
# check_ownership.sh — 파일 소유권 규칙 검증
#
# 역할:
#   git diff --cached 의 스테이징된 파일들이 단일 소유자의
#   영역만 수정하는지 확인한다.
#   여러 역할의 소유 파일이 한 커밋에 섞이면 차단한다.
#
# 사용법:
#   bash scripts/check_ownership.sh
#
# 종료 코드:
#   0 = 통과
#   1 = 소유권 위반
# =============================================================

# ── 파일 → 소유자 매핑 ────────────────────────────────────────
# 반환값: "A:김용" | "B:김은재" | "C:김규민" | "D:김원우" | "" (공통)
get_owner() {
    case "$1" in
        include/bptree.h|src/bptree/bptree.c)
            echo "A:김용" ;;
        include/index_manager.h|src/index/index_manager.c)
            echo "B:김은재" ;;
        src/input/lexer.c|src/parser/parser.c|src/schema/schema.c)
            echo "C:김규민" ;;
        src/executor/executor.c|src/main.c)
            echo "D:김원우" ;;
        *)
            echo "" ;; # 공통 파일 (누구나 수정 가능)
    esac
}

# ── 공통 파일이지만 전원 합의 필요한 파일 ──────────────────────
needs_consensus() {
    case "$1" in
        include/interface.h|Makefile)
            return 0 ;;
        *)
            return 1 ;;
    esac
}

# ── 스테이징된 파일 목록 확인 ──────────────────────────────────
STAGED=$(git diff --cached --name-only 2>/dev/null)

if [ -z "$STAGED" ]; then
    echo "[OWNERSHIP] 스테이징된 파일 없음. 검증 스킵."
    exit 0
fi

echo "[OWNERSHIP] 스테이징된 파일 검증 중..."

# ── 소유자 수집 ────────────────────────────────────────────────
OWNERS_FOUND=""
WARNED_CONSENSUS=0
VIOLATION_FILES=""

while IFS= read -r f; do
    owner=$(get_owner "$f")

    if [ -n "$owner" ]; then
        # 이미 수집된 소유자인지 확인
        if echo "$OWNERS_FOUND" | grep -qF "$owner"; then
            : # 이미 있음
        else
            OWNERS_FOUND="$OWNERS_FOUND $owner"
        fi
    fi

    # 전원 합의 필요 파일 경고
    if needs_consensus "$f"; then
        echo "  [WARN] '$f' 는 전원 합의 필요 파일입니다."
        WARNED_CONSENSUS=$((WARNED_CONSENSUS + 1))
    fi

done <<< "$STAGED"

# ── 소유자 수 계산 ─────────────────────────────────────────────
OWNER_COUNT=$(echo "$OWNERS_FOUND" | tr ' ' '\n' | grep -c '[A-D]:')

if [ "$OWNER_COUNT" -gt 1 ]; then
    echo ""
    echo "  [OWNERSHIP VIOLATION] 한 커밋에 여러 소유자의 파일이 섞여 있습니다:"
    echo "$OWNERS_FOUND" | tr ' ' '\n' | grep '[A-D]:' | while read -r o; do
        echo "    - $o"
    done
    echo ""
    echo "  스테이징 내용:"
    while IFS= read -r f; do
        owner=$(get_owner "$f")
        if [ -n "$owner" ]; then
            echo "    [$owner] $f"
        else
            echo "    [공통]   $f"
        fi
    done <<< "$STAGED"
    echo ""
    echo "  → 한 번에 한 역할의 파일만 커밋하세요."
    exit 1
fi

# ── 통과 ───────────────────────────────────────────────────────
if [ "$OWNER_COUNT" -eq 1 ]; then
    ROLE=$(echo "$OWNERS_FOUND" | tr ' ' '\n' | grep '[A-D]:')
    echo "  → 소유자: $ROLE"
fi
if [ "$WARNED_CONSENSUS" -gt 0 ]; then
    echo "  → 전원 합의 파일 포함. 팀 동의 확인 후 커밋."
fi
echo "[OWNERSHIP] PASSED"
exit 0
