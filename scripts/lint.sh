#!/usr/bin/env bash
# =============================================================
# lint.sh — C 소스 컴파일 에러 검증
#
# 사용법:
#   bash scripts/lint.sh
#   bash scripts/lint.sh [추가 소스파일...]
#
# 종료 코드:
#   0 = 성공 (에러 없음)
#   1 = 실패 (컴파일 에러 존재)
# =============================================================

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CFLAGS="-std=c99 -Wall -Wextra -Iinclude -fsyntax-only"

# ── 컴파일러 탐색: 환경변수 CC → gcc → clang → cc ──────────────
find_compiler() {
    if [ -n "$CC" ] && command -v "$CC" >/dev/null 2>&1; then
        echo "$CC"; return
    fi
    for candidate in gcc clang cc; do
        if command -v "$candidate" >/dev/null 2>&1; then
            echo "$candidate"; return
        fi
    done
    echo ""
}

CC=$(find_compiler)

if [ -z "$CC" ]; then
    echo "[LINT] SKIP — C 컴파일러를 찾을 수 없습니다 (gcc / clang 미설치)"
    echo "         컴파일러 설치 후 재실행하거나, CC 환경변수로 경로를 지정하세요."
    echo "         예) CC=/usr/bin/gcc bash scripts/lint.sh"
    exit 0   # 도구 부재는 커밋을 막지 않는다
fi

# 메인 빌드 대상 소스 목록 (Makefile의 SRCS와 동일하게 유지)
MAIN_SRCS=(
    src/main.c
    src/input/input.c
    src/input/lexer.c
    src/parser/parser.c
    src/schema/schema.c
    src/executor/executor.c
    src/bptree/bptree.c
    src/index/index_manager.c
)

cd "$ROOT" || exit 1

echo "[LINT] 컴파일 검증 시작 (CC=$(command -v "$CC"))"
echo "[LINT] CFLAGS: $CFLAGS"
echo ""

FAILED=0

# ── 1. 전체 빌드 대상을 한 번에 검증 ──────────────────────────
echo "[1] 메인 빌드 대상 전체 검증..."
if $CC $CFLAGS "${MAIN_SRCS[@]}" 2>&1; then
    echo "    → OK"
else
    echo "    → FAILED"
    FAILED=$((FAILED + 1))

    # 실패 시 파일별로 개별 검증해 원인 특정
    echo ""
    echo "    [개별 파일 진단 중...]"
    for f in "${MAIN_SRCS[@]}"; do
        if [ ! -f "$f" ]; then
            echo "    [SKIP] $f (파일 없음)"
            continue
        fi
        if ! $CC $CFLAGS "$f" 2>/dev/null; then
            echo "    [ERROR] $f"
        fi
    done
fi

# ── 2. 테스트 파일 검증 (존재하는 파일만) ──────────────────────
echo ""
echo "[2] 테스트 파일 검증..."
TEST_OK=0
TEST_SKIP=0
for f in tests/test_*.c tools/gen_data.c; do
    if [ ! -f "$f" ]; then
        TEST_SKIP=$((TEST_SKIP + 1))
        continue
    fi
    if $CC $CFLAGS "$f" 2>/dev/null; then
        TEST_OK=$((TEST_OK + 1))
    else
        echo "    [ERROR] $f"
        $CC $CFLAGS "$f" 2>&1 | sed 's/^/            /'
        FAILED=$((FAILED + 1))
    fi
done
echo "    → OK: $TEST_OK  SKIP: $TEST_SKIP"

# ── 결과 ───────────────────────────────────────────────────────
echo ""
if [ $FAILED -eq 0 ]; then
    echo "[LINT] PASSED — 컴파일 에러 없음"
    exit 0
else
    echo "[LINT] FAILED — $FAILED 개 에러 발생"
    exit 1
fi
