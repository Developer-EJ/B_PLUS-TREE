#!/usr/bin/env bash
# =============================================================
# install_hooks.sh — git hooks 설치 스크립트
#
# 사용법 (프로젝트 루트에서):
#   bash scripts/install_hooks.sh
#
# 효과:
#   .githooks/ 안의 모든 훅을 .git/hooks/ 에 복사하고
#   실행 권한을 부여한다.
#   (기존 훅은 .bak 으로 백업 후 덮어쓴다)
# =============================================================

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/.githooks"
DST="$ROOT/.git/hooks"

# ── 경로 확인 ─────────────────────────────────────────────────
if [ ! -d "$SRC" ]; then
    echo "ERROR: .githooks/ 디렉토리가 없습니다. (ROOT=$ROOT)"
    exit 1
fi

if [ ! -d "$DST" ]; then
    echo "ERROR: .git/hooks/ 디렉토리가 없습니다. git 저장소인지 확인하세요."
    exit 1
fi

# ── 훅 설치 ───────────────────────────────────────────────────
echo "Installing git hooks..."
echo "  Source : $SRC"
echo "  Dest   : $DST"
echo ""

INSTALLED=0
for hook in "$SRC"/*; do
    [ -f "$hook" ] || continue
    name=$(basename "$hook")
    dst_file="$DST/$name"

    # 기존 훅 백업
    if [ -f "$dst_file" ] && [ ! -L "$dst_file" ]; then
        cp "$dst_file" "${dst_file}.bak"
        echo "  [backup] ${name}.bak"
    fi

    cp "$hook" "$dst_file"
    chmod +x "$dst_file"
    echo "  [install] $name → .git/hooks/$name"
    INSTALLED=$((INSTALLED + 1))
done

echo ""
echo "Done. $INSTALLED hook(s) installed."
echo ""
echo "동작 확인:"
echo "  git add <파일> && git commit -m 'test'  → 훅이 자동 실행됩니다."
echo "  수동 실행: bash .git/hooks/pre-commit"
