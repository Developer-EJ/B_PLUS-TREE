# 역할 A — B+ Tree 알고리즘

**담당자**: 김용  
**시작 가능**: Day 1 즉시 (외부 의존성 없음)

---

## 소유 파일 (이 파일만 수정 가능)

| 파일 | 설명 |
|------|------|
| `include/bptree.h` | B+ Tree 공개 인터페이스 (헤더) |
| `src/bptree/bptree.c` | B+ Tree 구현체 |

## 절대 수정 금지 파일

```
include/interface.h       ← 전원 합의 없이 수정 금지
include/index_manager.h   ← 김은재 소유
src/index/index_manager.c ← 김은재 소유
src/input/lexer.c         ← 김규민 소유
src/parser/parser.c       ← 김규민 소유
src/schema/schema.c       ← 김규민 소유
src/executor/executor.c   ← 김원우 소유
src/main.c                ← 김원우 소유
```

---

## 기능 명세

### 단일 키 B+ Tree

```c
BPTree *bptree_create(int order);
void    bptree_destroy(BPTree *tree);
int     bptree_insert(BPTree *tree, int key, long value);
long    bptree_search(BPTree *tree, int key);   // 없으면 -1
int     bptree_range(BPTree *tree, int from, int to,
                     long *out, int max_count); // BETWEEN 용
int     bptree_height(BPTree *tree);
void    bptree_print(BPTree *tree);             // 디버그 출력
```

- **key**: 레코드의 `id` (INT)
- **value**: `.dat` 파일에서 해당 행의 시작 오프셋 (`long`)
- 리프 노드는 양방향 링크드 리스트로 연결 (range query 지원)

### 복합 키 B+ Tree

```c
BPTreeComp *bptree_comp_create(int order);
void        bptree_comp_destroy(BPTreeComp *tree);
int         bptree_comp_insert(BPTreeComp *tree, int key1, int key2, long value);
long        bptree_comp_search(BPTreeComp *tree, int key1, int key2); // 없으면 -1
int         bptree_comp_height(BPTreeComp *tree);
```

- **key1**: `id`, **key2**: `age`
- 비교 기준: `(key1, key2)` 사전순 — `key1` 먼저, 같으면 `key2`

---

## 구현 요구사항

### 알고리즘

- [ ] **노드 분열(split)**: 리프 / 내부 노드 모두 구현
- [ ] **루트 분열**: 분열 시 `height++`
- [ ] **리프 링크드 리스트**: `bptree_range()` 의 O(k) 순회를 위해 필수

### `order` 파라미터 (트리 높이 제어)

| order | 의미 | 용도 |
|-------|------|------|
| `IDX_ORDER_DEFAULT` (128) | 낮은 트리, 빠른 탐색 | 운영 모드 |
| `IDX_ORDER_SMALL` (4) | 높은 트리, 느린 탐색 | 높이 비교 실험 |

### 디스크 I/O 시뮬레이션

`bptree_search` 내에서 레벨을 이동할 때마다 `IO_SLEEP()` 호출:

```c
// bptree.c 내부 — 이미 매크로 정의되어 있음
IO_SLEEP();  // BPTREE_SIMULATE_IO=1 일 때만 sleep 실행
```

빌드 방법:
```bash
make sim   # -DBPTREE_SIMULATE_IO=1 로 빌드
make       # 시뮬레이션 없이 빌드 (기본)
```

---

## 개발 순서

1. `BPNode` 내부 구조체 설계 (리프 / 내부 노드 구분)
2. `bptree_insert` → `bptree_search` → `bptree_range` 순서로 구현
3. `bptree_print` 로 구조 확인
4. `tests/test_bptree.c` 단위 테스트 통과 확인
5. 복합 키 버전 구현 (`BPTreeComp`)

---

## 단위 테스트 기준 (`tests/test_bptree.c`)

| 테스트 | 기대 결과 |
|--------|----------|
| 1 ~ 1,000,000 순삽입 후 search(500000) | offset 반환 |
| 역순 삽입(1,000,000 → 1) 후 search | 정확히 일치 |
| range(100, 200) 후 반환 개수 | 101 |
| search(존재하지 않는 id) | -1 |
| order=4 vs order=128 트리 높이 비교 | order=4 가 더 높음 |

---

## 지켜야 할 점

1. `bptree.h` 에 선언된 API 시그니처를 변경하면 **반드시 팀 전체에 공지**한다.
2. 내부 구조체(`BPNode`, `BPNodeComp`)는 `.c` 파일 안에 정의하고 헤더에 노출하지 않는다.
3. 모든 `malloc/calloc` 에 NULL 체크를 한다.
4. `bptree_destroy` 에서 메모리 누수가 없어야 한다 (valgrind 통과 기준).
