/* =========================================================
 * gen_data.c — 대용량 INSERT SQL 생성기
 *
 * 공통 작업 파일 (담당자 지정 없음)
 *
 * 사용법:
 *   ./gen_data [count] [table]
 *   ./gen_data 1000000 users   → samples/bench_users.sql 생성
 *
 * 생성 형식:
 *   INSERT INTO users VALUES (1, 'user_0000001', 25, 'user1@example.com');
 *   INSERT INTO users VALUES (2, 'user_0000002', 26, 'user2@example.com');
 *   ...
 *
 * 주의:
 *   - id 는 1 부터 count 까지 선형적(linear)으로 고유하게 부여된다.
 *   - age 는 18~80 사이를 순환한다.
 * ========================================================= */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEFAULT_COUNT 1000000
#define DEFAULT_TABLE "users"

int main(int argc, char *argv[]) {
    int         count = DEFAULT_COUNT;
    const char *table = DEFAULT_TABLE;

    if (argc > 1) count = atoi(argv[1]);
    if (argc > 2) table = argv[2];

    if (count <= 0) {
        fprintf(stderr, "Usage: %s [count] [table]\n", argv[0]);
        return 1;
    }

    /* 출력 파일 경로: samples/bench_{table}.sql */
    char out_path[256];
    snprintf(out_path, sizeof(out_path),
             "samples/bench_%s.sql", table);

    FILE *fp = fopen(out_path, "w");
    if (!fp) {
        fprintf(stderr, "gen_data: cannot open '%s'\n", out_path);
        fprintf(stderr, "  (samples/ 디렉토리가 없으면 먼저 생성하세요)\n");
        return 1;
    }

    printf("Generating %d INSERT statements for table '%s'...\n",
           count, table);

    for (int i = 1; i <= count; i++) {
        int age = 18 + ((i - 1) % 63); /* 18 ~ 80 순환 */

        fprintf(fp,
                "INSERT INTO %s VALUES (%d, 'user_%07d', %d, "
                "'user%d@example.com');\n",
                table, i, i, age, i);

        /* 진행 상황 표시 (10만 건마다) */
        if (i % 100000 == 0) {
            printf("  %d / %d (%.0f%%)\n",
                   i, count, (double)i / count * 100.0);
        }
    }

    fclose(fp);
    printf("Done: %s\n", out_path);
    printf("Run with: ./sqlp %s\n", out_path);
    return 0;
}
