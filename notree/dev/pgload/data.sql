SET search_path = app;
-- 데이터 사용자 200명 × 과목 2 × 강의 8 × 전사 800문장 = 256만 행, 노트 블록 강의당 40개
INSERT INTO users SELECT g FROM generate_series(1,200) g;
INSERT INTO courses SELECT u*10+c, u FROM generate_series(1,200) u, generate_series(1,2) c;
INSERT INTO lectures SELECT (u*10+c)*100+l, u*10+c, u FROM generate_series(1,200) u, generate_series(1,2) c, generate_series(1,8) l;
CREATE TEMP TABLE vocab AS SELECT array['스택','큐','힙','트리','그래프','정렬','탐색','재귀','오버플로','메모리','포인터','배열','연결','리스트','해시','충돌','노드','간선','가중치','최단','경로','동적','계획법','분할','정복','탐욕','알고리즘','시간','복잡도','공간','최악','평균','이진','균형','회전','삽입','삭제','갱신','순회','전위','중위','후위','레벨','부모','자식','루트','리프','깊이','너비','우선순위','버킷','기수','병합','퀵','피벗','안정','교수님이','설명했다','예를','들어','따라서','그러므로','중요하다','시험에','나온다','이','그','저','것은','때문에','경우','다음','앞에서','말한','구조','연산','호출','반환','값','입력','출력','조건','반복','변수','함수','객체','클래스','상속','인터페이스','추상화','캡슐화','다형성','프로세스','스레드','교착','상태','세마포어','뮤텍스','페이지','캐시','적중','실패','가상','주소'] AS w;
INSERT INTO transcript_segments(lecture_id, course_id, user_id, seq, text)
SELECT l.id, l.course_id, l.user_id, s,
  (SELECT string_agg(v.w[1+floor(random()*array_length(v.w,1))::int], ' ') FROM vocab v, generate_series(1, 9 + (s % 5)) k WHERE s > 0)
FROM lectures l, generate_series(1,800) s;
INSERT INTO note_blocks(lecture_id, course_id, user_id, text)
SELECT l.id, l.course_id, l.user_id,
  (SELECT string_agg(v.w[1+floor(random()*array_length(v.w,1))::int], ' ') FROM vocab v, generate_series(1, 25) k WHERE b > 0)
FROM lectures l, generate_series(1,40) b;
INSERT INTO sessions SELECT 's'||u, u, md5(u::text), now() FROM generate_series(1,1000) u;
