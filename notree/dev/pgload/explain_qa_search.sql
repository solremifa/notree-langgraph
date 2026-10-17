SET search_path=app,public; SET ROLE notree; SELECT set_config('app.user_id','37',false);
EXPLAIN (ANALYZE, BUFFERS, TIMING) SELECT id, text, word_similarity('스택 오버플로 재귀', text) s FROM transcript_segments
 WHERE course_id = 371 AND '스택 오버플로 재귀' <% text ORDER BY s DESC LIMIT 5;
