-- Notree 설계 문서·데이터 흐름 페이지를 따른 최소 스키마 (부하 측정용)
DROP SCHEMA IF EXISTS app CASCADE; CREATE SCHEMA app;
SET search_path = app;
CREATE TABLE users(id bigint PRIMARY KEY);
CREATE TABLE sessions(id text PRIMARY KEY, user_id bigint NOT NULL, csrf text, last_seen timestamptz, revoked bool DEFAULT false);
CREATE TABLE courses(id bigint PRIMARY KEY, user_id bigint NOT NULL);
CREATE TABLE lectures(id bigint PRIMARY KEY, course_id bigint NOT NULL, user_id bigint NOT NULL);
CREATE TABLE transcript_segments(id bigserial PRIMARY KEY, lecture_id bigint NOT NULL, course_id bigint NOT NULL,
  user_id bigint NOT NULL, seq int NOT NULL, text text NOT NULL, created timestamptz DEFAULT now());
CREATE TABLE note_blocks(id bigserial PRIMARY KEY, lecture_id bigint, course_id bigint NOT NULL, user_id bigint NOT NULL, text text NOT NULL);
CREATE TABLE audio_segments(id bigserial PRIMARY KEY, lecture_id bigint NOT NULL, seq int, uri text, bytes int, sha text);
CREATE TABLE summaries(lecture_id bigint PRIMARY KEY, user_id bigint, body text, version int DEFAULT 1, updated timestamptz DEFAULT now());
CREATE TABLE answer_drafts(user_id bigint, question_id bigint, body text, updated timestamptz, PRIMARY KEY(user_id, question_id));
CREATE TABLE qa(id bigserial PRIMARY KEY, user_id bigint, course_id bigint, question text, answer text, created timestamptz DEFAULT now());
CREATE TABLE outbox(id bigserial PRIMARY KEY, user_id bigint, topic text, payload jsonb, created timestamptz DEFAULT now());
CREATE TABLE jobs(job_id bigserial PRIMARY KEY, kind text, user_id bigint, lecture_id bigint, stream text,
  state text NOT NULL DEFAULT 'pending', payload jsonb, attempts int DEFAULT 0, exec_epoch int DEFAULT 0,
  lease_owner text, lease_until timestamptz, created timestamptz DEFAULT now(), updated_at timestamptz DEFAULT now(), fail_reason text);
CREATE INDEX jobs_state ON jobs(state, updated_at);
CREATE INDEX jobs_lease ON jobs(lease_until);              -- 누락 복구가 만료 lease를 찾는 용도 (heartbeat마다 갱신됨)
CREATE UNIQUE INDEX jobs_one_waiting ON jobs(lecture_id, kind) WHERE state IN ('pending','reserved','sent');  -- 강의당 대기 1개
CREATE INDEX ts_lecture ON transcript_segments(lecture_id, seq);
-- RLS: 사용자 범위
ALTER TABLE transcript_segments ENABLE ROW LEVEL SECURITY;
ALTER TABLE note_blocks ENABLE ROW LEVEL SECURITY;
CREATE POLICY own_ts ON transcript_segments USING (user_id = current_setting('app.user_id')::bigint);
CREATE POLICY own_nb ON note_blocks USING (user_id = current_setting('app.user_id')::bigint);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='notree') THEN CREATE ROLE notree LOGIN; END IF; END $$;
GRANT USAGE ON SCHEMA app TO notree; GRANT ALL ON ALL TABLES IN SCHEMA app TO notree; GRANT ALL ON ALL SEQUENCES IN SCHEMA app TO notree;
