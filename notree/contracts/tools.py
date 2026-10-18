"""MCP 도구 계약 (팀 결정 8번).

규칙
  - 이름: `영역_동사`, 소문자·밑줄. (OpenAI 함수 이름은 점을 허용하지 않아 모델에 넘기는 도구와 이름을 맞추기 위함)
  - worker가 부르는 도구는 모두 `permit` 인자를 받는다 (job_start 제외 — permit을 발급하는 쪽).
    permit은 공통 인자라 아래 In 모델에는 넣지 않는다. 사용자·과목처럼 permit으로 알 수 있는 값은 인자로 받지 않는다.
  - 쓰기 도구는 `idem_key`(= job_id)를 받는다. 삭제·취소 영향을 받는 쓰기는 `expected_generation`도 받는다.
  - 응답은 envelope.py의 공통 형식. 아래 Out 모델은 envelope의 data 부분이다.
  - 바꾸는 규칙: 선택 필드 추가는 자유, 삭제·이름 변경·의미 변경은 쓰는 쪽(caller)·리뷰 담당과 합의 후.

이 파일이 기준이고, docs/tool-contracts.md는 여기서 자동 생성한다 (uv run python -m notree.dev.gen_tool_docs).
"""
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, Field

# ─────────────────────────────────────────────────────────────────────
# 작업 제어 (api / scheduler / worker 루프)
# ─────────────────────────────────────────────────────────────────────


class JobCreateIn(BaseModel):
    kind: str = Field(description="작업 종류 (핸들러 이름)")
    payload: dict[str, Any] = Field(description="핸들러 입력. Q&A·채점에 deadline_at이 없으면 서버가 요청 시각 + 90초로 넣는다")
    user_id: str = Field(description="요청한 사용자. permit에 담겨 MCP 서버의 RLS 기준이 된다")
    stream: str | None = Field(None, description="생략하면 작업 종류별 기본 Stream")


class JobCreateOut(BaseModel):
    job_id: str


class JobReservePendingIn(BaseModel):
    limit: int = 10
    stream: str | None = Field(None, description="이 레인(Stream)의 의도만 고른다. 생략하면 모든 레인을 생성 순서로 (레인 분리 전 방식)")


class ReservedJob(BaseModel):
    job_id: str
    kind: str
    stream: str
    gen: int | None = Field(None, description="전달 세대. XADD 메시지에 함께 넣는다 (Redis 유실을 발견할 때마다 DB가 올림)")


class JobListOut(BaseModel):
    jobs: list[ReservedJob]


class JobIdIn(BaseModel):
    job_id: str


class JobMarkSentOut(BaseModel):
    marked: bool


class JobResendStaleIn(BaseModel):
    reserved_older_than_sec: float = Field(30, description="허가 후 이만큼 지나도 전송 완료가 아니면 다시 보냄")
    sent_older_than_sec: float = Field(120, description="전송 완료 후 이만큼 아무도 시작하지 않으면 다시 보냄")
    limit: int = 50


class JobStartIn(BaseModel):
    job_id: str
    worker: str
    lease_sec: float = Field(description="DB lease 길이. 0이면 DB lease로 실행 중을 판단하지 않음 "
                                         "(NOTREE_LIVENESS=redis: 실행 중 판단은 Redis 소유 키, 실행권은 exec_epoch)")
    dispatch_gen: int | None = Field(None, description="메시지의 전달 세대. 지금 세대보다 작으면(Redis 유실 전 메시지) stale_dispatch로 거절")


class JobStartOut(BaseModel):
    status: Literal["started", "done", "failed", "leased", "unknown", "stale_dispatch"]
    exec_epoch: int | None = Field(None, description="started일 때 이번 실행 세대 (영속·단조). Redis 소유 키 모드는 이 값을 키에 담는다")
    kind: str | None = None
    payload: dict[str, Any] | None = None
    attempt: int | None = Field(None, description="1부터. 이번이 몇 번째 시도인지")
    previous_owner: str | None = None
    permit: str | None = Field(None, description="이후 모든 도구 호출에 첨부")
    owner: str | None = Field(None, description="status=leased일 때 현재 소유자")
    lease_left_sec: float | None = None


class JobHeartbeatIn(BaseModel):
    job_id: str
    worker: str
    lease_sec: float


class JobHeartbeatOut(BaseModel):
    renewed: bool = Field(description="false면 lease를 잃은 것 → 하던 일을 멈춘다")


class JobWorkerIn(BaseModel):
    job_id: str
    worker: str


class JobFinishOut(BaseModel):
    finished: bool


class JobFailIn(BaseModel):
    job_id: str
    worker: str
    reason: str
    user_retryable: bool = Field(description="사용자에게 '다시 시도' 버튼을 보여 줄지 (영구 실패면 false)")


class JobFailOut(BaseModel):
    failed: bool
    done_instead: bool = Field(False, description="이 작업의 결과가 이미 저장돼 있어 실패 대신 완료(done)로 확정함 "
                                                  "(저장 직후 응답만 잃은 경우). 이때도 XACK 해도 된다")


class JobReleaseIn(BaseModel):
    job_id: str
    worker: str
    reason: str


class JobReleaseOut(BaseModel):
    released: bool


class JobRetryIn(BaseModel):
    job_id: str
    deadline_at: float | None = Field(None, description="새 마감. 없으면 Q&A·채점은 지금 + 90초, 그 밖의 작업은 기존 값 유지")


class JobRetryOut(BaseModel):
    revived: bool
    reason: str | None = None


class JobRecoverIn(BaseModel):
    start: bool = Field(False, description="Redis 초기화를 방금 발견함 → 전달 세대를 올림")
    after: str | None = Field(None, description="이전 쪽의 next_after (job_id 순서로 이어서)")
    limit: int = 500


class JobRecoverOut(BaseModel):
    jobs: list[ReservedJob] = Field(description="다시 XADD할 작업 (sent로 표시됨)")
    failed: list[str] = Field(description="마감이 지났거나 시도 한도를 다 써서 실패 확정한 작업")
    done_instead: list[str] = Field(default_factory=list,
                                    description="마감·시도 한도에 걸렸지만 결과가 이미 저장돼 있어 완료로 확정한 작업")
    held: int = Field(0, description="lease가 아직 살아 있어 보류한 running (다음 주기에 다시 확인)")
    open: bool = Field(False, description="복구가 아직 안 끝남 (대상이 남아 있음)")
    next_after: str | None = Field(None, description="이 쪽이 꽉 찼으면 다음 쪽 시작점")


class JobRow(BaseModel):
    found: bool
    job_id: str | None = None
    kind: str | None = None
    user_id: str | None = None
    payload: dict[str, Any] | None = None
    stream: str | None = None
    state: str | None = None
    lease_owner: str | None = None
    lease_until: float | None = None
    attempts: int | None = None
    exec_epoch: int | None = Field(None, description="실행 세대. job_start마다 1씩 오르고 되돌아가지 않음 (늦은 쓰기 차단)")
    fail_reason: str | None = None
    user_retryable: bool | None = None
    created: float | None = None
    updated_at: float | None = None


# ─────────────────────────────────────────────────────────────────────
# 체크포인트 (LangGraph 저장기 전용)
# ─────────────────────────────────────────────────────────────────────


class CheckpointPutIn(BaseModel):
    thread_id: str
    ns: str
    checkpoint_id: str
    parent_id: str | None
    checkpoint: list[str] = Field(description="[serde 타입, base64]")
    metadata: list[str] = Field(description="[serde 타입, base64]")


class CheckpointPutWritesIn(BaseModel):
    thread_id: str
    ns: str
    checkpoint_id: str
    task_id: str
    task_path: str
    writes: list[list[Any]] = Field(description="[[idx, channel, v_type, v_b64], ...]")


class CheckpointGetIn(BaseModel):
    thread_id: str
    ns: str = ""
    checkpoint_id: str | None = None


class CheckpointItem(BaseModel):
    thread_id: str
    ns: str
    checkpoint_id: str
    parent_id: str | None
    checkpoint: list[str]
    metadata: list[str]
    writes: list[list[Any]]


class CheckpointGetOut(BaseModel):
    found: bool
    item: CheckpointItem | None = None


class CheckpointListIn(BaseModel):
    thread_id: str
    ns: str | None = None
    before: str | None = None
    limit: int | None = None


class CheckpointListOut(BaseModel):
    items: list[CheckpointItem]


class ThreadIn(BaseModel):
    thread_id: str


class Empty(BaseModel):
    pass


class CheckpointCleanupIn(BaseModel):
    older_than_sec: float = Field(7 * 24 * 3600, description="완료·실패 후 이만큼 지난 작업의 체크포인트 삭제")


class CheckpointCleanupOut(BaseModel):
    deleted_threads: int


# ─────────────────────────────────────────────────────────────────────
# 노트 발행 (PoC: 과목 노트 그래프의 publish 노드)
# ─────────────────────────────────────────────────────────────────────


class NotePublishIn(BaseModel):
    idem_key: str
    course: str
    note: str
    status: str


class NotePublishOut(BaseModel):
    idem_key: str


class NoteGetIn(BaseModel):
    idem_key: str


class NoteGetOut(BaseModel):
    found: bool
    user_id: str | None = None
    course: str | None = None
    note: str | None = None
    status: str | None = None


class ToolStatsOut(BaseModel):
    counts: dict[str, int]


# ─────────────────────────────────────────────────────────────────────
# 계획된 도구 (설계서 3절). 담당자가 PR로 필드를 확정하고 서버에 구현한다.
# ─────────────────────────────────────────────────────────────────────


class TranscriptRangeIn(BaseModel):
    lecture_id: str
    after_seq: int | None = Field(None, description="이 번호 이후의 확정 전사만. 없으면 요약이 끝난 위치(processed_until) 이후")


class TranscriptRangeOut(BaseModel):
    state: Literal["recording", "ended", "cancelled"]
    rolling_summary: str
    new_text: str
    from_seq: int = Field(description="이번 범위의 첫 번호 (= processed_until + 1)")
    last_seq: int = Field(description="이번 범위의 끝 번호 (= 읽은 시점의 saved_until). from_seq보다 작으면 새 전사 없음")
    generation: int = Field(description="발행할 때 expected_generation으로 다시 넘긴다")
    already_published: bool = Field(False, description="이 작업(permit)의 요약이 이미 저장됨 → 다시 요약하지 않고 완료")


class SummarySegmentsIn(BaseModel):
    lecture_id: str


class SummarySegment(BaseModel):
    seq_from: int
    seq_to: int
    text: str


class SummarySegmentsOut(BaseModel):
    segments: list[SummarySegment]
    missing_ranges: list[list[int]]
    generation: int


class SummaryPublishIn(BaseModel):
    idem_key: str
    lecture_id: str
    expected_generation: int
    from_seq: int = Field(description="transcript_range가 준 from_seq. 요약 위치가 그 사이 움직였으면 stale")
    upto_seq: int
    text: str


class SummaryPublishOut(BaseModel):
    status: Literal["published", "duplicate", "stale"]
    summary_version: int
    processed_until: int


class LectureStartIn(BaseModel):
    lecture_id: str
    user_id: str


class LectureIn(BaseModel):
    lecture_id: str
    user_id: str


class LectureOut(BaseModel):
    lecture_id: str
    state: str
    generation: int
    saved_until: int
    processed_until: int
    attempted_until: int
    rolling_summary: str
    summary_version: int


class TranscriptCommitIn(BaseModel):
    lecture_id: str
    user_id: str
    segments: list[dict[str, Any]] = Field(description="[{seq, text}] 확정 전사. seq는 saved_until+1부터 빈틈 없이 (이미 저장된 번호는 무시)")


class TranscriptCommitOut(BaseModel):
    saved_until: int
    added: int = Field(description="이번 커밋으로 새로 저장된 전사 수. 0이면 재전송·빈 묶음 → 요약을 등록하지 않음")
    enqueued_job_id: str | None = Field(None, description="이번 커밋이 새로 등록한 요약 작업")
    coalesced: bool = Field(description="새 전사가 있었지만 대기 중 요약이 이미 있어 그 요약에 합쳐짐")


class SummaryRecoverIn(BaseModel):
    limit: int = 50


class SummaryRecoverOut(BaseModel):
    registered: list[str] = Field(description="새로 등록한 요약 작업 job_id")


class NotesReadIn(BaseModel):
    note_id: str
    block_ids: list[str] | None = None


class NoteBlock(BaseModel):
    block_id: str
    text: str
    version: int


class NotesReadOut(BaseModel):
    note_id: str
    blocks: list[NoteBlock]
    generation: int


class NotesSearchIn(BaseModel):
    course_id: str
    query: str
    limit: int = 8


class NotesSearchHit(BaseModel):
    note_id: str
    block_id: str
    snippet: str
    score: float


class NotesSearchOut(BaseModel):
    hits: list[NotesSearchHit]


class LectureNotePublishIn(BaseModel):
    idem_key: str
    lecture_id: str
    expected_generation: int
    status: Literal["완료", "부분 완료"]
    blocks: list[NoteBlock]
    missing_ranges: list[list[int]] = []


class LectureNotePublishOut(BaseModel):
    note_id: str
    version: int


# ── Q&A (2026-10-17: 그래프 notree/graphs/qa의 검색 인터페이스·결과 형식에 맞춤) ──
# **잠정 계약**: 계약 담당(T5) 확인 전이다. 화면·API가 확정 계약으로 의존하기 전에 확인받고, 바뀌면 이 표시를 지운다.
# 흐름: api qa_ask(질문 + 작업 의도를 한 트랜잭션) → worker qa_context → qa_search·web_search(그래프가 여러 번)
#       → qa_complete(멱등, 세대 확인) → api qa_get(화면 조회). 노트 반영 제안·사용자 승인은 첫 통합에서 뺌.
QaStatus = Literal["completed", "부분 답변", "근거 부족", "시간 초과"]
QaLabel = Literal["강의", "웹", "일반 지식", "근거 부족"]
QaNotice = Literal["not_found", "insufficient", "timeout"]


class QaAskIn(BaseModel):
    user_id: str = Field(description="질문한 사용자 (실제로는 api가 세션에서 꺼냄)")
    course_id: str
    question: str


class QaAskOut(BaseModel):
    question_id: str
    job_id: str = Field(description="qa 작업. 마감(deadline_at)은 요청 시각 + 90초")


class QaContextIn(BaseModel):
    question_id: str


class QaContextOut(BaseModel):
    question: str
    course_id: str
    course_name: str = Field(description="LLM에 주는 과목 이름")
    lecture_ids: list[str] = Field(description="과목의 강의 목록 (전사 검색 범위를 좁힐 때)")
    generation: int = Field(description="질문 세대. 삭제·수정되면 바뀐다 → qa_complete의 expected_generation")
    recent_turns: list[str] = Field([], description="같은 과목의 최근 대화 (첫 통합에서는 비어 있음)")
    answered: bool = Field(False, description="이 작업의 답이 이미 저장됨 (저장 응답을 못 받고 재시도된 경우) → 그래프를 건너뛰고 완료")


class QaSearchIn(BaseModel):
    question_id: str = Field(description="검색 범위(사용자·과목)는 질문에서 정한다")
    source: Literal["note", "transcript"]
    keywords: list[str]
    limit: int = 3
    lecture_ids: list[str] | None = Field(None, description="전사 검색을 이 강의들로 좁힘. 없으면 과목 전체")


class QaHit(BaseModel):
    id: str = Field(description="근거 ID. 답변 인용과 같은 값. **출처(노트·전사·웹) 전체에서 유일해야 한다** — "
                               "그래프는 같은 ID에 다른 출처·원문이 오면 뒤에 온 것을 거절한다")
    source: Literal["note", "transcript", "web"]
    text: str
    score: float = Field(description="0~1. 그래프가 '충분한지' 판단에 씀")
    ref: str = Field(description="화면 출처 표시 (노트 블록, 전사 구간, URL)")
    lecture_id: str | None = None


class QaSearchOut(BaseModel):
    hits: list[QaHit]


class QaSentence(BaseModel):
    text: str
    citations: list[str]


class QaSource(BaseModel):
    id: str
    source: Literal["note", "transcript", "web"]
    text: str
    ref: str


class QaCompleteIn(BaseModel):
    idem_key: str = Field(description="= job_id. 같은 키로 두 번 오면 처음 결과를 돌려주고 duplicate=true")
    question_id: str
    expected_generation: int = Field(description="qa_context에서 받은 세대. 다르면 generation_mismatch (질문이 삭제·수정됨)")
    status: QaStatus
    label: QaLabel
    answer: str = Field(description="화면용 본문. 인용 표시 [^ID] 포함 (코드가 만든 것)")
    sentences: list[QaSentence]
    sources: list[QaSource] = Field(description="인용된 근거만. 문장의 인용은 모두 여기에 있어야 함 (없으면 invalid_args)")
    notice_code: QaNotice | None = None


class QaCompleteOut(BaseModel):
    answer_id: str
    reflect_suggested: bool = Field(False, description="노트 반영 제안 여부 (첫 통합에서는 항상 false)")


class QaGetIn(BaseModel):
    question_id: str
    user_id: str


class QaGetOut(BaseModel):
    found: bool
    question: str | None = None
    state: Literal["pending", "answered", "deleted"] | None = None
    job_id: str | None = None
    answer: dict[str, Any] | None = Field(None, description="저장된 결과 (status, label, answer, sentences, citations, sources, notice_code). "
                                                             "citations는 서버가 문장별 인용을 순서대로 중복 없이 모은 것. 삭제된 질문은 question·answer 모두 null")


class QaDeleteIn(BaseModel):
    """삭제하면 질문 원문·답변·출처 복사본을 바로 지운다 (PoC 보관 정책: 보관 안 함). 행은 세대 확인용으로 남김."""
    question_id: str
    user_id: str


class QaDeleteOut(BaseModel):
    deleted: bool


class QaSeedIn(BaseModel):
    user_id: str
    course_id: str
    course_name: str
    docs: list[dict[str, Any]] = Field(description="[{source: note|transcript|web, id, text, lecture_id?, ref?}]")


class QaSeedOut(BaseModel):
    inserted: int


class WebSearchIn(BaseModel):
    question_id: str = Field(description="어떤 질문의 검색인지 (사용자별 한도·감사용. PoC는 그 과목에 시드한 web 문서에서 찾음)")
    query: str
    limit: int = 3


class WebSearchOut(BaseModel):
    results: list[dict[str, Any]] = Field(description="[{id, title, url, snippet, score, fetched_at}]")


class GradeContextIn(BaseModel):
    answer_id: str


class GradeContextOut(BaseModel):
    question: str
    rubric: list[dict[str, Any]]
    rubric_version: int
    answer: str
    evidence_blocks: list[NoteBlock]


class GradeSaveIn(BaseModel):
    idem_key: str
    answer_id: str
    rubric_version: int
    scores: list[dict[str, Any]] = Field(description="[{element_id, score: 0|0.5|1, note}]")
    misconception: bool
    held: bool = Field(description="모호해서 보류 → FSRS에 기록하지 않음")


class GradeSaveOut(BaseModel):
    saved: bool


class QuestionPublishIn(BaseModel):
    idem_key: str
    course_id: str
    questions: list[dict[str, Any]] = Field(description="[{concept_id, prompt, rubric, source_block_ids}]")


class QuestionPublishOut(BaseModel):
    question_ids: list[str]


# ─────────────────────────────────────────────────────────────────────
# 명세 목록
# ─────────────────────────────────────────────────────────────────────

Caller = Literal["worker", "api", "scheduler", "dev"]
Kind = Literal["control", "read", "write", "external", "dev"]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    input: type[BaseModel]
    output: type[BaseModel]
    caller: Caller
    kind: Kind
    owner: str                       # 입출력을 정의하는 담당
    reviewer: str = ""               # 그 도구가 읽고 쓰는 테이블의 담당
    model_exposed: bool = False      # OpenAI tool calling에 그대로 넘기는 도구 (설명도 계약)
    status: Literal["implemented", "planned"] = "implemented"
    summary: str = ""


W, API, SCH, DEV = "worker", "api", "scheduler", "dev"
LEAD = "worker 담당"

TOOLS: dict[str, ToolSpec] = {s.name: s for s in [
    # 작업 제어
    ToolSpec("job_create", JobCreateIn, JobCreateOut, API, "control", LEAD, summary="의도(intent) 기록. 실제로는 api가 PG에 직접"),
    ToolSpec("job_reserve_pending", JobReservePendingIn, JobListOut, SCH, "control", LEAD, summary="대기 의도 → 실행 허가. stream을 주면 그 레인만 (배정기가 레인별로 부름)"),
    ToolSpec("job_mark_sent", JobIdIn, JobMarkSentOut, SCH, "control", LEAD, summary="XADD 후 전송 완료 표시"),
    ToolSpec("job_resend_stale", JobResendStaleIn, JobListOut, SCH, "control", LEAD, summary="오래 멈춘 reserved/sent 의도를 다시 보낼 목록 (결정 6)"),
    ToolSpec("job_recover_orphans", JobRecoverIn, JobRecoverOut, SCH, "control", LEAD,
             summary="Redis 통째 유실 뒤: 마지막 전달이 이전 세대인 reserved·sent·lease 끝난 running을 새 세대로 다시 보낼 목록 "
                     "(살아 있는 lease는 보류, 쪽 나누기). 마감·시도 한도는 실패 확정(결과가 저장돼 있으면 완료). 대상이 없으면 복구 종료"),
    ToolSpec("job_start", JobStartIn, JobStartOut, W, "control", LEAD,
             summary="실행 시작 + lease + permit(실행 세대 포함) 발급. 살아 있는 lease면 같은 worker라도 leased. "
                     "lease_sec=0이면 DB lease 없이 실행 세대만 올림 (Redis 소유 키 모드: 받은 세대를 소유 키에 담음)"),
    ToolSpec("job_heartbeat", JobHeartbeatIn, JobHeartbeatOut, W, "control", LEAD,
             summary="lease 연장. 실행권(소유자·실행 세대)이 없으면 renewed=false. Redis 소유 키 모드에서는 부르지 않음"),
    ToolSpec("job_finish", JobWorkerIn, JobFinishOut, W, "control", LEAD,
             summary="완료 표시. finished=true를 확인한 뒤에만 XACK"),
    ToolSpec("job_fail", JobFailIn, JobFailOut, W, "control", LEAD, summary="실패 확정. 같은 트랜잭션에서 이 작업의 결과가 이미 저장됐는지 보고, 저장돼 있으면 "
                     "완료로 확정(done_instead). failed 또는 done_instead를 확인한 뒤에만 XACK"),
    ToolSpec("job_release", JobReleaseIn, JobReleaseOut, W, "control", LEAD, summary="일시적 실패: lease 반납, 재배송 대기"),
    ToolSpec("job_retry", JobRetryIn, JobRetryOut, API, "control", LEAD, summary="사용자의 '다시 시도': failed → pending (결정 7)"),
    ToolSpec("job_get", JobIdIn, JobRow, DEV, "dev", LEAD, summary="작업 상태 조회"),
    # 체크포인트
    ToolSpec("checkpoint_put", CheckpointPutIn, Empty, W, "write", LEAD, summary="LangGraph 단계 상태 저장"),
    ToolSpec("checkpoint_put_writes", CheckpointPutWritesIn, Empty, W, "write", LEAD, summary="노드 결과(pending writes) 저장"),
    ToolSpec("checkpoint_get", CheckpointGetIn, CheckpointGetOut, W, "read", LEAD, summary="체크포인트 조회 (없으면 최신)"),
    ToolSpec("checkpoint_list", CheckpointListIn, CheckpointListOut, W, "read", LEAD, summary="체크포인트 이력"),
    ToolSpec("checkpoint_delete_thread", ThreadIn, Empty, W, "write", LEAD, summary="thread 체크포인트 삭제"),
    ToolSpec("checkpoint_cleanup", CheckpointCleanupIn, CheckpointCleanupOut, SCH, "write", LEAD, summary="끝난 작업 체크포인트 정리 (결정 9)"),
    # 발행 (PoC)
    ToolSpec("note_publish", NotePublishIn, NotePublishOut, W, "write", "T3·T4", summary="PoC 과목 노트 발행. 사용자 ID는 permit에서"),
    ToolSpec("note_get", NoteGetIn, NoteGetOut, DEV, "dev", "T3·T4", summary="발행된 노트 조회"),
    ToolSpec("tool_stats", ThreadIn, ToolStatsOut, DEV, "dev", LEAD, summary="도구 호출 횟수 (실행 흐름 확인용)"),
    # 계획된 도구
    ToolSpec("transcript_range", TranscriptRangeIn, TranscriptRangeOut, W, "write", "T3·T4", "T1·T2", True,
             summary="[PoC] 롤링 요약 + 요약 위치 이후 확정 전사. **상태 변경**: 읽은 끝 번호를 attempted_until로 남김 "
                     "(누락 복구 판단용) → 이 강의의 live_summary 작업의 현재 실행권이 있어야 함"),
    ToolSpec("summary_segments", SummarySegmentsIn, SummarySegmentsOut, W, "read", "T3·T4", "", False, "planned",
             "최종 노트용 구간 요약 목록"),
    ToolSpec("summary_publish", SummaryPublishIn, SummaryPublishOut, W, "write", "T3·T4",
             summary="[PoC] 실시간 요약 저장 + 요약 위치 이동을 한 트랜잭션에. 멱등(idem_key), 세대·실행권·위치(from_seq), "
                     "작업↔강의 연결(종류 live_summary·같은 lecture_id·같은 사용자) 확인. duplicate도 연결 확인"),
    ToolSpec("lecture_start", LectureStartIn, LectureOut, API, "write", "T3·T4", summary="[PoC] 녹음 시작 (강의 행)"),
    ToolSpec("transcript_commit", TranscriptCommitIn, TranscriptCommitOut, API, "write", "T3·T4",
             summary="[PoC] 확정 전사 묶음 저장 + **새 전사가 실제로 저장됐고** 대기 중 요약이 없으면 요약 작업 1건 등록 "
                     "(강의당 대기 1개, DB 유일 인덱스). 재전송·빈 묶음은 상태만 돌려줌"),
    ToolSpec("lecture_end", LectureIn, LectureOut, API, "write", "T3·T4",
             summary="[PoC] 녹음 종료: 상태 ended, 세대 +1 (늦은 실시간 요약이 덮지 못하게)"),
    ToolSpec("lecture_get", LectureIn, LectureOut, DEV, "dev", "T3·T4", summary="[PoC] 강의 상태·요약 위치 조회"),
    ToolSpec("summary_recover", SummaryRecoverIn, SummaryRecoverOut, SCH, "control", "T3·T4",
             summary="[PoC] 누락 복구: 녹음 중이고 아직 아무도 시도 안 한 전사가 있는데 요약 작업이 없는 강의에 1건 등록"),
    ToolSpec("notes_read", NotesReadIn, NotesReadOut, W, "read", "T3·T4", "T5", True, "planned",
             "노트 블록을 읽는다"),
    ToolSpec("notes_search", NotesSearchIn, NotesSearchOut, W, "read", "T3·T4", "T5", True, "planned",
             "과목 노트에서 질문과 관련된 블록을 찾는다 (pg_trgm)"),
    ToolSpec("lecture_note_publish", LectureNotePublishIn, LectureNotePublishOut, W, "write", "T3·T4", "", False,
             "planned", "강의 최종 노트 발행 (완료/부분 완료)"),
    ToolSpec("qa_ask", QaAskIn, QaAskOut, API, "write", "T5", "T3·T4",
             summary="[잠정 · T5 확인 전] 질문 저장 + qa 작업 의도를 한 트랜잭션에 (PoC: SQLite)"),
    ToolSpec("qa_context", QaContextIn, QaContextOut, W, "read", "T5", "T3·T4",
             summary="[잠정 · T5 확인 전] 질문·과목·강의 목록·세대. 사용자는 permit에서"),
    ToolSpec("qa_search", QaSearchIn, QaSearchOut, W, "read", "T5", "T3·T4",
             summary="[잠정 · T5 확인 전] 노트·전사 근거 검색. PoC는 키워드 일치 점수, 실제는 pg_trgm(Read)"),
    ToolSpec("web_search", WebSearchIn, WebSearchOut, W, "external", "T5", "",
             summary="[잠정 · T5 확인 전] 웹 검색 1회. PoC는 시드한 web 문서에서 찾는 가짜 (실제 공급자 미정)"),
    ToolSpec("qa_complete", QaCompleteIn, QaCompleteOut, W, "write", "T5", "T3·T4",
             summary="[잠정 · T5 확인 전] 답변 저장. 멱등(idem_key), 질문 세대가 그대로일 때만"),
    ToolSpec("qa_get", QaGetIn, QaGetOut, API, "read", "T5", "T3·T4", summary="[잠정 · T5 확인 전] 화면 조회: 질문 상태와 저장된 답변"),
    ToolSpec("qa_delete", QaDeleteIn, QaDeleteOut, API, "write", "T5", "T3·T4",
             summary="[잠정 · T5 확인 전] 질문 삭제 (세대 증가 → 진행 중 답변 저장은 generation_mismatch)"),
    ToolSpec("qa_seed", QaSeedIn, QaSeedOut, DEV, "dev", "T5", summary="PoC 검색용 노트·전사·웹 문서 넣기"),
    ToolSpec("grade_context", GradeContextIn, GradeContextOut, W, "read", "T5", "", False, "planned",
             "채점용 문제·루브릭·답안·근거"),
    ToolSpec("grade_save", GradeSaveIn, GradeSaveOut, W, "write", "T5", "", False, "planned",
             "채점 결과 저장, 이해 상태·FSRS 갱신"),
    ToolSpec("question_publish", QuestionPublishIn, QuestionPublishOut, W, "write", "T5", "T3·T4", False, "planned",
             "복습 문제 발행 (노트 블록 근거)"),
]}

# 이 도구들은 permit을 받지 않는다 (permit을 발급하거나, worker가 아닌 쪽이 부름)
NO_PERMIT = {"job_start"} | {n for n, s in TOOLS.items() if s.caller != W}
