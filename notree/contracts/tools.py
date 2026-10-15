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
    payload: dict[str, Any] = Field(description="핸들러 입력")
    user_id: str = Field(description="요청한 사용자. permit에 담겨 MCP 서버의 RLS 기준이 된다")
    stream: str | None = Field(None, description="생략하면 작업 종류별 기본 Stream")


class JobCreateOut(BaseModel):
    job_id: str


class JobReservePendingIn(BaseModel):
    limit: int = 10


class ReservedJob(BaseModel):
    job_id: str
    kind: str
    stream: str


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
    lease_sec: float


class JobStartOut(BaseModel):
    status: Literal["started", "done", "failed", "leased", "unknown"]
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


class JobReleaseIn(BaseModel):
    job_id: str
    worker: str
    reason: str


class JobReleaseOut(BaseModel):
    released: bool


class JobRetryIn(BaseModel):
    job_id: str
    deadline_at: float | None = Field(None, description="마감이 있던 작업(Q&A)은 새 마감을 준다. 없으면 기존 값 유지")


class JobRetryOut(BaseModel):
    revived: bool
    reason: str | None = None


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
    after_seq: int = Field(description="이 번호 이후의 확정 전사만")


class TranscriptRangeOut(BaseModel):
    rolling_summary: str
    new_text: str
    last_seq: int
    generation: int = Field(description="발행할 때 expected_generation으로 다시 넘긴다")


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
    upto_seq: int
    text: str


class SummaryPublishOut(BaseModel):
    summary_version: int


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


class QaContextIn(BaseModel):
    question_id: str


class QaContextOut(BaseModel):
    question: str
    context_blocks: list[NoteBlock]
    recent_turns: list[str]
    token_budget_left: int
    generation: int


class QaCompleteIn(BaseModel):
    idem_key: str
    question_id: str
    expected_generation: int
    answer: str
    sources: list[dict[str, Any]] = Field(description="[{type: lecture|web|general, ref, fetched_at?}]")


class QaCompleteOut(BaseModel):
    answer_id: str
    reflect_suggested: bool = Field(description="노트 반영 제안 여부")


class WebSearchIn(BaseModel):
    query: str


class WebSearchOut(BaseModel):
    results: list[dict[str, Any]] = Field(description="[{title, url, snippet, fetched_at}]")


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
    ToolSpec("job_reserve_pending", JobReservePendingIn, JobListOut, SCH, "control", LEAD, summary="대기 의도 → 실행 허가"),
    ToolSpec("job_mark_sent", JobIdIn, JobMarkSentOut, SCH, "control", LEAD, summary="XADD 후 전송 완료 표시"),
    ToolSpec("job_resend_stale", JobResendStaleIn, JobListOut, SCH, "control", LEAD, summary="오래 멈춘 reserved/sent 의도를 다시 보낼 목록 (결정 6)"),
    ToolSpec("job_start", JobStartIn, JobStartOut, W, "control", LEAD, summary="실행 시작 + lease + permit 발급"),
    ToolSpec("job_heartbeat", JobHeartbeatIn, JobHeartbeatOut, W, "control", LEAD, summary="lease 연장"),
    ToolSpec("job_finish", JobWorkerIn, JobFinishOut, W, "control", LEAD, summary="완료 표시 (뒤에 XACK)"),
    ToolSpec("job_fail", JobFailIn, JobFailOut, W, "control", LEAD, summary="실패 확정 (뒤에 XACK)"),
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
    ToolSpec("transcript_range", TranscriptRangeIn, TranscriptRangeOut, W, "read", "T3·T4", "T1·T2", True, "planned",
             "강의의 롤링 요약과 지정 번호 이후 확정 전사를 읽는다"),
    ToolSpec("summary_segments", SummarySegmentsIn, SummarySegmentsOut, W, "read", "T3·T4", "", False, "planned",
             "최종 노트용 구간 요약 목록"),
    ToolSpec("summary_publish", SummaryPublishIn, SummaryPublishOut, W, "write", "T3·T4", "", False, "planned",
             "실시간 요약 저장 + outbox"),
    ToolSpec("notes_read", NotesReadIn, NotesReadOut, W, "read", "T3·T4", "T5", True, "planned",
             "노트 블록을 읽는다"),
    ToolSpec("notes_search", NotesSearchIn, NotesSearchOut, W, "read", "T3·T4", "T5", True, "planned",
             "과목 노트에서 질문과 관련된 블록을 찾는다 (pg_trgm)"),
    ToolSpec("lecture_note_publish", LectureNotePublishIn, LectureNotePublishOut, W, "write", "T3·T4", "", False,
             "planned", "강의 최종 노트 발행 (완료/부분 완료)"),
    ToolSpec("qa_context", QaContextIn, QaContextOut, W, "read", "T5", "T3·T4", False, "planned",
             "Q&A 16k 컨텍스트 구성"),
    ToolSpec("qa_complete", QaCompleteIn, QaCompleteOut, W, "write", "T5", "", False, "planned",
             "답변 저장 (근거가 그대로일 때만 completed)"),
    ToolSpec("web_search", WebSearchIn, WebSearchOut, W, "external", "T5", "", True, "planned",
             "웹 검색 1회 (Brave/OpenAI 미정)"),
    ToolSpec("grade_context", GradeContextIn, GradeContextOut, W, "read", "T5", "", False, "planned",
             "채점용 문제·루브릭·답안·근거"),
    ToolSpec("grade_save", GradeSaveIn, GradeSaveOut, W, "write", "T5", "", False, "planned",
             "채점 결과 저장, 이해 상태·FSRS 갱신"),
    ToolSpec("question_publish", QuestionPublishIn, QuestionPublishOut, W, "write", "T5", "T3·T4", False, "planned",
             "복습 문제 발행 (노트 블록 근거)"),
]}

# 이 도구들은 permit을 받지 않는다 (permit을 발급하거나, worker가 아닌 쪽이 부름)
NO_PERMIT = {"job_start"} | {n for n, s in TOOLS.items() if s.caller != W}
