"""Q&A 대화 세션 단기 메모리 (Redis). 대화 이력을 읽고 쓰는 규칙은 이 모듈에만 둔다.

MCP 서버(업무 서비스)만 이 모듈을 부른다. worker는 대화 키를 모르고, 이력은 qa_context로 받는다.
설계와 불변 조건: docs/changes/2026-10-20-qa-conversation-memory.md

키 하나 = 대화 세션 한 세대:  notree:conv:{conversation_id}:{epoch}   (해시)
    user, course, epoch    주인 (읽을 때마다 대조)
    expires_at             만료 시각 (epoch ms). 키의 TTL과 같은 값. 사용자 활동(touch)만 늘린다
    mark                   이미 반영한 마지막 차례(turn_seq). 이보다 큰 차례만 추가 → 재시도·재반영이 중복되지 않음
    messages               JSON [{message_id, question_id, role, content, sequence, status?}]  완료된 문답 쌍만

규칙
- 키를 만드는 곳은 create() 하나 (대화 시작). touch·apply·remove는 **있는 키만** 바꾼다 → 종료·만료·Redis 유실 뒤 자동 부활 없음.
- 모든 변경은 WATCH → 읽기 → MULTI(HSET + PEXPIREAT 같은 만료 시각) → EXEC. 키가 그사이 바뀌거나 지워지면 처음부터 다시.
  EXEC 직전에 키가 만료돼 HSET이 새 키를 만들더라도 같은 트랜잭션의 PEXPIREAT(지난 시각)이 바로 지운다.
- 종료·만료는 DB가 세대를 올린다. 이전 세대 키는 아무도 읽지 않고, 늦은 쓰기는 이전 세대 키(이미 없거나 곧 만료)로만 간다.
- Redis 오류는 MemoryUnavailable로 바꿔 던진다. 부르는 쪽이 정한다 (답 저장은 성공으로 두고, 질문 등록은 거절).
- 원문·Redis 값은 로그에 찍지 않는다.

실행 위치 (이벤트 루프 차단 방지, 문서 9절)
- 아래 갱신 함수(create·touch·read·apply_turns·remove_question·drop)는 동기 Redis를 쓰는 **동기 함수**다.
  MCP 도구(이벤트 루프)에서는 직접 부르지 말고 `await run(fn, ...)`으로 부른다: 전용 스레드(CONV_REDIS_THREADS개)에서 돈다.
- 실행 중 + 대기는 CONV_REDIS_MAX_PENDING개까지. 넘으면 기다리지 않고 MemoryUnavailable (요청이 쌓여 무한히 밀리지 않게).
- 작업마다 시한(CONV_REDIS_OP_DEADLINE_SEC): 스레드를 기다리다 시한이 지나면 Redis에 닿지 않고 실패, WATCH 재시도도 시한까지.
  명령 하나는 연결·읽기 시간 제한(CONV_REDIS_TIMEOUT_SEC) × 재시도 1회로 끝난다 → 최악 ≈ 시한 + 2 × (연결 + 읽기).
- 기다리던 쪽이 취소돼도 이미 시작한 스레드의 동기 I/O는 멈추지 않는다. 슬롯은 스레드가 실제로 끝날 때 돌려받는다.
- 클라이언트·스레드는 이벤트 루프에 묶이지 않는다 (동기 클라이언트 + 연결 풀 상한 = 스레드 수). shutdown()이 둘 다 닫는다.
"""
import asyncio
import json
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor

import redis
from redis.backoff import NoBackoff
from redis.exceptions import RedisError, WatchError
from redis.retry import Retry

from notree.common import settings as S

PREFIX = "notree:conv"
_client: redis.Redis | None = None
_executor: ThreadPoolExecutor | None = None
_inflight = 0                         # run()으로 넘겨 아직 끝나지 않은 작업 (대기 + 실행 중)
_lock = threading.Lock()
_local = threading.local()            # 스레드에서 도는 작업의 시한 (monotonic)


class MemoryUnavailable(Exception):
    """Redis에 닿지 못함 (연결·시간 초과·경합 반복·요청 밀림). 키가 '없음'과는 다르다."""


def client() -> redis.Redis:
    """처음 쓸 때 연결한다 (단일 질문만 쓰면 Redis에 붙지 않음). 장애면 짧게 한 번만 다시 해 보고 MemoryUnavailable로
    (질문 등록·답 저장이 Redis 장애 때문에 오래 붙잡히지 않게). 연결 수는 스레드 수까지 (넘으면 풀에서 시간 제한만큼 기다림)."""
    global _client
    with _lock:
        if _client is None:
            t = S.CONV_REDIS_TIMEOUT_SEC
            pool = redis.BlockingConnectionPool.from_url(
                S.CONV_REDIS_URL, max_connections=S.CONV_REDIS_THREADS, timeout=t, decode_responses=True,
                socket_timeout=t, socket_connect_timeout=t, retry=Retry(NoBackoff(), 1))
            _client = redis.Redis(connection_pool=pool)
        return _client


def _check_deadline() -> None:
    d = getattr(_local, "deadline", None)
    if d is not None and time.monotonic() >= d:
        raise MemoryUnavailable("시간 초과")


def _in_thread(deadline: float, fn, args):
    _local.deadline = deadline
    try:
        _check_deadline()                                   # 스레드를 기다리다 시한이 지났으면 Redis에 닿지 않는다
        return fn(*args)
    finally:
        _local.deadline = None


def _finished(_: Future) -> None:
    global _inflight
    with _lock:
        _inflight -= 1


async def run(fn, *args):
    """동기 Redis 작업 fn(*args)을 이벤트 루프 밖 전용 스레드에서 돌려 결과를 기다린다 (MCP 도구는 이것으로만 부른다).
    상한을 넘으면 바로 MemoryUnavailable. 기다리던 쪽이 취소되면 아직 시작 안 한 작업은 취소되고, 이미 도는 작업은
    시간 제한으로 끝날 때까지 슬롯을 잡고 있다 (취소가 I/O를 멈춘다고 가정하지 않는다)."""
    global _executor, _inflight
    with _lock:
        if _inflight >= S.CONV_REDIS_MAX_PENDING:
            raise MemoryUnavailable("대화 메모리 요청이 밀림")
        if _executor is None:
            _executor = ThreadPoolExecutor(S.CONV_REDIS_THREADS, thread_name_prefix="conv-redis")
        ex = _executor
        _inflight += 1
    try:
        fut = ex.submit(_in_thread, time.monotonic() + S.CONV_REDIS_OP_DEADLINE_SEC, fn, args)
    except RuntimeError:                                    # shutdown() 뒤
        with _lock:
            _inflight -= 1
        raise MemoryUnavailable("종료 중") from None
    fut.add_done_callback(_finished)                        # 끝남·취소 어느 쪽이든 슬롯 반환 (스레드가 실제로 끝난 뒤)
    return await asyncio.wrap_future(fut)


def stats() -> dict:
    """시험·점검용: 대기+실행 중 작업 수, 살아 있는 스레드 수."""
    with _lock:
        return {"inflight": _inflight, "threads": len(_executor._threads) if _executor else 0}


def shutdown(wait: bool = False) -> None:
    """서버 수명 종료: 새 작업을 받지 않고 대기 작업을 취소, 연결 풀을 닫는다. 이미 도는 스레드는 시간 제한으로 끝난다
    (wait=True면 그것까지 기다림). 다시 쓰면 새로 만든다."""
    global _executor, _client
    with _lock:
        ex, cli, _executor, _client = _executor, _client, None, None
    if ex is not None:
        ex.shutdown(wait=wait, cancel_futures=True)
    if cli is not None:
        cli.close()
        cli.connection_pool.disconnect()                    # 풀을 넘겨 만든 클라이언트는 close()가 풀을 닫지 않는다


def key(conversation_id: str, epoch: int) -> str:
    return f"{PREFIX}:{conversation_id}:{epoch}"


def now_ms() -> int:
    return int(time.time() * 1000)


def ttl_ms() -> int:
    return int(S.QA_CONV_TTL_SEC * 1000)


def _valid(h: dict, user: str, course: str, epoch: int) -> bool:
    return (bool(h) and h.get("user") == user and h.get("course") == course and h.get("epoch") == str(epoch)
            and int(h.get("expires_at", 0)) > now_ms())


def _cas(k: str, fn):
    """WATCH 조건부 갱신. fn(hash) → (결과, 쓸 필드 dict 또는 None). 쓸 때는 expires_at을 꼭 함께 정한다."""
    try:
        with client().pipeline() as p:
            for _ in range(20):
                _check_deadline()
                try:
                    p.watch(k)
                    result, writes = fn(p.hgetall(k))
                    if writes is None:
                        p.unwatch()
                        return result
                    p.multi()
                    p.hset(k, mapping=writes)
                    p.pexpireat(k, int(writes["expires_at"]))
                    p.execute()
                    return result
                except WatchError:
                    continue
    except RedisError as e:
        raise MemoryUnavailable(f"{type(e).__name__}") from e
    raise MemoryUnavailable("갱신 경합이 계속됨")


def create(conversation_id: str, epoch: int, user: str, course: str) -> int:
    """대화 시작. 새 키를 만든다 (이 모듈에서 키를 만드는 유일한 곳). 만료 시각(ms)을 돌려준다."""
    exp = now_ms() + ttl_ms()
    k = key(conversation_id, epoch)
    try:
        p = client().pipeline(transaction=True)
        p.delete(k)
        p.hset(k, mapping={"user": user, "course": course, "epoch": epoch, "expires_at": exp, "mark": 0,
                           "messages": "[]"})
        p.pexpireat(k, exp)
        p.execute()
    except RedisError as e:
        raise MemoryUnavailable(f"{type(e).__name__}") from e
    return exp


def touch(conversation_id: str, epoch: int, user: str, course: str) -> int | None:
    """사용자 활동: 키가 있고 주인·세대가 맞으면 만료 시각을 지금 + TTL로. 없으면 None (만들지 않는다)."""
    def fn(h):
        if not _valid(h, user, course, epoch):
            return None, None
        exp = now_ms() + ttl_ms()
        return exp, {"expires_at": exp}
    return _cas(key(conversation_id, epoch), fn)


def read(conversation_id: str, epoch: int, user: str, course: str) -> dict | None:
    """{mark, messages, expires_at} 또는 None (없음·주인 다름·만료)."""
    try:
        h = client().hgetall(key(conversation_id, epoch))
    except RedisError as e:
        raise MemoryUnavailable(f"{type(e).__name__}") from e
    if not _valid(h, user, course, epoch):
        return None
    return {"mark": int(h["mark"]), "messages": json.loads(h["messages"]), "expires_at": int(h["expires_at"])}


CLIPPED = "…(생략)"


def _clip(text: str) -> str:
    n = S.QA_CONV_MSG_MAX_CHARS
    return text if len(text) <= n else text[: n - len(CLIPPED)] + CLIPPED


def _pairs(messages: list[dict]) -> list[list[dict]]:
    """메시지를 차례(question_id)별 [user, assistant] 쌍으로. 순서는 sequence."""
    by_q: dict[str, list[dict]] = {}
    for m in sorted(messages, key=lambda m: m["sequence"]):
        by_q.setdefault(m["question_id"], []).append(m)
    return list(by_q.values())


def apply_turns(conversation_id: str, epoch: int, user: str, course: str, turns: list[dict]) -> tuple[str, int]:
    """완료된 문답 쌍을 이력에 반영. turns: [{turn_seq, question_id, user_message_id, assistant_message_id,
    question, answer, status}] (DB가 원본). mark보다 큰 차례만 추가하고 최근 QA_CONV_MAX_TURNS 쌍만 남긴다.
    → ("applied" | "noop" | "missing", 반영 뒤 mark). 키가 없으면 아무것도 쓰지 않는다 (늦은 쓰기 방어)."""
    def fn(h):
        if not _valid(h, user, course, epoch):
            return ("missing", -1), None
        mark = int(h["mark"])
        new = sorted((t for t in turns if t["turn_seq"] > mark), key=lambda t: t["turn_seq"])
        if not new:
            return ("noop", mark), None
        msgs = json.loads(h["messages"])
        for t in new:
            msgs += [{"message_id": t["user_message_id"], "question_id": t["question_id"], "role": "user",
                      "content": _clip(t["question"]), "sequence": 2 * t["turn_seq"] - 1},
                     {"message_id": t["assistant_message_id"], "question_id": t["question_id"], "role": "assistant",
                      "content": _clip(t["answer"]), "sequence": 2 * t["turn_seq"], "status": t.get("status")}]
        kept = [m for pair in _pairs(msgs)[-S.QA_CONV_MAX_TURNS:] for m in pair]
        mark = new[-1]["turn_seq"]
        return ("applied", mark), {"messages": json.dumps(kept, ensure_ascii=False), "mark": mark,
                                   "expires_at": h["expires_at"]}          # 만료 시각은 그대로 (답 도착은 활동이 아님)
    return _cas(key(conversation_id, epoch), fn)


def remove_question(conversation_id: str, epoch: int, user: str, course: str, question_id: str) -> bool:
    """질문 삭제(qa_delete) 때 그 질문의 메시지를 이력에서 뺀다. 키가 없으면 False."""
    def fn(h):
        if not _valid(h, user, course, epoch):
            return False, None
        msgs = [m for m in json.loads(h["messages"]) if m["question_id"] != question_id]
        return True, {"messages": json.dumps(msgs, ensure_ascii=False), "expires_at": h["expires_at"]}
    return _cas(key(conversation_id, epoch), fn)


def drop(conversation_id: str, epoch: int) -> None:
    """종료·만료: 이 세대의 단기 문맥을 지운다."""
    try:
        client().delete(key(conversation_id, epoch))
    except RedisError as e:
        raise MemoryUnavailable(f"{type(e).__name__}") from e


def est_tokens(text: str) -> int:
    """토크나이저 없이 쓰는 보수적 추정: UTF-8 바이트 / 3 (한글 한 글자 ≈ 1, 영어 3글자 ≈ 1)."""
    return (len(text.encode("utf-8")) + 2) // 3


def window(messages: list[dict], before_seq: int, exclude_question_id: str | None,
           max_tokens: int | None = None) -> list[dict]:
    """모델 입력용 이력: 현재 차례(before_seq)보다 앞선 완료 쌍만, 최신 쌍부터 상한 안에서. 결과는 오래된 순.
    반쪽 쌍(답 없는 질문)은 넣지 않는다. 최신 쌍 하나만으로 상한을 넘으면 답 → 질문 순으로 뒤를 잘라 그 쌍은 넣는다."""
    budget = S.QA_HISTORY_MAX_TOKENS if max_tokens is None else max_tokens
    pairs = [p for p in _pairs(messages)
             if len(p) == 2 and p[0]["role"] == "user" and p[1]["role"] == "assistant"
             and p[0]["question_id"] != exclude_question_id and p[1]["sequence"] < 2 * before_seq - 1]
    out: list[list[dict]] = []
    used = 0
    for p in reversed(pairs):
        cost = sum(est_tokens(m["content"]) for m in p)
        if used + cost <= budget:
            out.append(p)
            used += cost
            continue
        if not out:                                   # 최신 쌍이 혼자서도 넘침 → 잘라서라도 넣는다
            out.append(_shrink(p, budget))
        break
    return [m for p in reversed(out) for m in p]


def _shrink(pair: list[dict], budget: int) -> list[dict]:
    u, a = dict(pair[0]), dict(pair[1])
    mark = CLIPPED

    def cut(m: dict, allow: int) -> None:
        if est_tokens(m["content"]) > allow:
            text = m["content"]
            while text and est_tokens(text + mark) > max(allow, est_tokens(mark)):
                text = text[: max(0, int(len(text) * 0.9) - 1)]
            m["content"] = text + mark
    cut(a, max(budget - est_tokens(u["content"]), 0))
    if est_tokens(u["content"]) + est_tokens(a["content"]) > budget:
        cut(u, max(budget - est_tokens(a["content"]), 0))
    return [u, a]
