"""Agent Server용 Q&A 그래프 진입점 (실험 1단계).

    uv sync --group lgserver
    uv run --group lgserver langgraph dev --config notree/lgserver/langgraph.json --no-browser

왜 팩토리 함수인가
  worker handler는 llm · retriever 객체를 config["configurable"]에 넣어 그래프에 넘긴다.
  서버에서는 run 요청이 HTTP JSON이라 객체를 넘길 수 없다. 그래서 run마다 서버가 이 함수를 부르게 하고,
  JSON으로 받은 값(llm_kind 등)을 보고 여기서 객체를 만들어 그래프 기본 config에 붙인다.
  그래프 코드(notree/graphs/qa)는 그대로다.

run 요청에서 쓸 수 있는 값 (config.configurable, 모두 선택)
  llm_kind      "fake"(기본) | "openai"
  bad_citation  true면 가짜 LLM이 첫 답에서 없는 근거를 인용 → verify → answer 반복을 볼 수 있다
  deadline_sec  마감까지 남은 초. 없으면 마감 없음 (15쯤 주면 웹 검색을 건너뜀)
검색은 아직 가짜 말뭉치(sample_corpus). MCP로 실제 검색하는 건 2단계.
"""
import time

from langchain_core.runnables import RunnableConfig

from notree.graphs.qa import FakeRetriever, build_qa_graph
from notree.graphs.qa.llm import get_qa_llm
from notree.graphs.qa.retriever import sample_corpus


def make_qa_graph(config: RunnableConfig):
    c = config.get("configurable", {}) or {}
    kind = c.get("llm_kind", "fake")
    llm = get_qa_llm(kind, **({"bad_citation": True} if c.get("bad_citation") and kind == "fake" else {}))
    sec = c.get("deadline_sec")
    # 키 이름을 그래프가 읽는 llm · retriever · deadline_at과 다르게 둔 이유:
    # run 요청의 configurable이 그래프 기본 config 위에 덮이므로, 같은 이름이면 문자열이 객체를 덮어쓴다.
    return build_qa_graph().with_config(configurable={
        "llm": llm,
        "retriever": FakeRetriever(sample_corpus()),
        "deadline_at": time.time() + float(sec) if sec else None,
    })
