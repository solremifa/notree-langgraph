"""LLM 교체 지점. 기본은 키 없이 도는 FakeLLM, --llm openai면 실제 호출."""
import os
import time

CALLS: list[str] = []   # 호출 기록 (이어하기 때 중복 호출이 없는지 세는 용도)


FAKE_DELAY = float(os.getenv("NOTREE_FAKE_DELAY", "0"))   # 가짜 LLM 응답 지연(초). kill -9 실험용


class FakeLLM:
    name = "fake"

    def extract(self, lecture: str) -> str:
        CALLS.append(f"extract:{lecture}")
        time.sleep(FAKE_DELAY)
        return lecture.upper()

    def merge(self, words: list[str], attempt: int, sloppy: str | None) -> str:
        CALLS.append(f"merge#{attempt}")
        time.sleep(FAKE_DELAY)
        # once: 1차 병합만 단어를 빠뜨려 '재시도' 분기 / always: 매번 빠뜨려 '확인 필요' 분기
        if sloppy == "always" or (sloppy == "once" and attempt == 1):
            words = words[:-1]
        return " ".join(words) + "!"


class OpenAILLM:
    name = "openai"

    def __init__(self) -> None:
        from openai import OpenAI
        self.client = OpenAI()               # OPENAI_API_KEY 환경변수 사용
        self.model = os.getenv("NOTREE_MODEL", "gpt-4o-mini")

    def _ask(self, prompt: str) -> str:
        r = self.client.chat.completions.create(
            model=self.model, messages=[{"role": "user", "content": prompt}], temperature=0
        )
        return (r.choices[0].message.content or "").strip()

    def extract(self, lecture: str) -> str:
        CALLS.append(f"extract:{lecture}")
        return self._ask(f"다음 강의 내용의 핵심 단어 하나만 영어 대문자로 답해. 다른 말은 쓰지 마.\n강의: {lecture}")

    def merge(self, words: list[str], attempt: int, sloppy: str | None) -> str:
        CALLS.append(f"merge#{attempt}")
        return self._ask("다음 단어를 주어진 순서 그대로 모두 포함해 짧은 인사 한 줄로 만들어. 문장만 답해.\n"
                         + ", ".join(words))


def get_llm(kind: str):
    return OpenAILLM() if kind == "openai" else FakeLLM()
