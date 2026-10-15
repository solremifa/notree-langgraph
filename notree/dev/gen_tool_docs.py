"""notree/contracts/tools.py → docs/tool-contracts.md 생성.

    uv run python -m notree.dev.gen_tool_docs          # 문서 다시 쓰기
    uv run python -m notree.dev.gen_tool_docs --check  # 문서가 계약과 다르면 실패 (테스트에서 사용)
"""
import argparse
import sys
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from notree.contracts.envelope import RETRYABLE, ErrorCode
from notree.contracts.tools import NO_PERMIT, TOOLS, ToolSpec

OUT = Path(__file__).resolve().parents[2] / "docs" / "tool-contracts.md"
KIND = {"control": "작업 제어", "read": "읽기", "write": "쓰기", "external": "외부", "dev": "개발용"}


def _type(schema: dict[str, Any] | bool, defs: dict[str, Any]) -> str:
    if not isinstance(schema, dict):          # additionalProperties: true
        return "any"
    if "$ref" in schema:
        return schema["$ref"].split("/")[-1]
    if "anyOf" in schema:
        parts = [_type(s, defs) for s in schema["anyOf"]]
        return " | ".join(p for p in parts if p != "null") + (" | null" if "null" in parts else "")
    if "enum" in schema:
        return " | ".join(f'"{v}"' for v in schema["enum"])
    if "const" in schema:
        return f'"{schema["const"]}"'
    t = schema.get("type", "any")
    if t == "array":
        return f"list[{_type(schema.get('items', {}), defs)}]"
    if t == "object" and "additionalProperties" in schema:
        return f"dict[str, {_type(schema['additionalProperties'], defs)}]"
    return {"string": "str", "integer": "int", "number": "float", "boolean": "bool", "object": "dict"}.get(t, t)


def _fields(model: type[BaseModel]) -> list[str]:
    schema = model.model_json_schema()
    defs = schema.get("$defs", {})
    req = set(schema.get("required", []))
    rows = []
    for name, prop in schema.get("properties", {}).items():
        default = "" if name in req else f" = {prop.get('default')!r}"
        desc = prop.get("description", "")
        rows.append(f"| `{name}` | `{_type(prop, defs)}`{default} | {desc} |")
    return rows or ["| (없음) | | |"]


def _section(s: ToolSpec) -> list[str]:
    tags = [KIND[s.kind], f"호출: {s.caller}", f"정의: {s.owner}"]
    if s.reviewer:
        tags.append(f"리뷰: {s.reviewer}")
    if s.model_exposed:
        tags.append("**모델에 노출**")
    if s.name not in NO_PERMIT:
        tags.append("permit 필요")
    out = [f"### `{s.name}`" + (" (계획)" if s.status == "planned" else ""), "", " · ".join(tags), ""]
    if s.summary:
        out += [s.summary, ""]
    out += ["입력", "", "| 필드 | 타입 | 설명 |", "|---|---|---|", *_fields(s.input), ""]
    out += ["출력 (`data`)", "", "| 필드 | 타입 | 설명 |", "|---|---|---|", *_fields(s.output), ""]
    return out


def render() -> str:
    lines = [
        "# MCP 도구 계약",
        "",
        "> 이 문서는 `notree/contracts/tools.py`에서 자동 생성됩니다. 직접 고치지 말고 계약 코드를 고친 뒤",
        "> `uv run python -m notree.dev.gen_tool_docs`를 실행하세요. (테스트가 둘이 같은지 확인합니다)",
        "",
        "## 공통 규칙",
        "",
        "- 이름: `영역_동사`, 소문자·밑줄 (OpenAI 함수 이름은 점을 허용하지 않음)",
        "- worker가 부르는 도구는 모두 `permit`을 받는다 (`job_start` 제외). 사용자 ID 등 permit으로 알 수 있는 값은 인자로 받지 않는다",
        "- 쓰기 도구는 같은 트랜잭션 안에서 permit의 실행권(작업이 running, 소유자·실행 세대 일치)을 확인한다. 없으면 `permit_invalid`",
        "- 쓰기 도구는 `idem_key`(= job_id), 삭제·취소 영향을 받는 쓰기는 `expected_generation`을 받는다",
        "- 응답: `{ok: true, data}` / `{ok: true, data, duplicate: true}` / `{ok: false, code, message, retryable}`",
        "- 바꾸는 규칙: 선택 필드 추가는 자유, 삭제·이름 변경·의미 변경은 호출하는 쪽·리뷰 담당과 합의 후",
        "",
        "## 오류 코드",
        "",
        "| code | retryable | worker에서 |",
        "|---|---|---|",
    ]
    for c in ErrorCode:
        r = c in RETRYABLE
        lines.append(f"| `{c}` | {'예' if r else '아니오'} | {'ToolUnavailable (RetryableError)' if r else 'ToolRejected (FatalError)'} |")
    impl = [s for s in TOOLS.values() if s.status == "implemented"]
    plan = [s for s in TOOLS.values() if s.status == "planned"]
    lines += ["", "## 목록", "", "| 도구 | 종류 | 호출 | 정의 | 리뷰 | 모델 노출 | 상태 |", "|---|---|---|---|---|---|---|"]
    for s in impl + plan:
        lines.append(f"| `{s.name}` | {KIND[s.kind]} | {s.caller} | {s.owner} | {s.reviewer} | "
                     f"{'○' if s.model_exposed else ''} | {'구현' if s.status == 'implemented' else '계획'} |")
    lines += ["", "## 구현된 도구", ""]
    for s in impl:
        lines += _section(s)
    lines += ["## 계획된 도구", "", "담당자가 PR로 필드를 확정하고 `notree/mcp_server/server.py`에 구현합니다.", ""]
    for s in plan:
        lines += _section(s)
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--check", action="store_true")
    a = p.parse_args()
    text = render()
    if a.check:
        if not OUT.exists() or OUT.read_text("utf-8") != text:
            print("docs/tool-contracts.md가 계약과 다릅니다. uv run python -m notree.dev.gen_tool_docs 를 실행하세요.")
            sys.exit(1)
        print("ok")
        return
    OUT.write_text(text, "utf-8")
    print(f"→ {OUT}")


if __name__ == "__main__":
    main()
