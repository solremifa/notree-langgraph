"""작업 종류별 핸들러. 새 핸들러를 만들면 여기에 import를 추가한다 (import될 때 @handler로 등록됨)."""
from notree.worker.handlers import (  # noqa: F401
    course_update,
    echo,
    final_note,
    grading,
    live_summary,
    qa,
)
