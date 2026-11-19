import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "check_no_comments", Path(__file__).resolve().parents[1] / "scripts" / "check_no_comments.py"
)
assert _spec is not None and _spec.loader is not None
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


def test_comment_lines_skips_directives_and_strings() -> None:
    src = (
        "#!/usr/bin/env python\n"
        "# -*- coding: utf-8 -*-\n"
        "x = 1  # noqa: E501\n"
        "y: int = 2  # type: ignore\n"
        "s = '# not a comment'\n"
        "# real comment\n"
        "z = 3  # trailing\n"
    )
    assert mod.comment_lines(src) == [6, 7]
