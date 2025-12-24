"""Questionary / prompt_toolkit theme for BRICK-OPS.

Questionary uses prompt_toolkit under the hood. This module defines a single
central style so all interactive prompts (confirm/checkbox/etc.) look consistent.
"""

from __future__ import annotations

from prompt_toolkit.styles import Style

QUESTIONARY_STYLE_SELECT = Style.from_dict(
    {
        "question": "bold ansibrightred",
        "answer": "bold ansibrightyellow",
        "pointer": "bold ansibrightyellow",
        "highlighted": "bold ansibrightyellow",
        "selected": "bold ansibrightyellow",
        "checkbox": "ansibrightblack",
        "checkbox-selected": "bold ansibrightyellow",
        "separator": "ansibrightblack",
        "instruction": "ansibrightblack",
        "error": "bold ansired",
        "disabled": "ansibrightblack",
    }
)

QUESTIONARY_STYLE_CONFIRM = Style.from_dict(
    {
        "question": "bold ansibrightred",
        "answer": "bold ansibrightred",
        "pointer": "bold ansibrightred",
        "highlighted": "bold ansibrightred",
        "selected": "bold ansibrightred",
        "separator": "ansibrightblack",
        "instruction": "ansibrightblack",
        "error": "bold ansired",
        "disabled": "ansibrightblack",
    }
)
