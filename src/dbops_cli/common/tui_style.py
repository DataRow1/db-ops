"""TUI formatting for the CLI."""

from prompt_toolkit.styles import Style

QUESTIONARY_STYLE = Style.from_dict(
    {
        # Overall
        "question": "bold cyan",
        "answer": "bold green",
        "pointer": "cyan",
        "highlighted": "bold cyan",
        "selected": "bold green",
        "separator": "dim",
        "instruction": "dim",
        # Checkbox specific
        "checkbox": "",
        "checkbox-selected": "bold green",
        "checkbox-unselected": "",
        "checkbox-pointer": "cyan",
        # Text input
        "text": "",
    }
)
