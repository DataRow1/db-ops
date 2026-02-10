from dbops.cli.tui import _MAX_JOB_NAME_WIDTH, _job_choice_title, _truncate
from dbops.core.jobs import Job


def test_job_choice_title_shows_name_before_id_and_aligns_id_column():
    first = _job_choice_title(Job(id=11, name="alpha"), name_width=12)
    second = _job_choice_title(Job(id=22, name="beta"), name_width=12)

    assert first.startswith("alpha")
    assert second.startswith("beta")
    assert first.index("(id: ") == second.index("(id: ")


def test_job_choice_title_truncates_long_names():
    long_name = "x" * (_MAX_JOB_NAME_WIDTH + 10)
    rendered = _job_choice_title(
        Job(id=99, name=long_name),
        name_width=_MAX_JOB_NAME_WIDTH,
    )

    assert "..." in rendered
    assert "(id: 99)" in rendered
    assert _truncate(long_name, _MAX_JOB_NAME_WIDTH).endswith("...")
