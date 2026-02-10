from dbops.cli.common.progress import _display_job_label


def test_display_job_label_name_before_id_and_aligned():
    labels = {
        1: _display_job_label(1, {1: "alpha_job", 2: "beta"}, name_width=12),
        2: _display_job_label(2, {1: "alpha_job", 2: "beta"}, name_width=12),
    }

    assert labels[1].startswith("alpha_job")
    assert labels[2].startswith("beta")
    assert labels[1].index("(id: ") == labels[2].index("(id: ")


def test_display_job_label_falls_back_to_id_when_name_missing():
    assert _display_job_label(42, {1: "alpha"}, name_width=10) == "42"
