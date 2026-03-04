import pytest
from query_sanitization import resolve_nlp_n_process


def test_cli_value_takes_precedence_over_env_var(monkeypatch):
    monkeypatch.setenv("NLP_N_PROCESS", "8")
    assert resolve_nlp_n_process(cli_value=4) == 4


def test_env_var_used_when_cli_is_none(monkeypatch):
    monkeypatch.setenv("NLP_N_PROCESS", "6")
    assert resolve_nlp_n_process(cli_value=None) == 6


def test_default_when_neither_cli_nor_env_var(monkeypatch):
    monkeypatch.delenv("NLP_N_PROCESS", raising=False)
    assert resolve_nlp_n_process(cli_value=None) == 1
