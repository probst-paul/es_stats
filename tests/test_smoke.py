def test_imports_and_app_exists():
    import es_stats  # noqa: F401
    from es_stats.web.main import app

    assert app is not None
