def test_imports():
    import sys
    sys.path.insert(0, "./codes")
    import common.provenance
    import common.http
    import collectors.eth2
    import collectors.cosmos
    # substrateinterface is optional; only assertable if installed
    try:
        import substrateinterface  # type: ignore
    except Exception:
        pass
