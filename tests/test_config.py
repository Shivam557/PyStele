from pystele.core.config import Config


def test_defaults_and_validate():
    cfg = Config.load()
    cfg.validate()
