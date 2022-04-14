import pytest
from .context import pipelines
from pipelines.utils import manage_configs as cm

class TestManageConfigs(object):

    def test_islocal(self):
        assert cm.is_local() == True