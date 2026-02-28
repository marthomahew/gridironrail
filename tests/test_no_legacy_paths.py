from __future__ import annotations

import inspect

from grs.football.resolver import FootballResolver
from grs.simulation.dynasty import DynastyRuntime


def test_legacy_trait_weight_flag_removed_from_resolver_api():
    params = inspect.signature(FootballResolver.__init__).parameters
    assert "trait_weighted_enabled" not in params


def test_runtime_no_longer_reads_legacy_trait_weight_env_flag():
    source = inspect.getsource(DynastyRuntime.__init__)
    assert "GRS_TRAIT_WEIGHTED" not in source
