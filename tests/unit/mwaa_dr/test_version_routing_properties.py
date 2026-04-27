# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

"""
Property-based tests for DAG entry point version routing using Hypothesis.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

from hypothesis import given, settings
from hypothesis.strategies import sampled_from, composite, text, integers

from mwaa_dr.v_2_4.dr_factory import DRFactory_2_4
from mwaa_dr.v_2_5.dr_factory import DRFactory_2_5
from mwaa_dr.v_2_6.dr_factory import DRFactory_2_6
from mwaa_dr.v_2_7.dr_factory import DRFactory_2_7
from mwaa_dr.v_2_8.dr_factory import DRFactory_2_8
from mwaa_dr.v_2_9.dr_factory import DRFactory_2_9
from mwaa_dr.v_2_10.dr_factory import DRFactory_2_10
from mwaa_dr.v_2_11.dr_factory import DRFactory_2_11
from mwaa_dr.v_3_0.dr_factory import DRFactory_3_0
from mwaa_dr.framework.factory.default_dag_factory import DefaultDagFactory


# Version prefix to factory class mapping — mirrors the entry point DAG routing logic
VERSION_FACTORY_MAP = {
    "2.4": DRFactory_2_4,
    "2.5": DRFactory_2_5,
    "2.6": DRFactory_2_6,
    "2.7": DRFactory_2_7,
    "2.8": DRFactory_2_8,
    "2.9": DRFactory_2_9,
    "2.10": DRFactory_2_10,
    "2.11": DRFactory_2_11,
    "3.": DRFactory_3_0,
}

# Ordered prefixes — longer prefixes first to match the elif chain behavior
# (e.g., "2.10" must be checked before "2.1" would match)
ORDERED_PREFIXES = ["2.10", "2.11", "2.4", "2.5", "2.6", "2.7", "2.8", "2.9", "3."]


def resolve_factory_class(airflow_version: str):
    """
    Replicate the version routing logic from the entry point DAGs.

    This mirrors the if/elif chain in backup_metadata.py, restore_metadata.py,
    and cleanup_metadata.py.
    """
    if airflow_version.startswith("2.4"):
        return DRFactory_2_4
    elif airflow_version.startswith("2.5"):
        return DRFactory_2_5
    elif airflow_version.startswith("2.6"):
        return DRFactory_2_6
    elif airflow_version.startswith("2.7"):
        return DRFactory_2_7
    elif airflow_version.startswith("2.8"):
        return DRFactory_2_8
    elif airflow_version.startswith("2.9"):
        return DRFactory_2_9
    elif airflow_version.startswith("2.10"):
        return DRFactory_2_10
    elif airflow_version.startswith("2.11"):
        return DRFactory_2_11
    elif airflow_version.startswith("3."):
        return DRFactory_3_0
    else:
        return DefaultDagFactory


# --- Strategies ---

# Realistic version strings for each supported prefix
SUPPORTED_VERSION_EXAMPLES = [
    "2.4.3",
    "2.5.1",
    "2.6.3",
    "2.7.2",
    "2.8.1",
    "2.9.2",
    "2.10.1",
    "2.10.3",
    "2.11.0",
    "2.11.1",
    "3.0.2",
    "3.0.0",
    "3.1.0",
    "3.2.5",
]


@composite
def version_with_expected_factory(draw):
    """Generate a version string paired with its expected factory class."""
    version_str = draw(sampled_from(SUPPORTED_VERSION_EXAMPLES))
    expected_class = resolve_factory_class(version_str)
    return version_str, expected_class


@composite
def airflow_3x_versions(draw):
    """Generate Airflow 3.x version strings."""
    minor = draw(integers(min_value=0, max_value=20))
    patch = draw(integers(min_value=0, max_value=20))
    return f"3.{minor}.{patch}"


@composite
def airflow_2x_versions_with_factory(draw):
    """Generate Airflow 2.x version strings with their expected factory."""
    prefix = draw(
        sampled_from(["2.4", "2.5", "2.6", "2.7", "2.8", "2.9", "2.10", "2.11"])
    )
    patch = draw(integers(min_value=0, max_value=20))
    version_str = f"{prefix}.{patch}"
    expected_class = resolve_factory_class(version_str)
    return version_str, expected_class


@composite
def unsupported_versions(draw):
    """Generate version strings that don't match any supported prefix."""
    version_str = draw(
        sampled_from(
            [
                "1.10.15",
                "1.9.0",
                "4.0.0",
                "0.1.0",
                "2.3.4",
                "2.2.5",
                "2.1.0",
                "2.0.2",
            ]
        )
    )
    return version_str


# --- Property Tests ---


class TestVersionRoutingProperties:
    """
    **Validates: Requirements 6.4, 10.1**

    Property 9: Version routing selects correct factory

    For any Airflow version string, the entry point DAG version routing logic
    SHALL select DRFactory_3_0 when the version starts with "3.", and SHALL
    select the corresponding DRFactory_2_X class when the version starts with
    "2.X" (e.g., "2.10" → DRFactory_2_10).
    """

    @given(data=version_with_expected_factory())
    @settings(max_examples=100)
    def test_supported_versions_select_correct_factory(self, data):
        """
        **Validates: Requirements 6.4, 10.1**

        Property 9: Version routing selects correct factory — supported versions
        """
        version_str, expected_class = data
        actual_class = resolve_factory_class(version_str)
        assert actual_class == expected_class, (
            f"Version '{version_str}' should select {expected_class.__name__}, "
            f"got {actual_class.__name__}"
        )

    @given(version_str=airflow_3x_versions())
    @settings(max_examples=100)
    def test_all_3x_versions_select_drfactory_3_0(self, version_str):
        """
        **Validates: Requirements 6.4, 10.1**

        Property 9: Any version starting with "3." selects DRFactory_3_0
        """
        actual_class = resolve_factory_class(version_str)
        assert actual_class == DRFactory_3_0, (
            f"Version '{version_str}' starts with '3.' but selected "
            f"{actual_class.__name__} instead of DRFactory_3_0"
        )

    @given(data=airflow_2x_versions_with_factory())
    @settings(max_examples=100)
    def test_all_2x_versions_select_correct_factory(self, data):
        """
        **Validates: Requirements 6.4, 10.1**

        Property 9: Each 2.x version prefix selects the corresponding DRFactory_2_X
        """
        version_str, expected_class = data
        actual_class = resolve_factory_class(version_str)
        assert actual_class == expected_class, (
            f"Version '{version_str}' should select {expected_class.__name__}, "
            f"got {actual_class.__name__}"
        )

    @given(version_str=unsupported_versions())
    @settings(max_examples=100)
    def test_unsupported_versions_select_default_factory(self, version_str):
        """
        **Validates: Requirements 6.4, 10.1**

        Property 9: Unsupported versions fall back to DefaultDagFactory
        """
        actual_class = resolve_factory_class(version_str)
        assert actual_class == DefaultDagFactory, (
            f"Unsupported version '{version_str}' should select DefaultDagFactory, "
            f"got {actual_class.__name__}"
        )
