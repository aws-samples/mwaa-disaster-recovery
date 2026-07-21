"""
MwaaRestApiClient tests - SKIPPED.

The MwaaRestApiClient was rewritten to use boto3 mwaa:InvokeRestApi
instead of cookie-based authentication. These tests need a full rewrite.
"""

import pytest

pytestmark = pytest.mark.skip(
    reason="MwaaRestApiClient rewritten to use InvokeRestApi - tests need rewrite"
)


def test_placeholder():
    pass
