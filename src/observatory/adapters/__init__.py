"""Concrete adapters for publication and scholarly-identity sources."""

from .arxiv import ArxivOAIConnector
from .copernicus import CopernicusOAIConnector
from .copernicus_crossref import CopernicusCrossrefPostedConnector
from .crossref import CrossrefPeerReviewConnector
from .elife import ELifeProcessConnector
from .europepmc import EuropePMCConnector
from .f1000 import F1000ProcessConnector
from .openalex import OpenAlexSingletonConnector
from .openreview import OpenReviewLocalConnector
from .openreview_api import (
    OpenReviewAPINotesConnector,
    OpenReviewBatchedForumNotesConnector,
)
from .openreview_surface import OpenReviewSurfaceConnector
from .provider import CrossrefProviderConnector
from .scipost import SciPostProcessConnector

__all__ = [
    "ArxivOAIConnector",
    "CopernicusOAIConnector",
    "CopernicusCrossrefPostedConnector",
    "CrossrefPeerReviewConnector",
    "ELifeProcessConnector",
    "EuropePMCConnector",
    "F1000ProcessConnector",
    "OpenAlexSingletonConnector",
    "OpenReviewLocalConnector",
    "OpenReviewAPINotesConnector",
    "OpenReviewBatchedForumNotesConnector",
    "OpenReviewSurfaceConnector",
    "CrossrefProviderConnector",
    "SciPostProcessConnector",
]
