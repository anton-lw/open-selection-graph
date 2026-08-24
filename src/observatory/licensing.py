"""Object-level licence and release decisions."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum


class ReleaseClass(str, Enum):
    REDISTRIBUTE = "redistribute"
    POINTER_HASH = "pointer_hash"
    DERIVED_ONLY = "derived_only"
    EXCLUDE = "exclude"


@dataclass(frozen=True)
class LicenceDecision:
    release_class: ReleaseClass
    licence: str | None
    attribution_required: bool
    share_alike: bool
    noncommercial: bool
    reason: str


_OPEN = {"cc0", "cc-by-4.0", "cc-by-3.0", "public-domain", "mit", "apache-2.0"}
_DERIVATIVE = {"cc-by-nc-sa-4.0", "cc-by-sa-4.0"}


def canonical_licence(value: str | None) -> str | None:
    """Canonicalize common SPDX labels and Creative Commons URLs."""
    if not value:
        return None
    low = value.strip().lower()
    cc = re.search(
        r"creativecommons\.org/(?:publicdomain/zero|licenses)/(by(?:-nc)?(?:-sa)?|zero)/([1-4](?:\.0)?)",
        low,
    )
    if cc:
        family, version = cc.groups()
        family = "cc0" if family == "zero" else f"cc-{family}"
        return f"{family}-{version}" if family != "cc0" else "CC0"
    normalized = low.replace("_", "-").replace(" ", "-")
    aliases = {
        "creative-commons-attribution-4.0": "cc-by-4.0",
        "cc-by": "cc-by-4.0",
        "public-domain": "public-domain",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized.startswith("cc-"):
        return normalized.upper()
    return value.strip()


def decide_release(
    *,
    object_type: str,
    licence: str | None,
    source_allows_redistribution: bool | None,
    derived_feature_reconstructive: bool = False,
) -> LicenceDecision:
    canonical = canonical_licence(licence)
    norm = (canonical or "").strip().lower().replace(" ", "-")
    if source_allows_redistribution is False:
        return LicenceDecision(
            ReleaseClass.POINTER_HASH,
            canonical,
            False,
            False,
            False,
            "source terms prohibit or do not authorize redistribution",
        )
    if norm in _OPEN and source_allows_redistribution is not False:
        return LicenceDecision(
            ReleaseClass.REDISTRIBUTE,
            canonical,
            norm != "cc0" and norm != "public-domain",
            norm.endswith("-sa-4.0"),
            "-nc-" in norm,
            "affirmative open licence",
        )
    if norm in _DERIVATIVE and source_allows_redistribution is not False:
        return LicenceDecision(
            ReleaseClass.REDISTRIBUTE,
            canonical,
            True,
            "-sa-" in norm,
            "-nc-" in norm,
            "redistributable in a separately compatible package",
        )
    if not derived_feature_reconstructive and object_type not in {"email", "contact", "authorization"}:
        return LicenceDecision(
            ReleaseClass.DERIVED_ONLY,
            canonical,
            False,
            False,
            False,
            "licence unresolved; only non-reconstructive derived features and pointer/hash are safe",
        )
    return LicenceDecision(
        ReleaseClass.EXCLUDE,
        canonical,
        False,
        False,
        False,
        "unknown rights or sensitive/reconstructive object",
    )
