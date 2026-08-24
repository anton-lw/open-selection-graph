"""Licence-aware content storage and pointer-first extraction records."""

from __future__ import annotations

from dataclasses import dataclass

from .ids import content_hash, stable_id
from .licensing import LicenceDecision, ReleaseClass, decide_release
from .storage import RawReceipt, RawStore


@dataclass(frozen=True)
class ContentReceipt:
    content_artifact_id: str
    raw_receipt: RawReceipt | None
    byte_hash: str
    normalized_text_hash: str | None
    release: LicenceDecision
    source_url: str | None


class LicenceAwareContentStore:
    def __init__(self, raw_store: RawStore):
        self.raw_store = raw_store

    def put(
        self,
        *,
        source_id: str,
        native_id: str,
        object_type: str,
        payload: bytes | str,
        source_url: str | None,
        licence: str | None,
        source_allows_redistribution: bool | None,
        normalized_text: str | None = None,
        derived_feature_reconstructive: bool = False,
        retain_protected_raw: bool = True,
    ) -> ContentReceipt:
        data = payload.encode() if isinstance(payload, str) else payload
        decision = decide_release(
            object_type=object_type,
            licence=licence,
            source_allows_redistribution=source_allows_redistribution,
            derived_feature_reconstructive=derived_feature_reconstructive,
        )
        raw = None
        if retain_protected_raw and decision.release_class is not ReleaseClass.EXCLUDE:
            raw = self.raw_store.put(
                source_id=source_id, native_id=native_id, object_type=object_type,
                payload=data, metadata={"source_url": source_url, "protected": decision.release_class != ReleaseClass.REDISTRIBUTE},
            )
        return ContentReceipt(
            content_artifact_id=stable_id("content_artifact", source_id, native_id),
            raw_receipt=raw,
            byte_hash=content_hash(data),
            normalized_text_hash=content_hash(normalized_text) if normalized_text is not None else None,
            release=decision,
            source_url=source_url,
        )
