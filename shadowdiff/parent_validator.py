"""Parent relationship validation for ShadowDiff engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional
from enum import Enum


class ParentValidationStatus(Enum):
    VALID = "VALID"
    ORPHAN = "ORPHAN"
    PARENT_CHANGED = "PARENT_CHANGED"
    CONSISTENT = "CONSISTENT"


@dataclass
class ParentConfig:
    """Configuration for parent relationship validation."""
    candidates_path: str = "candidates"
    id_field: str = "internalId"
    parent_field: str = "parentId"
    identity_key: str | list[str] = "label"
    duplicate_handling: str = "error"  # error, first, last, warn

    @classmethod
    def from_schema(cls, schema: dict) -> Optional['ParentConfig']:
        """Extract parent config from schema x-migration-parent-config."""
        config_dict = schema.get('x-migration-parent-config')
        if not config_dict:
            return None

        return cls(
            candidates_path=config_dict.get('candidatesPath', 'candidates'),
            id_field=config_dict.get('idField', 'internalId'),
            parent_field=config_dict.get('parentField', 'parentId'),
            identity_key=config_dict.get('identityKey', 'label'),
            duplicate_handling=config_dict.get('duplicateHandling', 'error')
        )


@dataclass
class ParentRelationship:
    """Represents a parent-child relationship."""
    child_identity: tuple
    child_id: str
    parent_id: Optional[str]
    parent_identity: Optional[tuple]

    def __repr__(self):
        child_str = self.child_identity[0] if len(self.child_identity) == 1 else self.child_identity
        if self.parent_identity:
            parent_str = self.parent_identity[0] if len(self.parent_identity) == 1 else self.parent_identity
            return f"{child_str} -> {parent_str}"
        elif self.parent_id:
            return f"{child_str} -> ORPHAN({self.parent_id})"
        return f"{child_str} -> ROOT"


@dataclass
class ParentValidationResult:
    """Result of parent relationship validation."""
    status: ParentValidationStatus
    entity_identity: tuple
    before_relationship: Optional[ParentRelationship] = None
    after_relationship: Optional[ParentRelationship] = None
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "status": self.status.value,
            "entity": self.entity_identity[0] if len(self.entity_identity) == 1 else list(self.entity_identity),
            "before": str(self.before_relationship) if self.before_relationship else None,
            "after": str(self.after_relationship) if self.after_relationship else None,
            "message": self.message
        }


@dataclass
class ParentValidationReport:
    """Complete parent validation report."""
    results: list[ParentValidationResult] = field(default_factory=list)
    orphans: list[ParentValidationResult] = field(default_factory=list)
    changed: list[ParentValidationResult] = field(default_factory=list)
    consistent: list[ParentValidationResult] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.orphans) == 0 and len(self.changed) == 0

    def to_dict(self) -> dict:
        return {
            "is_valid": self.is_valid,
            "summary": {
                "total": len(self.results),
                "orphans": len(self.orphans),
                "changed": len(self.changed),
                "consistent": len(self.consistent)
            },
            "orphans": [r.to_dict() for r in self.orphans],
            "changed": [r.to_dict() for r in self.changed],
            "consistent": [r.to_dict() for r in self.consistent]
        }


class ParentValidator:
    """Validates parent-child relationships across before/after payloads."""

    def __init__(self, config: ParentConfig):
        self.config = config

    def validate(self, before: dict, after: dict) -> ParentValidationReport:
        """
        Validate parent relationships between before and after payloads.

        Checks:
        1. Orphan detection: parentId points to non-existent id
        2. Relationship consistency: same logical parent in before and after
        """
        report = ParentValidationReport()

        # Extract candidates arrays
        before_candidates = self._get_candidates(before)
        after_candidates = self._get_candidates(after)

        if before_candidates is None or after_candidates is None:
            return report

        # Build lookup maps
        before_id_map = self._build_id_map(before_candidates)
        after_id_map = self._build_id_map(after_candidates)
        before_identity_map = self._build_identity_map(before_candidates)
        after_identity_map = self._build_identity_map(after_candidates)

        # Build relationship maps
        before_relationships = self._build_relationships(
            before_candidates, before_id_map
        )
        after_relationships = self._build_relationships(
            after_candidates, after_id_map
        )

        # Validate each entity that has a parent
        all_identities = set(before_relationships.keys()) | set(after_relationships.keys())

        for identity in all_identities:
            before_rel = before_relationships.get(identity)
            after_rel = after_relationships.get(identity)

            result = self._validate_relationship(
                identity, before_rel, after_rel
            )

            report.results.append(result)

            if result.status == ParentValidationStatus.ORPHAN:
                report.orphans.append(result)
            elif result.status == ParentValidationStatus.PARENT_CHANGED:
                report.changed.append(result)
            elif result.status == ParentValidationStatus.CONSISTENT:
                report.consistent.append(result)

        return report

    def _get_candidates(self, data: dict) -> Optional[list]:
        """Extract candidates array from data using configured path."""
        path_parts = self.config.candidates_path.split('.')
        current = data

        for part in path_parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None

        return current if isinstance(current, list) else None

    def _get_identity_key(self, item: dict) -> Optional[tuple]:
        """Extract identity key value(s) from an item."""
        key_spec = self.config.identity_key
        if isinstance(key_spec, str):
            key_spec = [key_spec]

        values = []
        for key in key_spec:
            value = self._get_nested_value(item, key)
            if value is None:
                return None
            values.append(value)

        return tuple(values)

    def _get_nested_value(self, obj: dict, path: str) -> Any:
        """Get a nested value using dot notation."""
        parts = path.split('.')
        current = obj

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None

        return current

    def _build_id_map(self, candidates: list) -> dict[str, dict]:
        """Build map from internalId to candidate item."""
        id_map = {}
        for item in candidates:
            item_id = item.get(self.config.id_field)
            if item_id:
                id_map[item_id] = item
        return id_map

    def _build_identity_map(self, candidates: list) -> dict[tuple, dict]:
        """Build map from identity key to candidate item."""
        identity_map = {}
        for item in candidates:
            identity = self._get_identity_key(item)
            if identity:
                identity_map[identity] = item
        return identity_map

    def _build_relationships(
        self,
        candidates: list,
        id_map: dict[str, dict]
    ) -> dict[tuple, ParentRelationship]:
        """Build map of identity -> ParentRelationship."""
        relationships = {}

        for item in candidates:
            identity = self._get_identity_key(item)
            if not identity:
                continue

            child_id = item.get(self.config.id_field)
            parent_id = item.get(self.config.parent_field)

            if parent_id is None:
                # Root item, no parent relationship to track
                continue

            # Resolve parent identity
            parent_identity = None
            if parent_id in id_map:
                parent_item = id_map[parent_id]
                parent_identity = self._get_identity_key(parent_item)

            relationships[identity] = ParentRelationship(
                child_identity=identity,
                child_id=child_id,
                parent_id=parent_id,
                parent_identity=parent_identity
            )

        return relationships

    def _validate_relationship(
        self,
        identity: tuple,
        before_rel: Optional[ParentRelationship],
        after_rel: Optional[ParentRelationship]
    ) -> ParentValidationResult:
        """Validate a single entity's parent relationship."""

        # Check for orphans
        if before_rel and before_rel.parent_id and not before_rel.parent_identity:
            return ParentValidationResult(
                status=ParentValidationStatus.ORPHAN,
                entity_identity=identity,
                before_relationship=before_rel,
                after_relationship=after_rel,
                message=f"Orphan in BEFORE: parentId '{before_rel.parent_id}' not found"
            )

        if after_rel and after_rel.parent_id and not after_rel.parent_identity:
            return ParentValidationResult(
                status=ParentValidationStatus.ORPHAN,
                entity_identity=identity,
                before_relationship=before_rel,
                after_relationship=after_rel,
                message=f"Orphan in AFTER: parentId '{after_rel.parent_id}' not found"
            )

        # Check for relationship changes
        before_parent = before_rel.parent_identity if before_rel else None
        after_parent = after_rel.parent_identity if after_rel else None

        if before_parent != after_parent:
            # One might be None (entity only in before or after)
            if before_rel is None or after_rel is None:
                return ParentValidationResult(
                    status=ParentValidationStatus.VALID,
                    entity_identity=identity,
                    before_relationship=before_rel,
                    after_relationship=after_rel,
                    message="Entity only exists in one payload"
                )

            return ParentValidationResult(
                status=ParentValidationStatus.PARENT_CHANGED,
                entity_identity=identity,
                before_relationship=before_rel,
                after_relationship=after_rel,
                message=f"Parent changed: {before_parent} -> {after_parent}"
            )

        return ParentValidationResult(
            status=ParentValidationStatus.CONSISTENT,
            entity_identity=identity,
            before_relationship=before_rel,
            after_relationship=after_rel,
            message="Parent relationship consistent"
        )
