# openapi_override.py
# type: ignore

import copy
import inspect
from enum import Enum
from typing import Any, Iterable, cast

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, TypeAdapter

import backend.lib.websocket.types as ws_types


def _open_string_enums(node: Any) -> None:
    """
    Recursively transform any closed string enum:
        { "type": "string", "enum": ["a","b"] }
    into an "open" enum per Apple's guidance:
        { "anyOf": [ { "type":"string", "enum":[...] }, { "type":"string" } ] }

    We skip nodes that already have "anyOf"/"oneOf" to avoid double wrapping.
    """
    if isinstance(node, dict):
        # If it's already a composition, just recurse inside and return
        if "anyOf" in node or "oneOf" in node or "allOf" in node:
            for v in node.values():
                _open_string_enums(v)
            return

        # Detect a closed string enum
        t = node.get("type")
        enum_vals = node.get("enum")
        if t == "string" and isinstance(enum_vals, list) and enum_vals:
            # Build the closed enum schema and an open string schema
            closed = {"type": "string", "enum": list(enum_vals)}
            # Carry through descriptive fields that belong on the closed side
            for carry_key in (
                "description",
                "title",
                "examples",
                "default",
                "deprecated",
                "readOnly",
                "writeOnly",
            ):
                if carry_key in node and carry_key not in closed:
                    closed[carry_key] = node[carry_key]

            # Replace current node with anyOf wrapper (open enum)
            # Note: we intentionally *replace* keys to a minimal, clear shape
            node.clear()
            node.update({"anyOf": [closed, {"type": "string"}]})
            # Done for this node; no further recursion inside 'closed'
            return

        # Recurse into child dict/list values
        for v in node.values():
            _open_string_enums(v)

    elif isinstance(node, list):
        for item in node:
            _open_string_enums(item)


# --- JSON-schema utils ---------------------------------------------------------
def _ensure_components_schemas(doc: dict[str, Any]) -> dict[str, Any]:
    doc.setdefault("components", {}).setdefault("schemas", {})
    return doc["components"]["schemas"]


def _schema_of(obj: type[Any] | Any) -> dict[str, Any]:
    """
    Return a JSON Schema for a Pydantic BaseModel/Enum/typing construct
    (including Annotated[Union[...], Field(discriminator=...)]).
    """
    try:
        return obj.model_json_schema(ref_template="#/components/schemas/{model}")  # type: ignore[attr-defined]
    except AttributeError:
        return TypeAdapter(obj).json_schema(ref_template="#/components/schemas/{model}")


def _walk_replace_refs(node: Any) -> None:
    """Rewrite any $ref '#/$defs/...' → '#/components/schemas/...'. In-place."""
    if isinstance(node, dict):
        if (
            "$ref" in node
            and isinstance(node["$ref"], str)
            and node["$ref"].startswith("#/$defs/")
        ):
            tail = node["$ref"].split("#/$defs/", 1)[1]
            node["$ref"] = f"#/components/schemas/{tail}"
        for v in node.values():
            _walk_replace_refs(v)
    elif isinstance(node, list):
        for v in node:
            _walk_replace_refs(v)


def _hoist_defs(
    schema: dict[str, Any], components_schemas: dict[str, Any]
) -> dict[str, Any]:
    """Move local $defs into components.schemas and fix $ref paths. Returns cleaned schema."""
    schema = copy.deepcopy(schema)
    local_defs = schema.pop("$defs", None)
    if local_defs:
        for name, def_schema in local_defs.items():
            if name not in components_schemas:
                _walk_replace_refs(def_schema)
                components_schemas[name] = def_schema
        _walk_replace_refs(schema)
    return schema


def _rewrite_nullable(schema: Any) -> None:
    """
    Recursively rewrite anyOf [X, null] → either type: [X,"null"] or allOf + nullable: true
    to appease generators that prefer OpenAPI-3 style nullables.
    """
    if isinstance(schema, dict):
        if "anyOf" in schema:
            any_of = cast("Any", schema["anyOf"])
            if isinstance(any_of, list) and any(
                isinstance(fragment, dict) and fragment.get("type") == "null"
                for fragment in any_of
            ):
                non_null = [
                    fragment
                    for fragment in any_of
                    if not (
                        isinstance(fragment, dict) and fragment.get("type") == "null"
                    )
                ]
                if len(non_null) == 1 and isinstance(non_null[0], dict):
                    base = dict(non_null[0])
                    if "$ref" in base:
                        replacement: dict[str, Any] = {
                            "allOf": [{"$ref": base["$ref"]}],
                            "nullable": True,
                        }
                    else:
                        replacement = base
                        old_type = replacement.get("type")
                        if old_type is not None:
                            if isinstance(old_type, list):
                                if "null" not in old_type:
                                    replacement["type"] = old_type + ["null"]
                            else:
                                replacement["type"] = [old_type, "null"]

                    for key, value in schema.items():
                        if key != "anyOf":
                            replacement.setdefault(key, value)

                    schema.clear()
                    schema.update(replacement)

        for value in list(schema.values()):
            _rewrite_nullable(value)

    elif isinstance(schema, list):
        for fragment in schema:
            _rewrite_nullable(fragment)


# --- Auto-collect your WS types ------------------------------------------------
def _collect_ws_types() -> Iterable[type[Any]]:
    for _, obj in inspect.getmembers(ws_types):
        if inspect.isclass(obj):
            if issubclass(obj, BaseModel) and obj is not BaseModel:
                yield obj
            elif issubclass(obj, Enum) and obj is not Enum:
                yield obj


# --- Discriminator mapping patch ----------------------------------------------
def _extract_fixed_event_value(subtype_schema: dict[str, Any]) -> str | None:
    """
    Given a concrete message schema with properties.event, return its fixed value.
    Supports:
      properties.event.enum: [ "value" ]   (OAS 3.0 style)
      properties.event.const: "value"      (OAS 3.1 JSON Schema style)
    """
    props = subtype_schema.get("properties") or {}
    event_schema = props.get("event") or {}
    if (
        "enum" in event_schema
        and isinstance(event_schema["enum"], list)
        and event_schema["enum"]
    ):
        return event_schema["enum"][0]
    if "const" in event_schema and isinstance(event_schema["const"], str):
        return event_schema["const"]
    return None


def _add_discriminator_mapping(
    components_schemas: dict[str, Any], union_name: str
) -> None:
    """
    For a union already in components (with oneOf refs), add:
      discriminator:
        propertyName: event
        mapping:
          <event_value>: '#/components/schemas/<SubtypeName>'
    """
    union_schema = components_schemas.get(union_name)
    if not union_schema:
        return
    one_of = union_schema.get("oneOf")
    if not isinstance(one_of, list):
        return

    mapping: dict[str, str] = {}
    for item in one_of:
        if not (isinstance(item, dict) and "$ref" in item):
            continue
        ref: str = item["$ref"]
        subtype_name = ref.split("/")[-1]
        subtype_schema = components_schemas.get(subtype_name)
        if not isinstance(subtype_schema, dict):
            continue
        event_value = _extract_fixed_event_value(subtype_schema)
        if event_value:
            mapping[event_value] = f"#/components/schemas/{subtype_name}"

    if mapping:
        discriminator = union_schema.setdefault("discriminator", {})
        discriminator["propertyName"] = "event"
        # Only set mapping if absent or empty; otherwise merge
        existing = discriminator.get("mapping") or {}
        if isinstance(existing, dict):
            existing.update(mapping)
            discriminator["mapping"] = existing
        else:
            discriminator["mapping"] = mapping


def _ensure_union_schemas_present(components_schemas: dict[str, Any]) -> None:
    """
    Ensure the two Annotated unions exist in components (in case the default
    generator didn't emit them because they weren't referenced by any path).
    """
    for union_attr in ("ClientToServerMessage", "ServerToClientMessage"):
        if union_attr in components_schemas:
            continue
        try:
            union_type = getattr(ws_types, union_attr)
        except AttributeError:
            continue
        raw_schema = _schema_of(union_type)
        cleaned = _hoist_defs(raw_schema, components_schemas)
        components_schemas[union_attr] = cleaned


# --- Public entrypoint ---------------------------------------------------------
def build_base_openapi(app: FastAPI) -> dict[str, Any]:
    """
    Canonical API schema:
      - hoists defs
      - ensures WS unions & discriminator mappings
      - rewrites nullables (generator-friendly)
    Does NOT open enums.
    """
    if getattr(app, "openapi_schema", None):
        return app.openapi_schema  # type: ignore[return-value]

    schema: dict[str, Any] = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )

    components_schemas = _ensure_components_schemas(schema)

    # ensure your WS types/enums appear
    for typ in _collect_ws_types():
        name = typ.__name__
        if name in components_schemas:
            continue
        raw_schema = _schema_of(typ)
        cleaned = _hoist_defs(raw_schema, components_schemas)
        components_schemas[name] = cleaned

    # ensure union aliases
    _ensure_union_schemas_present(components_schemas)

    # normalize nullables
    _rewrite_nullable(schema)

    # discriminator mapping for WS unions
    _add_discriminator_mapping(components_schemas, "ClientToServerMessage")
    _add_discriminator_mapping(components_schemas, "ServerToClientMessage")

    app.openapi_schema = schema  # cache canonical
    return schema


def build_swift_openapi(app: FastAPI) -> dict[str, Any]:
    """
    Swift overlay:
      - start from the canonical cached schema,
      - deep-copy,
      - apply open-enum transform for *string* enums only.
    """
    base = build_base_openapi(app)
    swift_schema = copy.deepcopy(base)
    _open_string_enums(swift_schema)  # <— the transformer from earlier
    return swift_schema
