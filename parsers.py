from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import parse_qs, unquote, urlsplit

SUPPORTED_PROTOCOLS = (
    "ss",
    "ssr",
    "vmess",
    "vless",
    "trojan",
    "hy2",
    "hysteria2",
    "anytls",
)

_SUPPORTED_PREFIXES = tuple(f"{protocol}://" for protocol in SUPPORTED_PROTOCOLS)
_URI_PATTERN = re.compile(
    r"(?i)(ss|ssr|vmess|vless|trojan|hy2|hysteria2|anytls)://[^\s]+"
)


@dataclass(slots=True)
class NodeEntry:
    uri: str
    name: str
    protocol: str


@dataclass(slots=True)
class ParseResult:
    nodes: list[NodeEntry]
    source_format: str


def parse_subscription_payload(payload: bytes) -> ParseResult:
    text = _decode_text(payload)
    lines = _extract_uri_lines(text)
    source_format = "plain_text"

    if not lines:
        decoded = _try_decode_base64_blob(text)
        if decoded:
            lines = _extract_uri_lines(decoded)
            source_format = "base64_blob"

    if not lines:
        raise ValueError("No supported node links were found in the subscription payload.")

    nodes: list[NodeEntry] = []
    seen: set[str] = set()
    for raw_line in lines:
        uri = raw_line.strip()
        if not uri or uri in seen:
            continue
        protocol = get_protocol(uri)
        name = extract_node_name(uri) or _fallback_node_name(uri, protocol)
        nodes.append(NodeEntry(uri=uri, name=name, protocol=protocol))
        seen.add(uri)

    if not nodes:
        raise ValueError("The subscription contained only unsupported or empty entries.")

    return ParseResult(nodes=nodes, source_format=source_format)


def filter_nodes(nodes: Iterable[NodeEntry], exclude_keywords: Iterable[str]) -> list[NodeEntry]:
    keywords = [keyword.strip().lower() for keyword in exclude_keywords if keyword.strip()]
    if not keywords:
        return list(nodes)

    filtered: list[NodeEntry] = []
    for node in nodes:
        haystack = f"{node.name} {node.uri}".lower()
        if any(keyword in haystack for keyword in keywords):
            continue
        filtered.append(node)
    return filtered


def split_keywords(raw_keywords: str) -> list[str]:
    return [item.strip() for item in re.split(r"[\r\n,]+", raw_keywords) if item.strip()]


def get_protocol(uri: str) -> str:
    lowered = uri.lower()
    for prefix in _SUPPORTED_PREFIXES:
        if lowered.startswith(prefix):
            return prefix[:-3]
    raise ValueError(f"Unsupported node scheme: {uri[:24]}")


def extract_node_name(uri: str) -> str:
    protocol = get_protocol(uri)
    if protocol == "vmess":
        name = _extract_vmess_name(uri)
        if name:
            return name
    if protocol == "ssr":
        name = _extract_ssr_name(uri)
        if name:
            return name

    parsed = urlsplit(uri)
    if parsed.fragment:
        return _clean_node_name(unquote(parsed.fragment))

    return ""


def _extract_uri_lines(text: str) -> list[str]:
    lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if any(stripped.lower().startswith(prefix) for prefix in _SUPPORTED_PREFIXES):
            lines.append(stripped)

    if lines:
        return lines

    return [match.group(0).strip() for match in _URI_PATTERN.finditer(text)]


def _decode_text(payload: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "gb18030", "big5", "latin-1"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode("utf-8", errors="replace")


def _try_decode_base64_blob(text: str) -> str | None:
    compact = "".join(text.split())
    if len(compact) < 16:
        return None
    if not re.fullmatch(r"[A-Za-z0-9+/=_-]+", compact):
        return None

    decoded = _decode_base64_bytes(compact)
    if not decoded:
        return None
    return _decode_text(decoded)


def _decode_base64_bytes(value: str) -> bytes | None:
    compact = "".join(value.split())
    if not compact:
        return None

    padding = (-len(compact)) % 4
    candidate = compact + ("=" * padding)

    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            return decoder(candidate)
        except Exception:
            continue
    return None


def _extract_vmess_name(uri: str) -> str:
    payload = uri.split("://", 1)[1]
    payload = payload.split("#", 1)[0]
    decoded = _decode_base64_bytes(payload)
    if not decoded:
        return ""

    try:
        data = json.loads(decoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return ""

    return _clean_node_name(str(data.get("ps", "")))


def _extract_ssr_name(uri: str) -> str:
    payload = uri.split("://", 1)[1]
    decoded = _decode_base64_bytes(payload)
    if not decoded:
        return ""

    try:
        decoded_text = decoded.decode("utf-8")
    except UnicodeDecodeError:
        decoded_text = decoded.decode("utf-8", errors="replace")

    _, _, query = decoded_text.partition("/?")
    if not query:
        return ""

    params = parse_qs(query, keep_blank_values=True)
    raw_name = params.get("remarks", [""])[0]
    if not raw_name:
        return ""

    decoded_name = _decode_base64_bytes(unquote(raw_name))
    if not decoded_name:
        return _clean_node_name(unquote(raw_name))

    try:
        return _clean_node_name(decoded_name.decode("utf-8"))
    except UnicodeDecodeError:
        return _clean_node_name(decoded_name.decode("utf-8", errors="replace"))


def _clean_node_name(name: str) -> str:
    return re.sub(r"\s+", " ", name).strip()


def _fallback_node_name(uri: str, protocol: str) -> str:
    parsed = urlsplit(uri)
    host = parsed.hostname or parsed.netloc or protocol.upper()
    return f"{protocol.upper()} {host}"
