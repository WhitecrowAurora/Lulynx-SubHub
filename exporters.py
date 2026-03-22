from __future__ import annotations

import base64
import json
import re
from typing import Any, Iterable
from urllib.parse import parse_qs, quote, unquote, urlsplit, urlunsplit

from parsers import NodeEntry

SUPPORTED_EXPORT_FORMATS = ("base64", "plain", "json", "clash", "surge", "singbox")
_SURGE_SUPPORTED_PROTOCOLS = {"ss", "vmess", "trojan", "hysteria2", "hy2", "anytls"}
_MIHOMO_SUPPORTED_PROTOCOLS = {"ss", "ssr", "vmess", "vless", "trojan", "hysteria2", "hy2", "anytls"}
_SINGBOX_SUPPORTED_PROTOCOLS = {"ss", "vmess", "vless", "trojan", "hysteria2", "hy2", "anytls"}


def build_export(
    nodes: Iterable[NodeEntry],
    *,
    profile: dict[str, Any],
    format_name: str,
    generated_at: str,
) -> str:
    normalized = format_name.strip().lower()
    materialized = list(nodes)
    if normalized == "plain":
        return "\n".join(serialize_node_uri(node) for node in materialized)
    if normalized == "base64":
        plain_text = "\n".join(serialize_node_uri(node) for node in materialized)
        return base64.b64encode(plain_text.encode("utf-8")).decode("ascii")
    if normalized == "json":
        return json.dumps(
            {
                "profile": {
                    "id": profile["id"],
                    "name": profile["name"],
                    "mode": profile["mode"],
                },
                "generated_at": generated_at,
                "count": len(materialized),
                "nodes": [
                    {
                        "name": node.name,
                        "protocol": node.protocol,
                        "uri": serialize_node_uri(node),
                    }
                    for node in materialized
                ],
            },
            ensure_ascii=False,
        )
    if normalized == "clash":
        return build_mihomo_payload(materialized)
    if normalized == "surge":
        return build_surge_payload(materialized)
    if normalized == "singbox":
        return build_singbox_payload(materialized)
    raise ValueError("Unsupported subscription format.")


def build_mihomo_payload(nodes: Iterable[NodeEntry]) -> str:
    proxies: list[dict[str, Any]] = []
    warnings: list[str] = []
    for node in nodes:
        try:
            parsed = parse_node_uri(node)
        except ValueError as exc:
            warnings.append(f"{node.name}: {exc}")
            continue
        if parsed["protocol"] not in _MIHOMO_SUPPORTED_PROTOCOLS:
            warnings.append(f"{node.name}: mihomo 暂不导出 {parsed['protocol']}")
            continue
        try:
            proxies.append(_to_mihomo_proxy(parsed))
        except ValueError as exc:
            warnings.append(f"{node.name}: {exc}")

    lines: list[str] = []
    if warnings:
        lines.append("# Some nodes were omitted during mihomo export.")
        for warning in warnings[:20]:
            lines.append(f"# {warning}")
    lines.append("proxies:")
    if not proxies:
        lines.append("  []")
    else:
        lines.extend(_dump_yaml_list(proxies, indent=0))
    return "\n".join(lines)


def build_surge_payload(nodes: Iterable[NodeEntry]) -> str:
    lines = ["[Proxy]"]
    warnings: list[str] = []
    exported = 0
    for node in nodes:
        try:
            parsed = parse_node_uri(node)
        except ValueError as exc:
            warnings.append(f"{node.name}: {exc}")
            continue
        if parsed["protocol"] not in _SURGE_SUPPORTED_PROTOCOLS:
            warnings.append(f"{node.name}: Surge 暂不导出 {parsed['protocol']}")
            continue
        try:
            lines.append(_to_surge_line(parsed))
            exported += 1
        except ValueError as exc:
            warnings.append(f"{node.name}: {exc}")

    if exported == 0:
        lines.append("# No compatible proxies were exported.")
    if warnings:
        lines.append("")
        lines.append("# Omitted entries")
        for warning in warnings[:20]:
            lines.append(f"# {warning}")
    return "\n".join(lines)


def build_singbox_payload(nodes: Iterable[NodeEntry]) -> str:
    outbounds: list[dict[str, Any]] = []
    for node in nodes:
        try:
            parsed = parse_node_uri(node)
        except ValueError:
            continue
        if parsed["protocol"] not in _SINGBOX_SUPPORTED_PROTOCOLS:
            continue
        try:
            outbounds.append(_to_singbox_outbound(parsed))
        except ValueError:
            continue
    return json.dumps({"outbounds": outbounds}, ensure_ascii=False, indent=2)


def parse_node_uri(node: NodeEntry) -> dict[str, Any]:
    protocol = node.protocol.lower()
    if protocol == "ss":
        return _parse_ss_uri(node)
    if protocol == "ssr":
        return _parse_ssr_uri(node)
    if protocol == "vmess":
        return _parse_vmess_uri(node)
    if protocol == "vless":
        return _parse_vless_like_uri(node, "vless")
    if protocol == "trojan":
        return _parse_vless_like_uri(node, "trojan")
    if protocol in {"hy2", "hysteria2"}:
        return _parse_hysteria2_uri(node)
    if protocol == "anytls":
        return _parse_anytls_uri(node)
    raise ValueError(f"Unsupported node scheme: {protocol}")


def serialize_node_uri(node: NodeEntry) -> str:
    protocol = node.protocol.lower()
    if protocol == "vmess":
        return _serialize_vmess_uri(node)
    if protocol == "ssr":
        return _serialize_ssr_uri(node)
    return _serialize_fragment_uri(node)


def _parse_ss_uri(node: NodeEntry) -> dict[str, Any]:
    remainder = node.uri.split("://", 1)[1]
    payload = remainder.split("#", 1)[0]
    main, _, raw_query = payload.partition("?")
    params = parse_qs(raw_query, keep_blank_values=True)

    if "@" not in main:
        decoded = _decode_base64_text(main)
        if not decoded:
            raise ValueError("Invalid Shadowsocks URI.")
        main = decoded

    userinfo, host_port = main.rsplit("@", 1)
    if ":" not in userinfo:
        decoded_userinfo = _decode_base64_text(userinfo)
        if not decoded_userinfo or ":" not in decoded_userinfo:
            raise ValueError("Invalid Shadowsocks credentials.")
        userinfo = decoded_userinfo
    method, password = userinfo.split(":", 1)
    server, port = _split_host_port(host_port)
    plugin = _parse_plugin(params.get("plugin", [""])[0])

    return {
        "name": node.name,
        "protocol": "ss",
        "server": server,
        "port": port,
        "cipher": unquote(method),
        "password": unquote(password),
        "plugin": plugin,
        "udp": True,
    }


def _parse_ssr_uri(node: NodeEntry) -> dict[str, Any]:
    payload = node.uri.split("://", 1)[1]
    decoded = _decode_base64_text(payload)
    if not decoded:
        raise ValueError("Invalid ShadowsocksR URI.")
    main, _, raw_query = decoded.partition("/?")
    parts = main.split(":")
    if len(parts) != 6:
        raise ValueError("Invalid ShadowsocksR payload.")
    server, raw_port, protocol, method, obfs, raw_password = parts
    params = parse_qs(raw_query, keep_blank_values=True)
    password = _decode_base64_text(raw_password) or raw_password
    return {
        "name": node.name,
        "protocol": "ssr",
        "server": server,
        "port": int(raw_port),
        "cipher": method,
        "password": password,
        "protocol_name": protocol,
        "obfs": obfs,
        "obfs_param": _decode_base64_text(params.get("obfsparam", [""])[0]) or params.get("obfsparam", [""])[0],
        "protocol_param": _decode_base64_text(params.get("protoparam", [""])[0]) or params.get("protoparam", [""])[0],
        "udp": True,
    }


def _parse_vmess_uri(node: NodeEntry) -> dict[str, Any]:
    payload = node.uri.split("://", 1)[1].split("#", 1)[0]
    decoded = _decode_base64_text(payload)
    if not decoded:
        raise ValueError("Invalid VMess payload.")
    try:
        data = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid VMess JSON payload.") from exc

    security_mode = str(data.get("tls") or data.get("security") or "").lower()
    network = str(data.get("net") or "tcp").lower()
    host = str(data.get("host") or "")
    path = str(data.get("path") or "")
    alpn = _split_csv(data.get("alpn"))
    return {
        "name": node.name,
        "protocol": "vmess",
        "server": str(data.get("add") or ""),
        "port": int(data.get("port") or 0),
        "uuid": str(data.get("id") or ""),
        "alter_id": int(str(data.get("aid") or "0") or "0"),
        "security": str(data.get("scy") or "auto"),
        "network_name": network,
        "host": host,
        "path": path,
        "service_name": _normalize_service_name(path, data.get("path")),
        "headers": {"Host": host} if host else {},
        "tls_enabled": security_mode in {"tls", "reality"},
        "server_name": str(data.get("sni") or host or ""),
        "skip_cert_verify": _as_bool(data.get("allowInsecure")),
        "alpn": alpn,
        "client_fingerprint": str(data.get("fp") or ""),
        "reality_public_key": str(data.get("pbk") or ""),
        "reality_short_id": str(data.get("sid") or ""),
        "packet_encoding": str(data.get("packetEncoding") or data.get("packet-encoding") or ""),
        "udp": True,
    }


def _parse_vless_like_uri(node: NodeEntry, protocol: str) -> dict[str, Any]:
    parsed = urlsplit(node.uri)
    if not parsed.hostname or not parsed.port:
        raise ValueError(f"Invalid {protocol.upper()} URI.")
    params = parse_qs(parsed.query, keep_blank_values=True)
    security_mode = _first_param(params, "security").lower()
    network = (_first_param(params, "type") or "tcp").lower()
    host = _first_param(params, "host")
    raw_path = _first_param(params, "path")
    path = unquote(raw_path) if raw_path else ""
    userinfo = _rebuild_userinfo(parsed)
    server_name = (
        _first_param(params, "sni")
        or _first_param(params, "servername")
        or _first_param(params, "peer")
        or parsed.hostname
    )
    return {
        "name": node.name,
        "protocol": protocol,
        "server": parsed.hostname,
        "port": int(parsed.port),
        "uuid": userinfo if protocol == "vless" else "",
        "password": userinfo if protocol != "vless" else "",
        "flow": _first_param(params, "flow"),
        "encryption": _first_param(params, "encryption"),
        "network_name": network,
        "host": host,
        "path": path,
        "service_name": _first_param(params, "serviceName") or _normalize_service_name(path, raw_path),
        "headers": {"Host": host} if host else {},
        "tls_enabled": security_mode in {"tls", "reality"} or protocol == "trojan",
        "tls_mode": security_mode,
        "server_name": server_name,
        "skip_cert_verify": _as_bool(_first_param(params, "insecure")),
        "alpn": _split_csv(_first_param(params, "alpn")),
        "client_fingerprint": _first_param(params, "fp"),
        "reality_public_key": _first_param(params, "pbk"),
        "reality_short_id": _first_param(params, "sid"),
        "packet_encoding": _first_param(params, "packetEncoding") or _first_param(params, "packet-encoding"),
        "udp": True,
    }


def _parse_hysteria2_uri(node: NodeEntry) -> dict[str, Any]:
    parsed = urlsplit(node.uri)
    if not parsed.hostname:
        raise ValueError("Invalid Hysteria2 URI.")
    params = parse_qs(parsed.query, keep_blank_values=True)
    password = _rebuild_userinfo(parsed)
    if not password:
        password = _first_param(params, "password")
    ports = _first_param(params, "ports") or _first_param(params, "mport")
    return {
        "name": node.name,
        "protocol": "hysteria2",
        "server": parsed.hostname,
        "port": int(parsed.port or 0),
        "ports": ports,
        "hop_interval": _first_param(params, "hop-interval") or _first_param(params, "hopInterval"),
        "password": password,
        "up_mbps": _extract_speed(_first_param(params, "upmbps") or _first_param(params, "up")),
        "down_mbps": _extract_speed(_first_param(params, "downmbps") or _first_param(params, "down")),
        "obfs": _first_param(params, "obfs"),
        "obfs_password": _first_param(params, "obfs-password") or _first_param(params, "obfsPassword"),
        "server_name": _first_param(params, "sni") or _first_param(params, "peer") or parsed.hostname,
        "skip_cert_verify": _as_bool(_first_param(params, "insecure")),
        "alpn": _split_csv(_first_param(params, "alpn")),
        "fingerprint": _first_param(params, "fp"),
        "udp": True,
    }


def _parse_anytls_uri(node: NodeEntry) -> dict[str, Any]:
    parsed = urlsplit(node.uri)
    if not parsed.hostname or not parsed.port:
        raise ValueError("Invalid AnyTLS URI.")
    params = parse_qs(parsed.query, keep_blank_values=True)
    return {
        "name": node.name,
        "protocol": "anytls",
        "server": parsed.hostname,
        "port": int(parsed.port),
        "password": _rebuild_userinfo(parsed),
        "server_name": _first_param(params, "sni") or _first_param(params, "servername") or parsed.hostname,
        "skip_cert_verify": _as_bool(_first_param(params, "insecure")),
        "alpn": _split_csv(_first_param(params, "alpn")),
        "client_fingerprint": _first_param(params, "fp"),
        "idle_session_check_interval": _first_param(params, "idle-session-check-interval"),
        "idle_session_timeout": _first_param(params, "idle-session-timeout"),
        "min_idle_session": _first_param(params, "min-idle-session"),
        "udp": True,
    }


def _serialize_fragment_uri(node: NodeEntry) -> str:
    parsed = urlsplit(node.uri)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, quote(node.name or "")))


def _serialize_vmess_uri(node: NodeEntry) -> str:
    payload = node.uri.split("://", 1)[1].split("#", 1)[0]
    decoded = _decode_base64_text(payload)
    if not decoded:
        return _serialize_fragment_uri(node)
    try:
        data = json.loads(decoded)
    except json.JSONDecodeError:
        return _serialize_fragment_uri(node)
    data["ps"] = node.name
    encoded = base64.b64encode(
        json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    return f"vmess://{encoded}"


def _serialize_ssr_uri(node: NodeEntry) -> str:
    payload = node.uri.split("://", 1)[1]
    decoded = _decode_base64_text(payload)
    if not decoded:
        return _serialize_fragment_uri(node)
    main, separator, raw_query = decoded.partition("/?")
    if not separator:
        return _serialize_fragment_uri(node)
    params = parse_qs(raw_query, keep_blank_values=True)
    encoded_name = base64.urlsafe_b64encode((node.name or "").encode("utf-8")).decode("ascii").rstrip("=")
    params["remarks"] = [encoded_name]
    query_parts: list[str] = []
    for key, values in params.items():
        for value in values:
            query_parts.append(f"{key}={quote(str(value))}")
    rebuilt = f"{main}/?{'&'.join(query_parts)}"
    encoded_payload = base64.urlsafe_b64encode(rebuilt.encode("utf-8")).decode("ascii").rstrip("=")
    return f"ssr://{encoded_payload}"


def _to_mihomo_proxy(parsed: dict[str, Any]) -> dict[str, Any]:
    protocol = parsed["protocol"]
    if protocol == "ss":
        proxy = {
            "name": parsed["name"],
            "type": "ss",
            "server": parsed["server"],
            "port": parsed["port"],
            "cipher": parsed["cipher"],
            "password": parsed["password"],
            "udp": True,
        }
        plugin = parsed.get("plugin") or {}
        if plugin.get("name") in {"obfs-local", "simple-obfs"}:
            proxy["plugin"] = "obfs"
            opts: dict[str, Any] = {}
            mode = plugin.get("opts", {}).get("obfs")
            host = plugin.get("opts", {}).get("obfs-host")
            if mode:
                opts["mode"] = mode
            if host:
                opts["host"] = host
            if opts:
                proxy["plugin-opts"] = opts
        elif plugin.get("name") == "v2ray-plugin":
            proxy["plugin"] = "v2ray-plugin"
            opts = {"mode": "websocket"}
            host = plugin.get("opts", {}).get("host")
            path = plugin.get("opts", {}).get("path")
            if host:
                opts["host"] = host
            if path:
                opts["path"] = path
            if plugin.get("opts", {}).get("tls") == "true":
                opts["tls"] = True
            proxy["plugin-opts"] = opts
        return proxy

    if protocol == "ssr":
        proxy = {
            "name": parsed["name"],
            "type": "ssr",
            "server": parsed["server"],
            "port": parsed["port"],
            "cipher": parsed["cipher"],
            "password": parsed["password"],
            "obfs": parsed["obfs"],
            "protocol": parsed["protocol_name"],
            "udp": True,
        }
        if parsed.get("obfs_param"):
            proxy["obfs-param"] = parsed["obfs_param"]
        if parsed.get("protocol_param"):
            proxy["protocol-param"] = parsed["protocol_param"]
        return proxy

    if protocol == "vmess":
        proxy = {
            "name": parsed["name"],
            "type": "vmess",
            "server": parsed["server"],
            "port": parsed["port"],
            "uuid": parsed["uuid"],
            "alterId": parsed["alter_id"],
            "cipher": parsed["security"] or "auto",
            "udp": True,
        }
        _apply_mihomo_transport(proxy, parsed)
        _apply_mihomo_tls(proxy, parsed, servername_key="servername")
        if parsed.get("packet_encoding"):
            proxy["packet-encoding"] = parsed["packet_encoding"]
        return proxy

    if protocol == "vless":
        proxy = {
            "name": parsed["name"],
            "type": "vless",
            "server": parsed["server"],
            "port": parsed["port"],
            "uuid": parsed["uuid"],
            "udp": True,
        }
        if parsed.get("flow"):
            proxy["flow"] = parsed["flow"]
        if parsed.get("encryption"):
            proxy["encryption"] = parsed["encryption"]
        if parsed.get("packet_encoding"):
            proxy["packet-encoding"] = parsed["packet_encoding"]
        _apply_mihomo_transport(proxy, parsed)
        _apply_mihomo_tls(proxy, parsed, servername_key="servername")
        return proxy

    if protocol == "trojan":
        proxy = {
            "name": parsed["name"],
            "type": "trojan",
            "server": parsed["server"],
            "port": parsed["port"],
            "password": parsed["password"],
            "udp": True,
        }
        _apply_mihomo_transport(proxy, parsed)
        _apply_mihomo_tls(proxy, parsed, servername_key="sni")
        return proxy

    if protocol == "hysteria2":
        proxy = {
            "name": parsed["name"],
            "type": "hysteria2",
            "server": parsed["server"],
            "port": parsed["port"],
            "password": parsed["password"],
        }
        if parsed.get("ports"):
            proxy["ports"] = parsed["ports"]
        if parsed.get("hop_interval"):
            proxy["hop-interval"] = parsed["hop_interval"]
        if parsed.get("up_mbps"):
            proxy["up"] = f"{parsed['up_mbps']} Mbps"
        if parsed.get("down_mbps"):
            proxy["down"] = f"{parsed['down_mbps']} Mbps"
        if parsed.get("obfs"):
            proxy["obfs"] = parsed["obfs"]
        if parsed.get("obfs_password"):
            proxy["obfs-password"] = parsed["obfs_password"]
        _apply_mihomo_tls(proxy, parsed, servername_key="sni")
        return proxy

    if protocol == "anytls":
        proxy = {
            "name": parsed["name"],
            "type": "anytls",
            "server": parsed["server"],
            "port": parsed["port"],
            "password": parsed["password"],
            "udp": True,
        }
        if parsed.get("client_fingerprint"):
            proxy["client-fingerprint"] = parsed["client_fingerprint"]
        if parsed.get("idle_session_check_interval"):
            proxy["idle-session-check-interval"] = _safe_int(parsed["idle_session_check_interval"])
        if parsed.get("idle_session_timeout"):
            proxy["idle-session-timeout"] = _safe_int(parsed["idle_session_timeout"])
        if parsed.get("min_idle_session"):
            proxy["min-idle-session"] = _safe_int(parsed["min_idle_session"])
        _apply_mihomo_tls(proxy, parsed, servername_key="sni")
        return proxy

    raise ValueError(f"mihomo export does not support {protocol}")


def _to_surge_line(parsed: dict[str, Any]) -> str:
    protocol = parsed["protocol"]
    name = _sanitize_surge_name(parsed["name"])
    base = f"{name} = "
    if protocol == "ss":
        line = (
            f"{base}ss, {parsed['server']}, {parsed['port']}, "
            f"encrypt-method={parsed['cipher']}, password={_surge_value(parsed['password'])}"
        )
        plugin = parsed.get("plugin") or {}
        if plugin.get("name") in {"obfs-local", "simple-obfs"}:
            mode = plugin.get("opts", {}).get("obfs")
            host = plugin.get("opts", {}).get("obfs-host")
            if mode:
                line += f", obfs={mode}"
            if host:
                line += f", obfs-host={_surge_value(host)}"
        line += ", udp-relay=true"
        return line

    if protocol == "vmess":
        line = f"{base}vmess, {parsed['server']}, {parsed['port']}, username={parsed['uuid']}"
        if parsed.get("network_name") == "ws":
            line += ", ws=true"
            if parsed.get("path"):
                line += f", ws-path={_surge_value(parsed['path'])}"
            host = parsed.get("host")
            if host:
                line += f", ws-headers=Host:{host}"
        _append_surge_tls(parsed, line_parts := [line])
        return line_parts[0]

    if protocol == "trojan":
        line = f"{base}trojan, {parsed['server']}, {parsed['port']}, password={_surge_value(parsed['password'])}"
        if parsed.get("network_name") == "ws":
            line += ", ws=true"
            if parsed.get("path"):
                line += f", ws-path={_surge_value(parsed['path'])}"
            host = parsed.get("host")
            if host:
                line += f", ws-headers=Host:{host}"
        _append_surge_tls(parsed, line_parts := [line])
        return line_parts[0]

    if protocol == "hysteria2":
        line = f"{base}hysteria2, {parsed['server']}, {parsed['port']}, password={_surge_value(parsed['password'])}"
        if parsed.get("down_mbps"):
            line += f", download-bandwidth={parsed['down_mbps']}"
        if parsed.get("ports"):
            line += f", port-hopping={parsed['ports']}"
        if parsed.get("hop_interval"):
            line += f", port-hopping-interval={_extract_speed(parsed['hop_interval']) or parsed['hop_interval']}"
        _append_surge_tls(parsed, line_parts := [line])
        return line_parts[0]

    if protocol == "anytls":
        line = f"{base}anytls, {parsed['server']}, {parsed['port']}, password={_surge_value(parsed['password'])}"
        _append_surge_tls(parsed, line_parts := [line])
        return line_parts[0]

    raise ValueError(f"Surge export does not support {protocol}")


def _to_singbox_outbound(parsed: dict[str, Any]) -> dict[str, Any]:
    protocol = parsed["protocol"]
    if protocol == "ss":
        outbound = {
            "type": "shadowsocks",
            "tag": parsed["name"],
            "server": parsed["server"],
            "server_port": parsed["port"],
            "method": parsed["cipher"],
            "password": parsed["password"],
        }
        plugin = parsed.get("plugin") or {}
        if plugin.get("name"):
            outbound["plugin"] = plugin["name"]
            outbound["plugin_opts"] = plugin.get("raw_options", "")
        return outbound

    if protocol == "vmess":
        outbound = {
            "type": "vmess",
            "tag": parsed["name"],
            "server": parsed["server"],
            "server_port": parsed["port"],
            "uuid": parsed["uuid"],
            "security": parsed["security"] or "auto",
            "alter_id": parsed["alter_id"],
        }
        _apply_singbox_transport(outbound, parsed)
        _apply_singbox_tls(outbound, parsed)
        if parsed.get("packet_encoding"):
            outbound["packet_encoding"] = parsed["packet_encoding"]
        return outbound

    if protocol == "vless":
        outbound = {
            "type": "vless",
            "tag": parsed["name"],
            "server": parsed["server"],
            "server_port": parsed["port"],
            "uuid": parsed["uuid"],
        }
        if parsed.get("flow"):
            outbound["flow"] = parsed["flow"]
        _apply_singbox_transport(outbound, parsed)
        _apply_singbox_tls(outbound, parsed)
        if parsed.get("packet_encoding"):
            outbound["packet_encoding"] = parsed["packet_encoding"]
        return outbound

    if protocol == "trojan":
        outbound = {
            "type": "trojan",
            "tag": parsed["name"],
            "server": parsed["server"],
            "server_port": parsed["port"],
            "password": parsed["password"],
        }
        _apply_singbox_transport(outbound, parsed)
        _apply_singbox_tls(outbound, parsed)
        return outbound

    if protocol == "hysteria2":
        outbound = {
            "type": "hysteria2",
            "tag": parsed["name"],
            "server": parsed["server"],
            "password": parsed["password"],
            "tls": {
                "enabled": True,
                "server_name": parsed.get("server_name") or parsed["server"],
                "insecure": bool(parsed.get("skip_cert_verify")),
            },
        }
        if parsed.get("ports"):
            outbound["server_ports"] = [item.strip() for item in str(parsed["ports"]).split(",") if item.strip()]
        else:
            outbound["server_port"] = parsed["port"]
        if parsed.get("hop_interval"):
            outbound["hop_interval"] = _normalize_duration(parsed["hop_interval"])
        if parsed.get("up_mbps"):
            outbound["up_mbps"] = parsed["up_mbps"]
        if parsed.get("down_mbps"):
            outbound["down_mbps"] = parsed["down_mbps"]
        if parsed.get("obfs") or parsed.get("obfs_password"):
            outbound["obfs"] = {
                "type": parsed.get("obfs") or "salamander",
                "password": parsed.get("obfs_password") or "",
            }
        if parsed.get("alpn"):
            outbound["tls"]["alpn"] = parsed["alpn"]
        return outbound

    if protocol == "anytls":
        outbound = {
            "type": "anytls",
            "tag": parsed["name"],
            "server": parsed["server"],
            "server_port": parsed["port"],
            "password": parsed["password"],
        }
        if parsed.get("idle_session_check_interval"):
            outbound["idle_session_check_interval"] = _normalize_duration(parsed["idle_session_check_interval"])
        if parsed.get("idle_session_timeout"):
            outbound["idle_session_timeout"] = _normalize_duration(parsed["idle_session_timeout"])
        if parsed.get("min_idle_session"):
            outbound["min_idle_session"] = _safe_int(parsed["min_idle_session"])
        _apply_singbox_tls(outbound, parsed)
        return outbound

    raise ValueError(f"sing-box export does not support {protocol}")


def _apply_mihomo_transport(proxy: dict[str, Any], parsed: dict[str, Any]) -> None:
    network = parsed.get("network_name") or "tcp"
    if network not in {"tcp", "ws", "grpc", "http", "h2"}:
        network = "tcp"
    proxy["network"] = network
    if network == "ws":
        ws_opts: dict[str, Any] = {}
        if parsed.get("path"):
            ws_opts["path"] = parsed["path"]
        if parsed.get("headers"):
            ws_opts["headers"] = parsed["headers"]
        if ws_opts:
            proxy["ws-opts"] = ws_opts
    elif network == "grpc":
        service_name = parsed.get("service_name")
        if service_name:
            proxy["grpc-opts"] = {"grpc-service-name": service_name}
    elif network == "http":
        http_opts: dict[str, Any] = {"method": "GET"}
        if parsed.get("path"):
            http_opts["path"] = [parsed["path"]]
        if parsed.get("host"):
            http_opts["headers"] = {"Host": [parsed["host"]]}
        proxy["http-opts"] = http_opts
    elif network == "h2":
        h2_opts: dict[str, Any] = {}
        if parsed.get("host"):
            h2_opts["host"] = [parsed["host"]]
        if parsed.get("path"):
            h2_opts["path"] = parsed["path"]
        if h2_opts:
            proxy["h2-opts"] = h2_opts


def _apply_mihomo_tls(proxy: dict[str, Any], parsed: dict[str, Any], *, servername_key: str) -> None:
    if parsed.get("tls_enabled"):
        proxy["tls"] = True
    if parsed.get("server_name"):
        proxy[servername_key] = parsed["server_name"]
    if parsed.get("skip_cert_verify"):
        proxy["skip-cert-verify"] = True
    if parsed.get("alpn"):
        proxy["alpn"] = parsed["alpn"]
    if parsed.get("client_fingerprint"):
        proxy["client-fingerprint"] = parsed["client_fingerprint"]
    if parsed.get("fingerprint"):
        proxy["fingerprint"] = parsed["fingerprint"]
    if parsed.get("reality_public_key"):
        proxy["reality-opts"] = {
            "public-key": parsed["reality_public_key"],
            "short-id": parsed.get("reality_short_id", ""),
        }


def _append_surge_tls(parsed: dict[str, Any], line_parts: list[str]) -> None:
    line = line_parts[0]
    if parsed.get("skip_cert_verify"):
        line += ", skip-cert-verify=true"
    if parsed.get("server_name"):
        line += f", sni={_surge_value(parsed['server_name'])}"
    line_parts[0] = line


def _apply_singbox_transport(outbound: dict[str, Any], parsed: dict[str, Any]) -> None:
    network = parsed.get("network_name") or "tcp"
    if network not in {"tcp", "ws", "grpc", "http", "h2"}:
        network = "tcp"
    outbound["network"] = network
    if network == "ws":
        transport: dict[str, Any] = {"type": "ws"}
        if parsed.get("path"):
            transport["path"] = parsed["path"]
        if parsed.get("headers"):
            transport["headers"] = parsed["headers"]
        outbound["transport"] = transport
    elif network == "grpc":
        outbound["transport"] = {
            "type": "grpc",
            "service_name": parsed.get("service_name") or "",
        }
    elif network in {"http", "h2"}:
        outbound["transport"] = {
            "type": "http",
            "host": [parsed["host"]] if parsed.get("host") else [],
            "path": parsed.get("path") or "",
            "method": "GET",
            "headers": parsed.get("headers") or {},
        }


def _apply_singbox_tls(outbound: dict[str, Any], parsed: dict[str, Any]) -> None:
    if not parsed.get("tls_enabled"):
        return
    tls: dict[str, Any] = {
        "enabled": True,
        "server_name": parsed.get("server_name") or parsed["server"],
        "insecure": bool(parsed.get("skip_cert_verify")),
    }
    if parsed.get("alpn"):
        tls["alpn"] = parsed["alpn"]
    if parsed.get("client_fingerprint"):
        tls["utls"] = {
            "enabled": True,
            "fingerprint": parsed["client_fingerprint"],
        }
    if parsed.get("reality_public_key"):
        tls["reality"] = {
            "enabled": True,
            "public_key": parsed["reality_public_key"],
            "short_id": parsed.get("reality_short_id") or "",
        }
    outbound["tls"] = tls


def _dump_yaml_list(items: list[dict[str, Any]], *, indent: int) -> list[str]:
    lines: list[str] = []
    space = " " * indent
    for item in items:
        keys = list(item.keys())
        if not keys:
            lines.append(f"{space}- {{}}")
            continue
        first_key = keys[0]
        lines.append(f"{space}- {first_key}: {_yaml_scalar(item[first_key])}")
        for key in keys[1:]:
            lines.extend(_dump_yaml_value(key, item[key], indent + 2))
    return lines


def _dump_yaml_value(key: str, value: Any, indent: int) -> list[str]:
    space = " " * indent
    if isinstance(value, dict):
        lines = [f"{space}{key}:"]
        for child_key, child_value in value.items():
            lines.extend(_dump_yaml_value(child_key, child_value, indent + 2))
        return lines
    if isinstance(value, list):
        if not value:
            return [f"{space}{key}: []"]
        lines = [f"{space}{key}:"]
        for item in value:
            if isinstance(item, dict):
                lines.extend(_dump_yaml_list([item], indent=indent + 2))
            else:
                lines.append(f"{space}  - {_yaml_scalar(item)}")
        return lines
    return [f"{space}{key}: {_yaml_scalar(value)}"]


def _yaml_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return '""'
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if text == "":
        return '""'
    if re.fullmatch(r"[A-Za-z0-9._:/+-]+", text):
        return text
    return json.dumps(text, ensure_ascii=False)


def _parse_plugin(raw_value: str) -> dict[str, Any] | None:
    candidate = unquote(raw_value or "").strip()
    if not candidate:
        return None
    parts = [part for part in candidate.split(";") if part]
    if not parts:
        return None
    opts: dict[str, str] = {}
    for part in parts[1:]:
        if "=" in part:
            key, value = part.split("=", 1)
            opts[key] = value
        else:
            opts[part] = "true"
    return {"name": parts[0], "opts": opts, "raw_options": ";".join(parts[1:])}


def _split_host_port(value: str) -> tuple[str, int]:
    candidate = value.strip()
    if candidate.startswith("["):
        end = candidate.find("]")
        if end == -1:
            raise ValueError("Invalid host/port value.")
        host = candidate[1:end]
        port_text = candidate[end + 2 :]
        return host, int(port_text)
    host, _, port_text = candidate.rpartition(":")
    if not host or not port_text:
        raise ValueError("Invalid host/port value.")
    return host, int(port_text)


def _decode_base64_text(value: str) -> str | None:
    compact = "".join(str(value or "").split())
    if not compact:
        return None
    padded = compact + ("=" * (-len(compact) % 4))
    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            decoded = decoder(padded)
        except Exception:
            continue
        for encoding in ("utf-8", "utf-8-sig", "latin-1"):
            try:
                return decoded.decode(encoding)
            except UnicodeDecodeError:
                continue
    return None


def _first_param(params: dict[str, list[str]], key: str) -> str:
    return str(params.get(key, [""])[0] or "")


def _split_csv(value: Any) -> list[str]:
    if isinstance(value, list):
        items = value
    else:
        items = str(value or "").split(",")
    return [item.strip() for item in items if str(item).strip()]


def _as_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _normalize_service_name(path: str, raw_path: Any) -> str:
    candidate = str(raw_path or path or "").strip()
    if not candidate:
        return ""
    return candidate.lstrip("/")


def _extract_speed(value: Any) -> int | None:
    candidate = str(value or "").strip()
    if not candidate:
        return None
    match = re.search(r"(\d+)", candidate)
    if not match:
        return None
    return int(match.group(1))


def _safe_int(value: Any) -> int:
    match = re.search(r"(\d+)", str(value or ""))
    return int(match.group(1)) if match else 0


def _normalize_duration(value: Any) -> str:
    candidate = str(value or "").strip()
    if not candidate:
        return ""
    if re.fullmatch(r"\d+[smh]?", candidate):
        return candidate if candidate[-1].isalpha() else f"{candidate}s"
    match = re.search(r"(\d+)", candidate)
    if not match:
        return candidate
    return f"{match.group(1)}s"


def _rebuild_userinfo(parsed: Any) -> str:
    username = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    if password:
        return f"{username}:{password}"
    return username


def _sanitize_surge_name(value: str) -> str:
    candidate = re.sub(r"[\r\n=,]", " ", str(value or "")).strip()
    return candidate or "Proxy"


def _surge_value(value: Any) -> str:
    text = str(value or "")
    if "," in text or " " in text:
        return f"\"{text}\""
    return text
