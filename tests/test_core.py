from __future__ import annotations

import base64
import json
import tempfile
import threading
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

from app import PanelHandler
from manager import SubscriptionManager
from parsers import extract_node_name, parse_subscription_payload
from http.server import ThreadingHTTPServer


class FakeSubscriptionManager(SubscriptionManager):
    def __init__(self, db_path: str | Path, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads
        self.sent_telegram_messages: list[tuple[str, str, str]] = []
        self.sent_webhook_payloads: list[tuple[dict[str, object], str]] = []
        super().__init__(db_path)

    def _download_subscription(self, url: str) -> bytes:
        if url not in self.payloads:
            raise RuntimeError("missing payload")
        return self.payloads[url]

    def _send_telegram_message(self, message: str, bot_token: str, chat_id: str) -> None:
        self.sent_telegram_messages.append((message, bot_token, chat_id))

    def _send_webhook_payload(self, payload: dict[str, object], webhook_url: str) -> None:
        self.sent_webhook_payloads.append((payload, webhook_url))


class ParserTests(unittest.TestCase):
    def test_parse_base64_subscription_blob(self) -> None:
        raw_lines = "\n".join(
            [
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#HK-Alpha",
                "trojan://password@example.net:443?security=tls#US-Beta",
            ]
        )
        payload = base64.b64encode(raw_lines.encode("utf-8"))
        result = parse_subscription_payload(payload)

        self.assertEqual(result.source_format, "base64_blob")
        self.assertEqual(len(result.nodes), 2)
        self.assertEqual(result.nodes[0].name, "HK-Alpha")
        self.assertEqual(result.nodes[1].protocol, "trojan")

    def test_extract_vmess_name(self) -> None:
        vmess_payload = {
            "v": "2",
            "ps": "JP-Relay",
            "add": "example.org",
            "port": "443",
            "id": "12345678-1234-1234-1234-123456789012",
            "aid": "0",
            "net": "ws",
            "type": "none",
            "host": "",
            "path": "/",
            "tls": "tls",
        }
        encoded = base64.b64encode(json.dumps(vmess_payload).encode("utf-8")).decode("ascii")
        uri = f"vmess://{encoded}"
        self.assertEqual(extract_node_name(uri), "JP-Relay")

    def test_extract_ssr_name(self) -> None:
        remarks = base64.urlsafe_b64encode("SG-SSR".encode("utf-8")).decode("ascii").rstrip("=")
        core = "example.com:443:origin:aes-256-cfb:plain:cGFzcw"
        payload = f"{core}/?remarks={quote(remarks)}"
        encoded = base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii").rstrip("=")
        uri = f"ssr://{encoded}"
        self.assertEqual(extract_node_name(uri), "SG-SSR")


class ManagerFlowTests(unittest.TestCase):
    def test_default_profile_filters_and_reencodes_as_base64(self) -> None:
        raw_lines = "\n".join(
            [
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#HK-Fast",
                "trojan://password@example.net:443?security=tls#US-Beta",
            ]
        )
        payloads = {
            "https://one.example/sub": base64.b64encode(raw_lines.encode("utf-8")),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            manager.add_subscription("", "https://one.example/sub")
            manager.update_settings("beta")

            default_profile_id = manager.get_default_profile()["id"]
            merged_plain = manager.build_merged_subscription(default_profile_id, "plain")
            merged_base64 = manager.build_merged_subscription(default_profile_id, "base64")

        self.assertIn("HK-Fast", merged_plain)
        self.assertNotIn("US-Beta", merged_plain)
        self.assertEqual(base64.b64decode(merged_base64).decode("utf-8"), merged_plain)

    def test_selected_profile_uses_only_selected_sources(self) -> None:
        payloads = {
            "https://one.example/sub": base64.b64encode(
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#HK-Only".encode("utf-8")
            ),
            "https://two.example/sub": base64.b64encode(
                "trojan://password@example.net:443?security=tls#US-Only".encode("utf-8")
            ),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            first = manager.add_subscription("one", "https://one.example/sub")
            manager.add_subscription("two", "https://two.example/sub")
            profile = manager.add_profile(
                name="Phone",
                mode="selected",
                subscription_ids=[first["id"]],
            )

            merged_plain = manager.build_merged_subscription(profile["id"], "plain")

        self.assertIn("HK-Only", merged_plain)
        self.assertNotIn("US-Only", merged_plain)

    def test_selected_profile_respects_source_order(self) -> None:
        payloads = {
            "https://one.example/sub": base64.b64encode(
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#First-HK".encode("utf-8")
            ),
            "https://two.example/sub": base64.b64encode(
                "trojan://password@example.net:443?security=tls#Second-US".encode("utf-8")
            ),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            first = manager.add_subscription("one", "https://one.example/sub")
            second = manager.add_subscription("two", "https://two.example/sub")
            profile = manager.add_profile(
                name="Ordered",
                mode="selected",
                subscription_ids=[second["id"], first["id"]],
            )

            merged_plain = manager.build_merged_subscription(profile["id"], "plain")
            ordered_profile = manager.get_profile(profile["id"])

        self.assertEqual(merged_plain.splitlines()[0], "trojan://password@example.net:443?security=tls#Second-US")
        self.assertEqual(merged_plain.splitlines()[1], "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#First-HK")
        self.assertEqual(ordered_profile["selected_subscription_ids"], [second["id"], first["id"]])
        self.assertEqual(ordered_profile["priority_source_count"], 2)

    def test_all_mode_profile_can_prioritize_sources(self) -> None:
        payloads = {
            "https://one.example/sub": base64.b64encode(
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#Alpha-HK".encode("utf-8")
            ),
            "https://two.example/sub": base64.b64encode(
                "trojan://password@example.net:443?security=tls#Beta-US".encode("utf-8")
            ),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            first = manager.add_subscription("one", "https://one.example/sub")
            second = manager.add_subscription("two", "https://two.example/sub")
            profile = manager.add_profile(
                name="All Ordered",
                mode="all",
                subscription_ids=[second["id"]],
            )

            merged_plain = manager.build_merged_subscription(profile["id"], "plain")
            updated_profile = manager.get_profile(profile["id"])

        self.assertEqual(merged_plain.splitlines()[0], "trojan://password@example.net:443?security=tls#Beta-US")
        self.assertEqual(merged_plain.splitlines()[1], "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#Alpha-HK")
        self.assertEqual(updated_profile["selected_subscription_ids"], [second["id"]])
        self.assertEqual(updated_profile["priority_source_count"], 1)

    def test_preview_nodes_supports_search_protocol_and_limit(self) -> None:
        payloads = {
            "https://one.example/sub": base64.b64encode(
                "\n".join(
                    [
                        "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#JP-Alpha",
                        "trojan://password@example.net:443?security=tls#JP-Beta",
                    ]
                ).encode("utf-8")
            ),
            "https://two.example/sub": base64.b64encode(
                (
                    "vless://12345678-1234-1234-1234-123456789012@example.org:443"
                    "?encryption=none&security=tls#US-Gamma"
                ).encode("utf-8")
            ),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            first = manager.add_subscription("one", "https://one.example/sub")
            second = manager.add_subscription("two", "https://two.example/sub")
            profile = manager.add_profile(
                name="Searchable",
                mode="selected",
                subscription_ids=[first["id"], second["id"]],
            )

            preview = manager.preview_profile_nodes(profile["id"], search="jp", limit=1)
            ss_only = manager.preview_profile_nodes(profile["id"], protocol="ss")

        self.assertEqual(preview["stats"]["total_nodes"], 3)
        self.assertEqual(preview["stats"]["matched_nodes"], 2)
        self.assertEqual(preview["stats"]["returned_nodes"], 1)
        self.assertTrue(preview["stats"]["truncated"])
        self.assertEqual(preview["items"][0]["name"], "JP-Alpha")
        self.assertEqual(ss_only["stats"]["matched_nodes"], 1)
        self.assertEqual(ss_only["items"][0]["protocol"], "ss")

    def test_json_export_returns_profile_metadata_and_filtered_nodes(self) -> None:
        raw_lines = "\n".join(
            [
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#HK-Fast",
                "trojan://password@example.net:443?security=tls#US-Beta",
            ]
        )
        payloads = {
            "https://one.example/sub": base64.b64encode(raw_lines.encode("utf-8")),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            manager.add_subscription("", "https://one.example/sub")
            manager.update_settings("beta")

            default_profile_id = manager.get_default_profile()["id"]
            exported = json.loads(manager.build_merged_subscription(default_profile_id, "json"))

        self.assertEqual(exported["profile"]["id"], default_profile_id)
        self.assertEqual(exported["profile"]["mode"], "all")
        self.assertEqual(exported["count"], 1)
        self.assertEqual(exported["nodes"][0]["name"], "HK-Fast")
        self.assertEqual(exported["nodes"][0]["protocol"], "ss")
        self.assertIn("generated_at", exported)

    def test_manual_subscription_can_be_previewed_and_merged(self) -> None:
        manual_nodes = "\n".join(
            [
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#Self-SS",
                (
                    "vless://12345678-1234-1234-1234-123456789012@example.org:443"
                    "?encryption=none&security=tls&type=ws&host=cdn.example.org&path=%2Fws#Self-VLESS"
                ),
            ]
        )
        payloads = {
            "https://remote.example/sub": base64.b64encode(
                "trojan://password@example.net:443?security=tls#Remote-Trojan".encode("utf-8")
            ),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            preview = manager.preview_subscription_input(source_type="manual", manual_content=manual_nodes)
            manual = manager.add_subscription(
                "自建节点",
                "",
                source_type="manual",
                manual_content=manual_nodes,
            )
            remote = manager.add_subscription("远程机场", "https://remote.example/sub")
            profile = manager.add_profile(
                name="Hybrid",
                mode="selected",
                subscription_ids=[manual["id"], remote["id"]],
            )
            merged_plain = manager.build_merged_subscription(profile["id"], "plain")

        self.assertEqual(preview["source_type"], "manual")
        self.assertEqual(preview["stats"]["total_nodes"], 2)
        self.assertIn("manual_text", manual["source_format"])
        self.assertTrue(manual["is_manual"])
        self.assertIsNone(manual["next_refresh_at"])
        self.assertIn("Self-SS", merged_plain)
        self.assertIn("Self-VLESS", merged_plain)
        self.assertIn("Remote-Trojan", merged_plain)

    def test_clash_surge_and_singbox_exports_are_generated(self) -> None:
        raw_lines = "\n".join(
            [
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#HK-SS",
                "trojan://password@example.net:443?security=tls&sni=example.net#US-Trojan",
                (
                    "vless://12345678-1234-1234-1234-123456789012@example.org:443"
                    "?encryption=none&security=tls&type=ws&host=cdn.example.org&path=%2Fws#JP-VLESS"
                ),
            ]
        )
        payloads = {
            "https://one.example/sub": base64.b64encode(raw_lines.encode("utf-8")),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            manager.add_subscription("", "https://one.example/sub")
            profile_id = manager.get_default_profile()["id"]
            clash = manager.build_merged_subscription(profile_id, "clash")
            surge = manager.build_merged_subscription(profile_id, "surge")
            singbox = json.loads(manager.build_merged_subscription(profile_id, "singbox"))

        self.assertIn("proxies:", clash)
        self.assertIn("type: ss", clash)
        self.assertIn("type: vless", clash)
        self.assertIn("[Proxy]", surge)
        self.assertIn("HK-SS = ss", surge)
        self.assertIn("US-Trojan = trojan", surge)
        self.assertIn("JP-VLESS", clash)
        self.assertEqual({item["type"] for item in singbox["outbounds"]}, {"shadowsocks", "trojan", "vless"})

    def test_failed_refresh_keeps_previous_nodes(self) -> None:
        initial_payloads = {
            "https://stable.example/sub": base64.b64encode(
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#Stable".encode("utf-8")
            )
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", initial_payloads)
            manager.create_initial_user("admin", "password123")
            manager.add_subscription("", "https://stable.example/sub")

            manager.payloads["https://stable.example/sub"] = b"not-a-subscription"
            manager.refresh_all()

            profile_id = manager.get_default_profile()["id"]
            filtered = manager.get_nodes_for_profile(profile_id)

        self.assertEqual([node.name for node in filtered], ["Stable"])

    def test_refresh_and_profile_access_counts_are_tracked(self) -> None:
        payloads = {
            "https://count.example/sub": base64.b64encode(
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#Counted".encode("utf-8")
            )
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            subscription = manager.add_subscription("", "https://count.example/sub")
            profile = manager.add_profile(
                name="Phone",
                mode="selected",
                subscription_ids=[subscription["id"]],
            )

            manager.refresh_subscription(subscription["id"])
            manager.get_public_subscription(profile["token"])
            plain_export = manager.get_public_subscription(profile["token"], format_name="plain")

            updated_subscription = manager.get_subscription(subscription["id"])
            updated_profile = manager.get_profile(profile["id"], "http://127.0.0.1:8787")
            dashboard = manager.get_dashboard_state("http://127.0.0.1:8787")
            dashboard_profile = next(item for item in dashboard["profiles"] if item["id"] == profile["id"])

        self.assertIn("Counted", plain_export)
        self.assertEqual(updated_subscription["refresh_count"], 2)
        self.assertEqual(updated_profile["source_refresh_count"], 2)
        self.assertEqual(updated_profile["access_count"], 2)
        self.assertEqual(updated_profile["access_count_24h"], 2)
        self.assertEqual(updated_profile["access_count_7d"], 2)
        self.assertEqual(dashboard_profile["source_refresh_count"], 2)
        self.assertEqual(dashboard_profile["access_count"], 2)
        self.assertEqual(dashboard_profile["access_count_24h"], 2)
        self.assertEqual(dashboard_profile["access_count_7d"], 2)
        self.assertEqual(dashboard["stats"]["subscription_refreshes"], 2)
        self.assertEqual(dashboard["stats"]["subscription_refreshes_24h"], 2)
        self.assertEqual(dashboard["stats"]["subscription_refreshes_7d"], 2)
        self.assertEqual(dashboard["stats"]["profile_accesses"], 2)
        self.assertEqual(dashboard["stats"]["profile_accesses_24h"], 2)
        self.assertEqual(dashboard["stats"]["profile_accesses_7d"], 2)

    def test_head_subscription_request_is_counted(self) -> None:
        payloads = {
            "https://count-head.example/sub": base64.b64encode(
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#Head-Counted".encode("utf-8")
            )
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            subscription = manager.add_subscription("", "https://count-head.example/sub")
            profile = manager.add_profile(
                name="Head Access",
                mode="selected",
                subscription_ids=[subscription["id"]],
            )

            server = ThreadingHTTPServer(("127.0.0.1", 0), PanelHandler)
            server.manager = manager  # type: ignore[attr-defined]
            server.static_dir = Path(__file__).resolve().parents[1] / "static"  # type: ignore[attr-defined]
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                request = Request(
                    f"http://127.0.0.1:{server.server_port}/subscribe/{profile['token']}",
                    method="HEAD",
                )
                with urlopen(request, timeout=5) as response:
                    self.assertEqual(response.status, 200)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

            updated_profile = manager.get_profile(profile["id"])
            dashboard = manager.get_dashboard_state(f"http://127.0.0.1:{server.server_port}")

        self.assertEqual(updated_profile["access_count"], 1)
        self.assertEqual(updated_profile["access_count_24h"], 1)
        self.assertEqual(dashboard["stats"]["profile_accesses"], 1)

    def test_subscription_update_supports_group_and_expiry(self) -> None:
        payloads = {
            "https://one.example/sub": base64.b64encode(
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#Stable".encode("utf-8")
            )
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            group = manager.add_group("高倍率", "主力机场", "#d86b34")
            subscription = manager.add_subscription("", "https://one.example/sub")
            updated = manager.update_subscription(
                subscription["id"],
                name="主力一号",
                url="https://one.example/sub",
                group_id=group["id"],
                expires_at="2026-12-31T10:00:00Z",
                refresh_interval_hours=12,
            )

        self.assertEqual(updated["name"], "主力一号")
        self.assertEqual(updated["group_id"], group["id"])
        self.assertEqual(updated["refresh_interval_hours"], 12)
        self.assertEqual(updated["expires_at"], "2026-12-31T10:00:00+00:00")

    def test_refresh_logs_capture_success_and_error_details(self) -> None:
        payloads = {
            "https://stable.example/sub": base64.b64encode(
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#Stable".encode("utf-8")
            )
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            subscription = manager.add_subscription("", "https://stable.example/sub")
            manager.payloads["https://stable.example/sub"] = b"broken-payload"
            manager.refresh_subscription(subscription["id"])
            logs = manager.list_subscription_refresh_logs(subscription["id"], limit=10)

        self.assertEqual(len(logs), 2)
        self.assertEqual(logs[0]["status"], "error")
        self.assertEqual(logs[1]["status"], "ok")
        self.assertEqual(logs[1]["trigger"], "save")
        self.assertGreaterEqual(logs[1]["node_count_after"], 1)
        self.assertTrue(logs[0]["error_message"])

    def test_bulk_import_subscriptions_creates_valid_entries_and_reports_errors(self) -> None:
        payloads = {
            "https://one.example/sub": base64.b64encode(
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#One".encode("utf-8")
            ),
            "https://two.example/sub": base64.b64encode(
                "trojan://password@example.net:443?security=tls#Two".encode("utf-8")
            ),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            result = manager.bulk_import_subscriptions(
                "\n".join(
                    [
                        "机场一,https://one.example/sub",
                        "https://two.example/sub",
                        "not-a-url",
                    ]
                )
            )
            subscriptions = manager.list_subscriptions()

        self.assertEqual(result["created_count"], 2)
        self.assertEqual(result["error_count"], 1)
        self.assertEqual(len(subscriptions), 2)
        self.assertTrue(any(item["name"] == "机场一" for item in subscriptions))
        self.assertTrue(any("http://" in item["error"] or "https://" in item["error"] for item in result["errors"]))

    def test_clone_profile_preserves_sources_and_rules(self) -> None:
        payloads = {
            "https://one.example/sub": base64.b64encode(
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#One".encode("utf-8")
            )
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            subscription = manager.add_subscription("", "https://one.example/sub")
            profile = manager.add_profile(
                name="Phone",
                description="主力设备",
                mode="selected",
                subscription_ids=[subscription["id"]],
                exclude_keywords="beta",
            )
            clone = manager.clone_profile(profile["id"])

        self.assertNotEqual(profile["id"], clone["id"])
        self.assertEqual(clone["selected_subscription_ids"], [subscription["id"]])
        self.assertEqual(clone["exclude_keywords"], "beta")
        self.assertIn("副本", clone["name"])

    def test_cleanup_settings_disable_expired_and_pause_failures(self) -> None:
        payloads = {
            "https://ok.example/sub": base64.b64encode(
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#Healthy".encode("utf-8")
            )
        }
        expired_at = (datetime.now(UTC) - timedelta(days=1)).isoformat()

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            expired = manager.add_subscription("到期机场", "https://ok.example/sub", expires_at=expired_at)
            failing = manager.add_subscription("故障机场", "https://down.example/sub", refresh_now=False)
            for _ in range(3):
                manager.refresh_subscription(failing["id"])
            cleanup = manager.update_cleanup_settings(
                auto_disable_expired=True,
                pause_failures_threshold=3,
            )
            actions = manager.apply_automatic_maintenance()
            expired_state = manager.get_subscription(expired["id"])
            failing_state = manager.get_subscription(failing["id"])

        self.assertTrue(cleanup["auto_disable_expired"])
        self.assertEqual(cleanup["pause_failures_threshold"], 3)
        self.assertEqual(actions["disabled_expired"], 1)
        self.assertEqual(actions["paused_failures"], 1)
        self.assertFalse(expired_state["enabled"])
        self.assertFalse(failing_state["enabled"])

    def test_advanced_filter_rules_support_protocol_filter_rename_and_dedup(self) -> None:
        manual_nodes = "\n".join(
            [
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#香港-A",
                "ss://YWVzLTI1Ni1nY206cGFzczJAZXhhbXBsZS5jb206NDQz#香港-A",
                "trojan://password@example.net:443?security=tls#日本-B",
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", {})
            manager.create_initial_user("admin", "password123")
            manager.add_subscription("本地", "", source_type="manual", manual_content=manual_nodes)
            manager.update_settings(
                "",
                exclude_protocols="trojan",
                dedup_strategy="name",
                rename_rules="香港 => HK",
            )
            profile_id = manager.get_default_profile()["id"]
            preview = manager.preview_profile_nodes(profile_id)
            exported_plain = manager.build_merged_subscription(profile_id, "plain")

        self.assertEqual(preview["stats"]["total_nodes"], 1)
        self.assertEqual(preview["items"][0]["name"], "HK-A")
        self.assertNotIn("日本-B", exported_plain)
        self.assertIn("#HK-A", exported_plain)

    def test_backup_restore_roundtrip_preserves_settings_and_profiles(self) -> None:
        raw_lines = "\n".join(
            [
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#HK-Fast",
                "trojan://password@example.net:443?security=tls#US-Beta",
            ]
        )
        payloads = {
            "https://one.example/sub": base64.b64encode(raw_lines.encode("utf-8")),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            source_manager = FakeSubscriptionManager(Path(temp_dir) / "source.db", payloads)
            source_manager.create_initial_user("admin", "password123")
            group = source_manager.add_group("高倍率", "主力机场", "#d86b34")
            subscription = source_manager.add_subscription(
                "主力一号",
                "https://one.example/sub",
                group_id=group["id"],
                expires_at="2026-12-31T10:00:00Z",
            )
            profile = source_manager.add_profile(
                name="Phone",
                mode="selected",
                subscription_ids=[subscription["id"]],
            )
            source_manager.refresh_subscription(subscription["id"])
            source_manager.get_public_subscription(profile["token"])
            source_manager.update_settings("beta")
            source_manager.update_panel_port(9988)

            backup = source_manager.export_backup()

            restored_manager = FakeSubscriptionManager(Path(temp_dir) / "restored.db", {})
            result = restored_manager.restore_backup(backup)
            restored_profile = restored_manager.get_profile(profile["id"], "http://127.0.0.1:8787")
            restored_subscription = restored_manager.get_subscription(subscription["id"])
            merged_plain = restored_manager.build_merged_subscription(profile["id"], "plain")
            restored_user = restored_manager.authenticate_user("admin", "password123")
            restored_port = restored_manager.get_panel_port()

        self.assertNotIn("session_secret", backup["settings"])
        self.assertEqual(result["counts"]["users"], 1)
        self.assertEqual(result["counts"]["groups"], 1)
        self.assertEqual(result["counts"]["subscriptions"], 1)
        self.assertEqual(result["counts"]["profiles"], 2)
        self.assertEqual(restored_profile["selected_subscription_ids"], [subscription["id"]])
        self.assertTrue(restored_profile["json_export_url"].endswith("?format=json"))
        self.assertEqual(restored_profile["access_count"], 1)
        self.assertEqual(restored_profile["source_refresh_count"], 2)
        self.assertEqual(restored_subscription["refresh_count"], 2)
        self.assertEqual(restored_port, 9988)
        self.assertIsNotNone(restored_user)
        self.assertIn("HK-Fast", merged_plain)
        self.assertNotIn("US-Beta", merged_plain)

    def test_backup_preview_reports_counts_and_warnings(self) -> None:
        manual_nodes = "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#Self-Only"

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", {})
            manager.create_initial_user("admin", "password123")
            manager.add_subscription("本地", "", source_type="manual", manual_content=manual_nodes)
            backup = manager.export_backup()
            preview = manager.preview_backup_restore(backup)

        self.assertTrue(preview["valid"])
        self.assertEqual(preview["backup_counts"]["subscriptions"], 1)
        self.assertEqual(preview["backup_counts"]["manual_subscriptions"], 1)
        self.assertTrue(any("重新登录" in item for item in preview["warnings"]))

    def test_health_alerts_include_expired_and_failing_subscriptions(self) -> None:
        payloads = {
            "https://ok.example/sub": base64.b64encode(
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#Healthy".encode("utf-8")
            ),
        }
        expired_at = (datetime.now(UTC) - timedelta(days=1)).isoformat()

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            expired = manager.add_subscription(
                "到期机场",
                "https://ok.example/sub",
                expires_at=expired_at,
            )
            failing = manager.add_subscription("故障机场", "https://down.example/sub", refresh_now=False)
            for _ in range(3):
                manager.refresh_subscription(failing["id"])

            alerts = manager.get_health_alerts()
            dashboard = manager.get_dashboard_state("http://127.0.0.1:8787")
            failing_state = manager.get_subscription(failing["id"])

        alert_types = {(item["type"], item["subscription_id"]) for item in alerts}
        failure_alert = next(item for item in alerts if item["type"] == "refresh_failures")

        self.assertIn(("expired", expired["id"]), alert_types)
        self.assertIn(("refresh_failures", failing["id"]), alert_types)
        self.assertEqual(failing_state["consecutive_failures"], 3)
        self.assertEqual(failure_alert["severity"], "warning")
        self.assertEqual(dashboard["stats"]["alerts"], len(alerts))

    def test_notification_dispatch_deduplicates_by_cooldown(self) -> None:
        payloads = {
            "https://ok.example/sub": base64.b64encode(
                "ss://YWVzLTI1Ni1nY206cGFzc0BleGFtcGxlLmNvbTo0NDM=#Healthy".encode("utf-8")
            ),
        }
        expired_at = (datetime.now(UTC) - timedelta(days=1)).isoformat()

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", payloads)
            manager.create_initial_user("admin", "password123")
            manager.add_subscription("到期机场", "https://ok.example/sub", expires_at=expired_at)
            manager.update_notification_settings(
                telegram_bot_token="123456:test",
                telegram_chat_id="10001",
                webhook_url="https://hook.example/notify",
                min_severity="warning",
                cooldown_minutes=360,
            )
            first = manager.dispatch_health_notifications()
            second = manager.dispatch_health_notifications()
            forced = manager.dispatch_health_notifications(force=True)

        self.assertEqual(first["sent"], 1)
        self.assertEqual(second["sent"], 0)
        self.assertEqual(forced["sent"], 1)
        self.assertEqual(len(manager.sent_telegram_messages), 2)
        self.assertEqual(len(manager.sent_webhook_payloads), 2)

    def test_account_and_panel_settings_can_be_updated(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", {})
            user = manager.create_initial_user("admin", "password123")

            updated_user = manager.update_current_user_credentials(
                user["id"],
                username="operator",
                current_password="password123",
                new_password="new-password-456",
            )
            port = manager.update_panel_port(9988)
            reset_user = manager.reset_primary_user(username="recovery")

        self.assertEqual(updated_user["username"], "operator")
        self.assertEqual(port, 9988)
        self.assertEqual(reset_user["username"], "recovery")
        self.assertGreaterEqual(len(reset_user["password"]), 8)

    def test_complete_initial_setup_and_schema_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FakeSubscriptionManager(Path(temp_dir) / "panel.db", {})
            result = manager.complete_initial_setup(
                username="admin",
                password="password123",
                panel_port=9988,
                theme="industrial-light",
                exclude_keywords="beta",
                exclude_protocols="ssr",
                dedup_strategy="name",
                rename_rules="香港 => HK",
                auto_disable_expired=True,
                pause_failures_threshold=4,
            )
            schema = manager.get_schema_status()
            filters = manager.get_advanced_filter_settings()
            cleanup = manager.get_cleanup_settings()

        self.assertEqual(result["user"]["username"], "admin")
        self.assertEqual(result["panel_port"], 9988)
        self.assertEqual(result["theme"], "industrial-light")
        self.assertTrue(schema["up_to_date"])
        self.assertEqual(filters["exclude_keywords"], "beta")
        self.assertEqual(filters["exclude_protocols"], "ssr")
        self.assertEqual(filters["dedup_strategy"], "name")
        self.assertEqual(filters["rename_rules"], "香港 => HK")
        self.assertTrue(cleanup["auto_disable_expired"])
        self.assertEqual(cleanup["pause_failures_threshold"], 4)


if __name__ == "__main__":
    unittest.main()
