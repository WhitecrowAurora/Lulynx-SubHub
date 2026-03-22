from __future__ import annotations

import argparse
import base64
import hmac
import json
import mimetypes
import threading
import time
from hashlib import sha256
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from manager import SubscriptionManager

SESSION_COOKIE_NAME = "lulynx_subhub_session"
SESSION_TTL_SECONDS = 60 * 60 * 24 * 7


class RefreshScheduler(threading.Thread):
    def __init__(self, manager: SubscriptionManager, interval_seconds: int = 30) -> None:
        super().__init__(daemon=True)
        self.manager = manager
        self.interval_seconds = interval_seconds
        self.stop_event = threading.Event()

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.manager.apply_automatic_maintenance()
                self.manager.refresh_due_subscriptions()
                self.manager.dispatch_health_notifications()
            except Exception as exc:
                print(f"[scheduler] refresh loop failed: {exc}")
            self.stop_event.wait(self.interval_seconds)

    def stop(self) -> None:
        self.stop_event.set()


class PanelHandler(BaseHTTPRequestHandler):
    server_version = "LulynxSubHub/1.0"

    @property
    def manager(self) -> SubscriptionManager:
        return self.server.manager  # type: ignore[attr-defined]

    @property
    def static_dir(self) -> Path:
        return self.server.static_dir  # type: ignore[attr-defined]

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/":
            self._handle_root()
            return
        if path == "/login":
            self._handle_login_page()
            return
        if path == "/setup":
            self._handle_setup_page()
            return
        if path.startswith("/static/"):
            self._serve_static(path)
            return
        if path == "/api/dashboard":
            self._handle_dashboard_api()
            return
        if path == "/api/backup":
            self._handle_backup_export()
            return
        if path.startswith("/api/subscriptions/") and path.endswith("/logs"):
            self._handle_subscription_logs(path, parsed.query)
            return
        if path == "/api/nodes":
            self._handle_node_preview(parsed.query)
            return
        if path == "/healthz":
            self._send_json({"status": "ok"})
            return
        if path.startswith("/subscribe/"):
            self._handle_subscription_export(path, parsed.query)
            return

        self._send_error_json(HTTPStatus.NOT_FOUND, "Not found.")

    def do_HEAD(self) -> None:
        self.do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/setup":
            self._handle_setup_api()
            return
        if path == "/api/login":
            self._handle_login_api()
            return
        if path == "/api/logout":
            self._handle_logout_api()
            return
        if path == "/api/account":
            self._handle_update_account()
            return
        if path == "/api/subscriptions":
            self._handle_create_subscription()
            return
        if path == "/api/subscriptions/preview":
            self._handle_preview_subscription()
            return
        if path == "/api/subscriptions/refresh-all":
            self._handle_refresh_all()
            return
        if path == "/api/subscriptions/import":
            self._handle_bulk_import_subscriptions()
            return
        if path == "/api/groups":
            self._handle_create_group()
            return
        if path == "/api/profiles":
            self._handle_create_profile()
            return
        if path == "/api/settings":
            self._handle_update_settings()
            return
        if path == "/api/settings/notifications":
            self._handle_update_notification_settings()
            return
        if path == "/api/settings/theme":
            self._handle_update_theme_settings()
            return
        if path == "/api/settings/cleanup":
            self._handle_update_cleanup_settings()
            return
        if path == "/api/settings/panel":
            self._handle_update_panel_settings()
            return
        if path == "/api/notifications/test":
            self._handle_test_notification()
            return
        if path == "/api/restore/preview":
            self._handle_backup_preview()
            return
        if path == "/api/restore":
            self._handle_backup_restore()
            return
        if path.startswith("/api/subscriptions/"):
            self._handle_subscription_action(path)
            return
        if path.startswith("/api/groups/"):
            self._handle_group_action(path)
            return
        if path.startswith("/api/profiles/"):
            self._handle_profile_action(path)
            return

        self._send_error_json(HTTPStatus.NOT_FOUND, "Not found.")

    def _handle_root(self) -> None:
        if self.manager.needs_setup():
            self._redirect("/setup")
            return
        if not self._current_user():
            self._redirect("/login")
            return
        self._serve_static("/static/dashboard.html")

    def _handle_login_page(self) -> None:
        if self.manager.needs_setup():
            self._redirect("/setup")
            return
        if self._current_user():
            self._redirect("/")
            return
        self._serve_static("/static/login.html")

    def _handle_setup_page(self) -> None:
        if self.manager.needs_setup():
            self._serve_static("/static/setup.html")
            return
        if self._current_user():
            self._redirect("/")
            return
        self._redirect("/login")

    def _handle_dashboard_api(self) -> None:
        user = self._require_auth()
        if user is None:
            return

        payload = self.manager.get_dashboard_state(self._base_url())
        payload["current_user"] = {
            "id": user["id"],
            "username": user["username"],
        }
        self._send_json(payload)

    def _handle_backup_export(self) -> None:
        if self._require_auth() is None:
            return

        backup = self.manager.export_backup()
        content = json.dumps(backup, ensure_ascii=False, indent=2).encode("utf-8")
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Cache-Control": "no-store",
            "Content-Disposition": 'attachment; filename="lulynx-subhub-backup.json"',
        }
        self._send_response(HTTPStatus.OK, content, headers)

    def _handle_node_preview(self, query: str) -> None:
        if self._require_auth() is None:
            return

        params = parse_qs(query, keep_blank_values=True)
        raw_profile_id = params.get("profile_id", [""])[0]
        raw_search = params.get("search", [""])[0]
        raw_protocol = params.get("protocol", [""])[0]
        raw_limit = params.get("limit", ["200"])[0]

        try:
            profile_id = int(raw_profile_id) if raw_profile_id else self.manager.get_default_profile()["id"]
            preview = self.manager.preview_profile_nodes(
                profile_id,
                search=raw_search,
                protocol=raw_protocol,
                limit=int(raw_limit),
            )
        except KeyError as exc:
            self._send_error_json(HTTPStatus.NOT_FOUND, str(exc))
            return
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        self._send_json({"ok": True, "preview": preview})

    def _handle_subscription_logs(self, path: str, query: str) -> None:
        if self._require_auth() is None:
            return
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            self._send_error_json(HTTPStatus.NOT_FOUND, "Not found.")
            return
        _, _, raw_id, action = parts
        if action != "logs":
            self._send_error_json(HTTPStatus.NOT_FOUND, "Not found.")
            return
        try:
            subscription_id = int(raw_id)
        except ValueError:
            self._send_error_json(HTTPStatus.BAD_REQUEST, "Invalid subscription id.")
            return
        params = parse_qs(query, keep_blank_values=True)
        try:
            limit = int(params.get("limit", ["20"])[0] or "20")
            logs = self.manager.list_subscription_refresh_logs(subscription_id, limit=limit)
        except KeyError as exc:
            self._send_error_json(HTTPStatus.NOT_FOUND, str(exc))
            return
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return
        self._send_json({"ok": True, "logs": logs})

    def _handle_setup_api(self) -> None:
        if not self.manager.needs_setup():
            self._send_error_json(HTTPStatus.CONFLICT, "Setup is already complete.")
            return

        payload = self._read_json_or_error()
        if payload is None:
            return
        try:
            result = self.manager.complete_initial_setup(
                username=str(payload.get("username", "")).strip(),
                password=str(payload.get("password", "")),
                panel_port=int(payload.get("panel_port", 8787)),
                theme=str(payload.get("theme", "classic")),
                exclude_keywords=str(payload.get("exclude_keywords", "")),
                exclude_protocols=str(payload.get("exclude_protocols", "")),
                dedup_strategy=str(payload.get("dedup_strategy", "uri")),
                rename_rules=str(payload.get("rename_rules", "")),
                auto_disable_expired=bool(payload.get("auto_disable_expired")),
                pause_failures_threshold=int(payload.get("pause_failures_threshold", 0)),
            )
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        headers = {"Set-Cookie": self._build_session_cookie(result["user"]["id"])}
        self._send_json({"ok": True, **result}, headers=headers)

    def _handle_login_api(self) -> None:
        if self.manager.needs_setup():
            self._send_error_json(HTTPStatus.CONFLICT, "Please complete setup first.")
            return

        payload = self._read_json_or_error()
        if payload is None:
            return
        username = str(payload.get("username", "")).strip()
        password = str(payload.get("password", ""))
        user = self.manager.authenticate_user(username, password)
        if not user:
            self._send_error_json(HTTPStatus.UNAUTHORIZED, "Invalid username or password.")
            return

        headers = {"Set-Cookie": self._build_session_cookie(user["id"])}
        self._send_json({"ok": True, "user": {"id": user["id"], "username": user["username"]}}, headers=headers)

    def _handle_logout_api(self) -> None:
        headers = {"Set-Cookie": self._build_logout_cookie()}
        self._send_json({"ok": True}, headers=headers)

    def _handle_update_account(self) -> None:
        user = self._require_auth()
        if user is None:
            return

        payload = self._read_json_or_error()
        if payload is None:
            return

        try:
            updated_user = self.manager.update_current_user_credentials(
                user["id"],
                username=str(payload.get("username", "")),
                current_password=str(payload.get("current_password", "")),
                new_password=str(payload.get("new_password", "")),
            )
        except KeyError as exc:
            self._send_error_json(HTTPStatus.NOT_FOUND, str(exc))
            return
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        self._send_json({"ok": True, "user": updated_user})

    def _handle_create_subscription(self) -> None:
        if self._require_auth() is None:
            return

        payload = self._read_json_or_error()
        if payload is None:
            return
        name = str(payload.get("name", ""))
        url = str(payload.get("url", ""))
        source_type = str(payload.get("source_type", "remote"))
        manual_content = str(payload.get("manual_content", ""))
        group_id = payload.get("group_id")
        expires_at = payload.get("expires_at")

        try:
            refresh_interval_hours = int(payload.get("refresh_interval_hours", 24))
            subscription = self.manager.add_subscription(
                name=name,
                url=url,
                group_id=group_id,
                expires_at=expires_at,
                refresh_interval_hours=refresh_interval_hours,
                source_type=source_type,
                manual_content=manual_content,
            )
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return
        except Exception as exc:
            self._send_error_json(HTTPStatus.BAD_GATEWAY, str(exc))
            return

        self._send_json({"ok": True, "subscription": subscription})

    def _handle_preview_subscription(self) -> None:
        if self._require_auth() is None:
            return

        payload = self._read_json_or_error()
        if payload is None:
            return

        try:
            preview = self.manager.preview_subscription_input(
                source_type=str(payload.get("source_type", "remote")),
                url=str(payload.get("url", "")),
                manual_content=str(payload.get("manual_content", "")),
            )
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return
        except Exception as exc:
            self._send_error_json(HTTPStatus.BAD_GATEWAY, str(exc))
            return

        self._send_json({"ok": True, "preview": preview})

    def _handle_refresh_all(self) -> None:
        if self._require_auth() is None:
            return
        self.manager.refresh_all()
        self._send_json({"ok": True})

    def _handle_bulk_import_subscriptions(self) -> None:
        if self._require_auth() is None:
            return
        payload = self._read_json_or_error()
        if payload is None:
            return
        try:
            result = self.manager.bulk_import_subscriptions(
                str(payload.get("raw_text", "")),
                group_id=payload.get("group_id"),
                refresh_interval_hours=int(payload.get("refresh_interval_hours", 24)),
            )
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return
        self._send_json({"ok": True, "result": result})

    def _handle_create_group(self) -> None:
        if self._require_auth() is None:
            return

        payload = self._read_json_or_error()
        if payload is None:
            return

        try:
            group = self.manager.add_group(
                name=str(payload.get("name", "")),
                description=str(payload.get("description", "")),
                color=str(payload.get("color", "#0c8d8a")),
            )
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        self._send_json({"ok": True, "group": group})

    def _handle_create_profile(self) -> None:
        if self._require_auth() is None:
            return

        payload = self._read_json_or_error()
        if payload is None:
            return

        try:
            profile = self.manager.add_profile(
                name=str(payload.get("name", "")),
                description=str(payload.get("description", "")),
                mode=str(payload.get("mode", "selected")),
                subscription_ids=payload.get("subscription_ids", []),
                exclude_keywords=str(payload.get("exclude_keywords", "")),
                exclude_protocols=str(payload.get("exclude_protocols", "")),
            )
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        self._send_json({"ok": True, "profile": profile})

    def _handle_update_settings(self) -> None:
        if self._require_auth() is None:
            return

        payload = self._read_json_or_error()
        if payload is None:
            return
        exclude_keywords = str(payload.get("global_exclude_keywords", payload.get("exclude_keywords", "")))
        try:
            self.manager.update_settings(
                exclude_keywords,
                exclude_protocols=str(payload.get("exclude_protocols", "")),
                dedup_strategy=str(payload.get("dedup_strategy", "uri")),
                rename_rules=str(payload.get("rename_rules", "")),
            )
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return
        self._send_json({"ok": True})

    def _handle_update_notification_settings(self) -> None:
        if self._require_auth() is None:
            return

        payload = self._read_json_or_error()
        if payload is None:
            return

        try:
            settings = self.manager.update_notification_settings(
                telegram_bot_token=str(payload.get("telegram_bot_token", "")),
                telegram_chat_id=str(payload.get("telegram_chat_id", "")),
                webhook_url=str(payload.get("webhook_url", "")),
                min_severity=str(payload.get("min_severity", "warning")),
                cooldown_minutes=int(payload.get("cooldown_minutes", 360)),
            )
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        self._send_json({"ok": True, "notifications": settings})

    def _handle_update_theme_settings(self) -> None:
        if self._require_auth() is None:
            return
        payload = self._read_json_or_error()
        if payload is None:
            return
        theme = self.manager.update_default_theme(str(payload.get("theme", "classic")))
        self._send_json({"ok": True, "theme": theme})

    def _handle_update_cleanup_settings(self) -> None:
        if self._require_auth() is None:
            return
        payload = self._read_json_or_error()
        if payload is None:
            return
        try:
            settings = self.manager.update_cleanup_settings(
                auto_disable_expired=bool(payload.get("auto_disable_expired")),
                pause_failures_threshold=int(payload.get("pause_failures_threshold", 0)),
            )
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return
        self._send_json({"ok": True, "cleanup": settings})

    def _handle_update_panel_settings(self) -> None:
        if self._require_auth() is None:
            return

        payload = self._read_json_or_error()
        if payload is None:
            return

        try:
            panel_port = self.manager.update_panel_port(payload.get("panel_port", 8787))
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        self._send_json(
            {
                "ok": True,
                "panel_port": panel_port,
                "restart_required": True,
            }
        )

    def _handle_backup_restore(self) -> None:
        if self._require_auth() is None:
            return

        payload = self._read_json_or_error()
        if payload is None:
            return

        backup_payload = payload.get("backup", payload)
        try:
            result = self.manager.restore_backup(backup_payload)
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        self._send_json({"ok": True, "result": result})

    def _handle_backup_preview(self) -> None:
        if self._require_auth() is None:
            return

        payload = self._read_json_or_error()
        if payload is None:
            return

        backup_payload = payload.get("backup", payload)
        try:
            preview = self.manager.preview_backup_restore(backup_payload)
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        self._send_json({"ok": True, "preview": preview})

    def _handle_test_notification(self) -> None:
        if self._require_auth() is None:
            return

        payload = self._read_json_or_error()
        if payload is None:
            return

        try:
            result = self.manager.dispatch_health_notifications(
                force=True,
                test_message=str(payload.get("message", "这是来自 Lulynx SubHub 的测试通知。")),
            )
        except Exception as exc:
            self._send_error_json(HTTPStatus.BAD_GATEWAY, str(exc))
            return

        self._send_json({"ok": True, "result": result})

    def _handle_subscription_action(self, path: str) -> None:
        if self._require_auth() is None:
            return

        parts = path.strip("/").split("/")
        if len(parts) != 4:
            self._send_error_json(HTTPStatus.NOT_FOUND, "Not found.")
            return

        _, _, raw_id, action = parts
        try:
            subscription_id = int(raw_id)
        except ValueError:
            self._send_error_json(HTTPStatus.BAD_REQUEST, "Invalid subscription id.")
            return

        try:
            if action == "refresh":
                subscription = self.manager.refresh_subscription(subscription_id)
                self._send_json({"ok": True, "subscription": subscription})
                return
            if action == "update":
                payload = self._read_json_or_error()
                if payload is None:
                    return
                subscription = self.manager.update_subscription(
                    subscription_id,
                    name=str(payload.get("name", "")),
                    url=str(payload.get("url", "")),
                    group_id=payload.get("group_id"),
                    expires_at=payload.get("expires_at"),
                    refresh_interval_hours=int(payload.get("refresh_interval_hours", 24)),
                    source_type=str(payload.get("source_type", "remote")),
                    manual_content=str(payload.get("manual_content", "")),
                )
                self._send_json({"ok": True, "subscription": subscription})
                return
            if action == "delete":
                self.manager.delete_subscription(subscription_id)
                self._send_json({"ok": True})
                return
            if action == "enabled":
                payload = self._read_json_or_error()
                if payload is None:
                    return
                enabled = bool(payload.get("enabled"))
                subscription = self.manager.set_subscription_enabled(subscription_id, enabled)
                self._send_json({"ok": True, "subscription": subscription})
                return
        except KeyError as exc:
            self._send_error_json(HTTPStatus.NOT_FOUND, str(exc))
            return
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return
        except Exception as exc:
            self._send_error_json(HTTPStatus.BAD_GATEWAY, str(exc))
            return

        self._send_error_json(HTTPStatus.NOT_FOUND, "Not found.")

    def _handle_group_action(self, path: str) -> None:
        if self._require_auth() is None:
            return

        parts = path.strip("/").split("/")
        if len(parts) != 4:
            self._send_error_json(HTTPStatus.NOT_FOUND, "Not found.")
            return

        _, _, raw_id, action = parts
        try:
            group_id = int(raw_id)
        except ValueError:
            self._send_error_json(HTTPStatus.BAD_REQUEST, "Invalid group id.")
            return

        try:
            if action == "update":
                payload = self._read_json_or_error()
                if payload is None:
                    return
                group = self.manager.update_group(
                    group_id,
                    name=str(payload.get("name", "")),
                    description=str(payload.get("description", "")),
                    color=str(payload.get("color", "#0c8d8a")),
                )
                self._send_json({"ok": True, "group": group})
                return
            if action == "delete":
                self.manager.delete_group(group_id)
                self._send_json({"ok": True})
                return
        except KeyError as exc:
            self._send_error_json(HTTPStatus.NOT_FOUND, str(exc))
            return
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        self._send_error_json(HTTPStatus.NOT_FOUND, "Not found.")

    def _handle_profile_action(self, path: str) -> None:
        if self._require_auth() is None:
            return

        parts = path.strip("/").split("/")
        if len(parts) not in (4, 5):
            self._send_error_json(HTTPStatus.NOT_FOUND, "Not found.")
            return

        _, _, raw_id, action = parts[:4]
        try:
            profile_id = int(raw_id)
        except ValueError:
            self._send_error_json(HTTPStatus.BAD_REQUEST, "Invalid profile id.")
            return

        try:
            if action == "update":
                payload = self._read_json_or_error()
                if payload is None:
                    return
                profile = self.manager.update_profile(
                    profile_id,
                    name=str(payload.get("name", "")),
                    description=str(payload.get("description", "")),
                    mode=str(payload.get("mode", "selected")),
                    subscription_ids=payload.get("subscription_ids", []),
                    exclude_keywords=str(payload.get("exclude_keywords", "")),
                    exclude_protocols=str(payload.get("exclude_protocols", "")),
                )
                self._send_json({"ok": True, "profile": profile})
                return
            if action == "delete":
                self.manager.delete_profile(profile_id)
                self._send_json({"ok": True})
                return
            if action == "clone":
                payload = self._read_json_or_error()
                if payload is None:
                    return
                profile = self.manager.clone_profile(profile_id, name=str(payload.get("name", "")))
                self._send_json({"ok": True, "profile": profile})
                return
            if action == "token" and len(parts) == 5 and parts[4] == "regenerate":
                profile = self.manager.regenerate_profile_token(profile_id)
                profile["export_url"] = f"{self._base_url()}/subscribe/{profile['token']}"
                profile["plain_export_url"] = f"{self._base_url()}/subscribe/{profile['token']}?format=plain"
                profile["json_export_url"] = f"{self._base_url()}/subscribe/{profile['token']}?format=json"
                profile["clash_export_url"] = f"{self._base_url()}/subscribe/{profile['token']}?format=clash"
                profile["surge_export_url"] = f"{self._base_url()}/subscribe/{profile['token']}?format=surge"
                profile["singbox_export_url"] = f"{self._base_url()}/subscribe/{profile['token']}?format=singbox"
                self._send_json({"ok": True, "profile": profile})
                return
        except KeyError as exc:
            self._send_error_json(HTTPStatus.NOT_FOUND, str(exc))
            return
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        self._send_error_json(HTTPStatus.NOT_FOUND, "Not found.")

    def _handle_subscription_export(self, path: str, query: str) -> None:
        token = path.rsplit("/", 1)[-1]
        params = parse_qs(query, keep_blank_values=True)
        format_name = params.get("format", ["base64"])[0] or "base64"

        try:
            payload = self.manager.get_public_subscription(token, format_name=format_name)
        except PermissionError:
            self._send_error_json(HTTPStatus.FORBIDDEN, "Invalid subscription token.")
            return
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        content_type = {
            "json": "application/json; charset=utf-8",
            "singbox": "application/json; charset=utf-8",
            "clash": "text/yaml; charset=utf-8",
            "surge": "text/plain; charset=utf-8",
        }.get(format_name, "text/plain; charset=utf-8")
        filename = {
            "json": "merged-subscription.json",
            "singbox": "merged-subscription-singbox.json",
            "clash": "merged-subscription-clash.yaml",
            "surge": "merged-subscription-surge.conf",
            "plain": "merged-subscription.txt",
        }.get(format_name, "merged-subscription.txt")
        headers = {
            "Content-Type": content_type,
            "Cache-Control": "no-store",
            "Profile-Update-Interval": "24",
            "Content-Disposition": f'inline; filename="{filename}"',
        }
        self._send_response(HTTPStatus.OK, payload.encode("utf-8"), headers)

    def _require_auth(self) -> dict | None:
        if self.manager.needs_setup():
            self._send_error_json(HTTPStatus.CONFLICT, "Please complete setup first.")
            return None
        user = self._current_user()
        if user is None:
            self._send_error_json(HTTPStatus.UNAUTHORIZED, "Please log in.")
            return None
        return user

    def _current_user(self) -> dict | None:
        cookie_header = self.headers.get("Cookie")
        if not cookie_header:
            return None

        cookie = SimpleCookie()
        cookie.load(cookie_header)
        morsel = cookie.get(SESSION_COOKIE_NAME)
        if morsel is None:
            return None

        token = morsel.value
        user_id = self._verify_session_token(token)
        if user_id is None:
            return None
        return self.manager.get_user(user_id)

    def _base_url(self) -> str:
        host = self.headers.get("X-Forwarded-Host") or self.headers.get("Host") or "127.0.0.1"
        proto = self.headers.get("X-Forwarded-Proto") or "http"
        return f"{proto}://{host}"

    def _build_session_cookie(self, user_id: int) -> str:
        token = self._create_session_token(user_id)
        return (
            f"{SESSION_COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict; "
            f"Max-Age={SESSION_TTL_SECONDS}"
        )

    def _build_logout_cookie(self) -> str:
        return (
            f"{SESSION_COOKIE_NAME}=deleted; Path=/; HttpOnly; SameSite=Strict; "
            "Max-Age=0"
        )

    def _create_session_token(self, user_id: int) -> str:
        expires_at = int(time.time()) + SESSION_TTL_SECONDS
        payload = f"{user_id}:{expires_at}"
        signature = hmac.new(
            self.manager.get_session_secret().encode("utf-8"),
            payload.encode("utf-8"),
            sha256,
        ).hexdigest()
        token = base64.urlsafe_b64encode(f"{payload}:{signature}".encode("utf-8")).decode("ascii")
        return token.rstrip("=")

    def _verify_session_token(self, token: str) -> int | None:
        padding = "=" * (-len(token) % 4)
        try:
            decoded = base64.urlsafe_b64decode(token + padding).decode("utf-8")
            raw_user_id, raw_expiry, signature = decoded.split(":")
            payload = f"{raw_user_id}:{raw_expiry}"
        except Exception:
            return None

        expected = hmac.new(
            self.manager.get_session_secret().encode("utf-8"),
            payload.encode("utf-8"),
            sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, expected):
            return None
        if int(raw_expiry) < int(time.time()):
            return None
        try:
            return int(raw_user_id)
        except ValueError:
            return None

    def _serve_static(self, request_path: str) -> None:
        relative = request_path.removeprefix("/static/").lstrip("/")
        file_path = (self.static_dir / relative).resolve()
        if not str(file_path).startswith(str(self.static_dir.resolve())) or not file_path.exists():
            self._send_error_json(HTTPStatus.NOT_FOUND, "Static file not found.")
            return

        content = file_path.read_bytes()
        mime_type, _ = mimetypes.guess_type(file_path.name)
        headers = {
            "Content-Type": mime_type or "application/octet-stream",
            "Cache-Control": "no-store",
        }
        self._send_response(HTTPStatus.OK, content, headers)

    def _read_json(self) -> dict:
        raw_length = self.headers.get("Content-Length", "0")
        try:
            content_length = int(raw_length)
        except ValueError:
            content_length = 0
        body = self.rfile.read(content_length) if content_length else b"{}"
        if not body:
            return {}
        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError("Request body must be valid JSON.") from exc

    def _read_json_or_error(self) -> dict | None:
        try:
            return self._read_json()
        except ValueError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return None

    def _redirect(self, location: str) -> None:
        headers = {"Location": location}
        self._send_response(HTTPStatus.SEE_OTHER, b"", headers)

    def _send_json(self, payload: dict, status: HTTPStatus = HTTPStatus.OK, headers: dict[str, str] | None = None) -> None:
        content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        response_headers = {"Content-Type": "application/json; charset=utf-8"}
        if headers:
            response_headers.update(headers)
        self._send_response(status, content, response_headers)

    def _send_error_json(self, status: HTTPStatus, message: str) -> None:
        self._send_json({"ok": False, "error": message}, status=status)

    def _send_response(self, status: HTTPStatus, content: bytes, headers: dict[str, str]) -> None:
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(content)

    def log_message(self, format: str, *args: object) -> None:
        print(f"[http] {self.address_string()} - {format % args}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Lulynx SubHub server for merging proxy subscriptions.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host, defaults to 127.0.0.1")
    parser.add_argument("--port", default=None, type=int, help="Bind port. If omitted, use the saved panel port.")
    parser.add_argument(
        "--db",
        default=str(Path("data") / "subpanel.db"),
        help="Path to the SQLite database file.",
    )
    parser.add_argument(
        "--reset-admin",
        action="store_true",
        help="Reset the primary admin username/password from the backend.",
    )
    parser.add_argument(
        "--migrate-db",
        action="store_true",
        help="Apply pending schema migrations and print the migration result.",
    )
    parser.add_argument("--admin-username", default="", help="New admin username for reset mode.")
    parser.add_argument("--admin-password", default="", help="New admin password for reset mode.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    static_dir = Path(__file__).resolve().parent / "static"
    manager = SubscriptionManager(args.db)

    if args.reset_admin:
        result = manager.reset_primary_user(
            username=args.admin_username or None,
            password=args.admin_password or None,
        )
        print(f"Admin username: {result['username']}")
        print(f"Admin password: {result['password']}")
        return

    if args.migrate_db:
        result = manager.migrate_database()
        print(f"Schema version: {result['before']} -> {result['after']} (target {result['target_version']})")
        print("Migration completed." if result["changed"] else "Database is already up to date.")
        return

    port = args.port if args.port is not None else manager.get_panel_port()
    scheduler = RefreshScheduler(manager)
    scheduler.start()

    server = ThreadingHTTPServer((args.host, port), PanelHandler)
    server.manager = manager  # type: ignore[attr-defined]
    server.static_dir = static_dir  # type: ignore[attr-defined]

    print(f"Lulynx SubHub is listening at http://{args.host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        scheduler.stop()
        server.server_close()


if __name__ == "__main__":
    main()
