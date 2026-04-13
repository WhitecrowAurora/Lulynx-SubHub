from __future__ import annotations

import base64
import hashlib
import json
import re
import secrets
import sqlite3
import threading
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from exporters import SUPPORTED_EXPORT_FORMATS, build_export
from parsers import SUPPORTED_PROTOCOLS, NodeEntry, filter_nodes, parse_subscription_payload, split_keywords

CURRENT_SCHEMA_VERSION = 5


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).replace(microsecond=0).isoformat()


def normalize_datetime_input(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None
    value = raw_value.strip()
    if not value:
        return None
    value = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("Invalid datetime format.") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return to_iso(parsed)


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


class SubscriptionManager:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.refresh_lock = threading.Lock()
        self._init_db()
        self._ensure_runtime_settings()
        self._ensure_default_profile()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    @contextmanager
    def _database(self) -> sqlite3.Connection:
        connection = self._connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _table_columns(self, table_name: str) -> set[str]:
        with self._database() as connection:
            rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {row["name"] for row in rows}

    def _init_db(self) -> None:
        with self._database() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    password_salt TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS subscription_groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    description TEXT NOT NULL DEFAULT '',
                    color TEXT NOT NULL DEFAULT '#0c8d8a',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS subscriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    source_type TEXT NOT NULL DEFAULT 'remote',
                    manual_content TEXT NOT NULL DEFAULT '',
                    group_id INTEGER REFERENCES subscription_groups(id) ON DELETE SET NULL,
                    expires_at TEXT,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    refresh_interval_hours INTEGER NOT NULL DEFAULT 24,
                    last_status TEXT NOT NULL DEFAULT 'idle',
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    last_updated_at TEXT,
                    next_refresh_at TEXT,
                    node_count INTEGER NOT NULL DEFAULT 0,
                    refresh_count INTEGER NOT NULL DEFAULT 0,
                    source_format TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS nodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    subscription_id INTEGER NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
                    uri TEXT NOT NULL,
                    name TEXT NOT NULL,
                    protocol TEXT NOT NULL,
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    UNIQUE(subscription_id, uri)
                );

                CREATE TABLE IF NOT EXISTS merge_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    token TEXT NOT NULL UNIQUE,
                    exclude_keywords TEXT NOT NULL DEFAULT '',
                    exclude_protocols TEXT NOT NULL DEFAULT '',
                    include_all INTEGER NOT NULL DEFAULT 0,
                    access_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS merge_profile_sources (
                    profile_id INTEGER NOT NULL REFERENCES merge_profiles(id) ON DELETE CASCADE,
                    subscription_id INTEGER NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (profile_id, subscription_id)
                );

                CREATE TABLE IF NOT EXISTS alert_notifications (
                    alert_key TEXT PRIMARY KEY,
                    fingerprint TEXT NOT NULL,
                    last_sent_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS subscription_refresh_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    subscription_id INTEGER NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
                    trigger TEXT NOT NULL DEFAULT 'manual',
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    duration_ms INTEGER NOT NULL DEFAULT 0,
                    node_count_before INTEGER NOT NULL DEFAULT 0,
                    node_count_after INTEGER NOT NULL DEFAULT 0,
                    added_count INTEGER NOT NULL DEFAULT 0,
                    removed_count INTEGER NOT NULL DEFAULT 0,
                    source_format TEXT,
                    error_message TEXT,
                    added_sample_json TEXT NOT NULL DEFAULT '[]',
                    removed_sample_json TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_refresh_logs_subscription_created
                ON subscription_refresh_logs (subscription_id, created_at DESC);

                CREATE TABLE IF NOT EXISTS profile_access_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL REFERENCES merge_profiles(id) ON DELETE CASCADE,
                    format_name TEXT NOT NULL DEFAULT 'base64',
                    accessed_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_profile_access_logs_profile_accessed
                ON profile_access_logs (profile_id, accessed_at DESC);
                """
            )
        self._migrate_schema()

    def _migrate_schema(self) -> None:
        subscription_columns = self._table_columns("subscriptions")
        profile_columns = self._table_columns("merge_profiles")
        group_columns = self._table_columns("subscription_groups")
        profile_source_columns = self._table_columns("merge_profile_sources")
        node_columns = self._table_columns("nodes")

        with self._database() as connection:
            if "group_id" not in subscription_columns:
                connection.execute(
                    "ALTER TABLE subscriptions ADD COLUMN group_id INTEGER REFERENCES subscription_groups(id) ON DELETE SET NULL"
                )
            if "source_type" not in subscription_columns:
                connection.execute(
                    "ALTER TABLE subscriptions ADD COLUMN source_type TEXT NOT NULL DEFAULT 'remote'"
                )
            if "manual_content" not in subscription_columns:
                connection.execute(
                    "ALTER TABLE subscriptions ADD COLUMN manual_content TEXT NOT NULL DEFAULT ''"
                )
            if "expires_at" not in subscription_columns:
                connection.execute("ALTER TABLE subscriptions ADD COLUMN expires_at TEXT")
            if "consecutive_failures" not in subscription_columns:
                connection.execute(
                    "ALTER TABLE subscriptions ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0"
                )
            if "refresh_count" not in subscription_columns:
                connection.execute(
                    "ALTER TABLE subscriptions ADD COLUMN refresh_count INTEGER NOT NULL DEFAULT 0"
                )

            if "description" not in group_columns:
                connection.execute(
                    "ALTER TABLE subscription_groups ADD COLUMN description TEXT NOT NULL DEFAULT ''"
                )
            if "color" not in group_columns:
                connection.execute(
                    "ALTER TABLE subscription_groups ADD COLUMN color TEXT NOT NULL DEFAULT '#0c8d8a'"
                )
            if "updated_at" not in group_columns:
                connection.execute(
                    "ALTER TABLE subscription_groups ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''"
                )

            if "description" not in profile_columns:
                connection.execute(
                    "ALTER TABLE merge_profiles ADD COLUMN description TEXT NOT NULL DEFAULT ''"
                )
            if "exclude_keywords" not in profile_columns:
                connection.execute(
                    "ALTER TABLE merge_profiles ADD COLUMN exclude_keywords TEXT NOT NULL DEFAULT ''"
                )
            if "exclude_protocols" not in profile_columns:
                connection.execute(
                    "ALTER TABLE merge_profiles ADD COLUMN exclude_protocols TEXT NOT NULL DEFAULT ''"
                )
            if "include_all" not in profile_columns:
                connection.execute(
                    "ALTER TABLE merge_profiles ADD COLUMN include_all INTEGER NOT NULL DEFAULT 0"
                )
            if "access_count" not in profile_columns:
                connection.execute(
                    "ALTER TABLE merge_profiles ADD COLUMN access_count INTEGER NOT NULL DEFAULT 0"
                )
            if "updated_at" not in profile_columns:
                connection.execute(
                    "ALTER TABLE merge_profiles ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''"
                )
            if "sort_order" not in profile_source_columns:
                connection.execute(
                    "ALTER TABLE merge_profile_sources ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0"
                )
                rows = connection.execute(
                    """
                    SELECT profile_id, subscription_id
                    FROM merge_profile_sources
                    ORDER BY profile_id ASC, subscription_id ASC
                    """
                ).fetchall()
                sort_updates: list[tuple[int, int, int]] = []
                current_profile_id: int | None = None
                sort_order = 0
                for row in rows:
                    if row["profile_id"] != current_profile_id:
                        current_profile_id = int(row["profile_id"])
                        sort_order = 0
                    sort_updates.append((sort_order, int(row["profile_id"]), int(row["subscription_id"])))
                    sort_order += 1
                if sort_updates:
                    connection.executemany(
                        """
                        UPDATE merge_profile_sources
                        SET sort_order = ?
                        WHERE profile_id = ? AND subscription_id = ?
                        """,
                        sort_updates,
                    )
            if "sort_order" not in node_columns:
                connection.execute(
                    "ALTER TABLE nodes ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0"
                )
                rows = connection.execute(
                    """
                    SELECT id, subscription_id
                    FROM nodes
                    ORDER BY subscription_id ASC, id ASC
                    """
                ).fetchall()
                node_sort_updates: list[tuple[int, int]] = []
                current_subscription_id: int | None = None
                sort_order = 0
                for row in rows:
                    if row["subscription_id"] != current_subscription_id:
                        current_subscription_id = int(row["subscription_id"])
                        sort_order = 0
                    node_sort_updates.append((sort_order, int(row["id"])))
                    sort_order += 1
                if node_sort_updates:
                    connection.executemany(
                        """
                        UPDATE nodes
                        SET sort_order = ?
                        WHERE id = ?
                        """,
                        node_sort_updates,
                    )
        self.set_setting("schema_version", str(CURRENT_SCHEMA_VERSION))

    def _ensure_runtime_settings(self) -> None:
        self._ensure_setting("schema_version", str(CURRENT_SCHEMA_VERSION))
        self._ensure_setting("session_secret", secrets.token_urlsafe(32))
        self._ensure_setting("exclude_keywords", "")
        self._ensure_setting("exclude_protocols", "")
        self._ensure_setting("dedup_strategy", "uri")
        self._ensure_setting("rename_rules", "")
        self._ensure_setting("default_theme", "classic")
        self._ensure_setting("public_token", "")
        self._ensure_setting("panel_port", "8787")
        self._ensure_setting("cleanup_auto_disable_expired", "0")
        self._ensure_setting("cleanup_pause_failures_threshold", "0")
        self._ensure_setting("notifications_telegram_bot_token", "")
        self._ensure_setting("notifications_telegram_chat_id", "")
        self._ensure_setting("notifications_webhook_url", "")
        self._ensure_setting("notifications_min_severity", "warning")
        self._ensure_setting("notifications_cooldown_minutes", "360")

    def _ensure_default_profile(self) -> None:
        token = self.get_setting("public_token")
        if not token:
            token = secrets.token_urlsafe(24)
            self.set_setting("public_token", token)

        with self._database() as connection:
            existing = connection.execute(
                "SELECT id FROM merge_profiles ORDER BY id ASC LIMIT 1"
            ).fetchone()
            if existing is not None:
                return

            now = to_iso(utc_now())
            connection.execute(
                """
                INSERT INTO merge_profiles (
                    name, description, token, exclude_keywords, include_all, created_at, updated_at
                )
                VALUES (?, ?, ?, '', 1, ?, ?)
                """,
                ("默认主订阅", "合并全部启用中的机场订阅。", token, now, now),
            )

    def _ensure_setting(self, key: str, default_value: str) -> None:
        with self._database() as connection:
            existing = connection.execute(
                "SELECT value FROM settings WHERE key = ?",
                (key,),
            ).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO settings (key, value) VALUES (?, ?)",
                    (key, default_value),
                )

    def get_setting(self, key: str, default: str = "") -> str:
        with self._database() as connection:
            row = connection.execute(
                "SELECT value FROM settings WHERE key = ?",
                (key,),
            ).fetchone()
        return row["value"] if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self._database() as connection:
            connection.execute(
                """
                INSERT INTO settings (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def needs_setup(self) -> bool:
        with self._database() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM users").fetchone()
        return bool(row["count"] == 0)

    def get_schema_status(self) -> dict[str, Any]:
        raw_value = self.get_setting("schema_version", "0")
        try:
            current_version = int(raw_value)
        except ValueError:
            current_version = 0
        return {
            "current_version": current_version,
            "target_version": CURRENT_SCHEMA_VERSION,
            "up_to_date": current_version >= CURRENT_SCHEMA_VERSION,
        }

    def migrate_database(self) -> dict[str, Any]:
        before = self.get_schema_status()
        self._migrate_schema()
        self._ensure_runtime_settings()
        after = self.get_schema_status()
        return {
            "before": before["current_version"],
            "after": after["current_version"],
            "target_version": after["target_version"],
            "changed": before["current_version"] != after["current_version"],
        }

    def create_initial_user(self, username: str, password: str) -> dict[str, Any]:
        if not username.strip():
            raise ValueError("Username is required.")
        if len(password) < 8:
            raise ValueError("Password must be at least 8 characters.")
        if not self.needs_setup():
            raise ValueError("Setup has already been completed.")

        created_at = to_iso(utc_now())
        password_hash, password_salt = self._hash_password(password)
        with self._database() as connection:
            cursor = connection.execute(
                """
                INSERT INTO users (username, password_hash, password_salt, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (username.strip(), password_hash, password_salt, created_at),
            )
        return {"id": cursor.lastrowid, "username": username.strip(), "created_at": created_at}

    def complete_initial_setup(
        self,
        *,
        username: str,
        password: str,
        panel_port: int = 8787,
        theme: str = "classic",
        exclude_keywords: str = "",
        exclude_protocols: str = "",
        dedup_strategy: str = "uri",
        rename_rules: str = "",
        auto_disable_expired: bool = False,
        pause_failures_threshold: int = 0,
    ) -> dict[str, Any]:
        if not self.needs_setup():
            raise ValueError("Setup has already been completed.")
        normalized_username = username.strip()
        if not normalized_username:
            raise ValueError("Username is required.")
        if len(password) < 8:
            raise ValueError("Password must be at least 8 characters.")

        normalized_port = self.update_panel_port(panel_port)
        normalized_theme = self._normalize_theme_name(theme)
        advanced_filters = self.update_settings(
            exclude_keywords,
            exclude_protocols=exclude_protocols,
            dedup_strategy=dedup_strategy,
            rename_rules=rename_rules,
        )
        cleanup_settings = self.update_cleanup_settings(
            auto_disable_expired=auto_disable_expired,
            pause_failures_threshold=pause_failures_threshold,
        )
        self.update_default_theme(normalized_theme)
        user = self.create_initial_user(normalized_username, password)
        return {
            "user": user,
            "panel_port": normalized_port,
            "theme": normalized_theme,
            "filters": advanced_filters,
            "cleanup": cleanup_settings,
        }

    def authenticate_user(self, username: str, password: str) -> dict[str, Any] | None:
        with self._database() as connection:
            row = connection.execute(
                "SELECT * FROM users WHERE username = ?",
                (username.strip(),),
            ).fetchone()
        if row is None:
            return None

        expected_hash = row["password_hash"]
        salt = row["password_salt"]
        candidate_hash, _ = self._hash_password(password, salt)
        if not secrets.compare_digest(candidate_hash, expected_hash):
            return None
        return _row_to_dict(row)

    def get_user(self, user_id: int) -> dict[str, Any] | None:
        with self._database() as connection:
            row = connection.execute(
                "SELECT id, username, created_at FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
        return _row_to_dict(row) if row else None

    def get_session_secret(self) -> str:
        return self.get_setting("session_secret")

    def get_exclude_keywords(self) -> str:
        return self.get_setting("exclude_keywords")

    def get_advanced_filter_settings(self) -> dict[str, Any]:
        return {
            "exclude_keywords": self.get_exclude_keywords(),
            "exclude_protocols": self._normalize_protocols(self.get_setting("exclude_protocols")),
            "dedup_strategy": self._normalize_dedup_strategy(self.get_setting("dedup_strategy", "uri")),
            "rename_rules": self.get_setting("rename_rules"),
        }

    def update_settings(
        self,
        exclude_keywords: str,
        *,
        exclude_protocols: str | None = None,
        dedup_strategy: str | None = None,
        rename_rules: str | None = None,
    ) -> dict[str, Any]:
        self.set_setting("exclude_keywords", self._normalize_keywords(exclude_keywords))
        if exclude_protocols is not None:
            self.set_setting("exclude_protocols", self._normalize_protocols(exclude_protocols))
        if dedup_strategy is not None:
            self.set_setting("dedup_strategy", self._normalize_dedup_strategy(dedup_strategy))
        if rename_rules is not None:
            self.set_setting("rename_rules", self._normalize_rename_rules(rename_rules))
        return self.get_advanced_filter_settings()

    def get_cleanup_settings(self) -> dict[str, Any]:
        raw_auto_disable_expired = self.get_setting("cleanup_auto_disable_expired", "0")
        raw_pause_threshold = self.get_setting("cleanup_pause_failures_threshold", "0")
        try:
            pause_threshold = int(raw_pause_threshold)
        except ValueError:
            pause_threshold = 0
        return {
            "auto_disable_expired": raw_auto_disable_expired == "1",
            "pause_failures_threshold": max(0, min(pause_threshold, 20)),
        }

    def update_cleanup_settings(
        self,
        *,
        auto_disable_expired: bool,
        pause_failures_threshold: int = 0,
    ) -> dict[str, Any]:
        try:
            normalized_threshold = int(pause_failures_threshold)
        except (TypeError, ValueError) as exc:
            raise ValueError("Failure pause threshold must be a whole number.") from exc
        normalized_threshold = max(0, min(normalized_threshold, 20))
        self.set_setting("cleanup_auto_disable_expired", "1" if auto_disable_expired else "0")
        self.set_setting("cleanup_pause_failures_threshold", str(normalized_threshold))
        return self.get_cleanup_settings()

    def get_notification_settings(self) -> dict[str, Any]:
        cooldown = self.get_setting("notifications_cooldown_minutes", "360")
        try:
            cooldown_minutes = int(cooldown)
        except ValueError:
            cooldown_minutes = 360
        return {
            "telegram_bot_token": self.get_setting("notifications_telegram_bot_token"),
            "telegram_chat_id": self.get_setting("notifications_telegram_chat_id"),
            "webhook_url": self.get_setting("notifications_webhook_url"),
            "min_severity": self.get_setting("notifications_min_severity", "warning"),
            "cooldown_minutes": max(5, min(cooldown_minutes, 10080)),
        }

    def update_notification_settings(
        self,
        *,
        telegram_bot_token: str,
        telegram_chat_id: str,
        webhook_url: str,
        min_severity: str = "warning",
        cooldown_minutes: int = 360,
    ) -> dict[str, Any]:
        normalized_severity = str(min_severity or "warning").strip().lower()
        if normalized_severity not in {"warning", "danger"}:
            raise ValueError("Notification severity must be warning or danger.")
        try:
            normalized_cooldown = int(cooldown_minutes)
        except (TypeError, ValueError) as exc:
            raise ValueError("Notification cooldown must be a whole number of minutes.") from exc
        normalized_cooldown = max(5, min(normalized_cooldown, 10080))
        self.set_setting("notifications_telegram_bot_token", str(telegram_bot_token or "").strip())
        self.set_setting("notifications_telegram_chat_id", str(telegram_chat_id or "").strip())
        self.set_setting("notifications_webhook_url", str(webhook_url or "").strip())
        self.set_setting("notifications_min_severity", normalized_severity)
        self.set_setting("notifications_cooldown_minutes", str(normalized_cooldown))
        return self.get_notification_settings()

    def get_panel_port(self, default: int = 8787) -> int:
        raw_value = self.get_setting("panel_port", str(default))
        try:
            port = int(raw_value)
        except ValueError:
            return default
        if 1 <= port <= 65535:
            return port
        return default

    def update_panel_port(self, panel_port: int) -> int:
        try:
            port = int(panel_port)
        except (TypeError, ValueError) as exc:
            raise ValueError("Panel port must be a number between 1 and 65535.") from exc
        if not 1 <= port <= 65535:
            raise ValueError("Panel port must be a number between 1 and 65535.")
        self.set_setting("panel_port", str(port))
        return port

    def get_default_theme(self) -> str:
        return self._normalize_theme_name(self.get_setting("default_theme", "classic"))

    def update_default_theme(self, theme: str) -> str:
        normalized_theme = self._normalize_theme_name(theme)
        self.set_setting("default_theme", normalized_theme)
        return normalized_theme

    def update_current_user_credentials(
        self,
        user_id: int,
        *,
        username: str,
        current_password: str,
        new_password: str = "",
    ) -> dict[str, Any]:
        normalized_username = username.strip()
        if not normalized_username:
            raise ValueError("Username is required.")
        if not current_password:
            raise ValueError("Current password is required.")
        if new_password and len(new_password) < 8:
            raise ValueError("New password must be at least 8 characters.")

        with self._database() as connection:
            row = connection.execute(
                "SELECT * FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
            if row is None:
                raise KeyError("User not found.")

            candidate_hash, _ = self._hash_password(current_password, row["password_salt"])
            if not secrets.compare_digest(candidate_hash, row["password_hash"]):
                raise ValueError("Current password is incorrect.")

            password_hash = row["password_hash"]
            password_salt = row["password_salt"]
            if new_password:
                password_hash, password_salt = self._hash_password(new_password)

            try:
                connection.execute(
                    """
                    UPDATE users
                    SET username = ?, password_hash = ?, password_salt = ?
                    WHERE id = ?
                    """,
                    (normalized_username, password_hash, password_salt, user_id),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError("Username already exists.") from exc

        return self.get_user(user_id) or {"id": user_id, "username": normalized_username}

    def reset_primary_user(
        self,
        username: str | None = None,
        password: str | None = None,
    ) -> dict[str, Any]:
        generated_password = password or secrets.token_urlsafe(12)
        if len(generated_password) < 8:
            raise ValueError("Password must be at least 8 characters.")

        with self._database() as connection:
            row = connection.execute(
                "SELECT * FROM users ORDER BY id ASC LIMIT 1"
            ).fetchone()

            if row is None:
                normalized_username = (username or "admin").strip()
                if not normalized_username:
                    raise ValueError("Username is required.")
                password_hash, password_salt = self._hash_password(generated_password)
                created_at = to_iso(utc_now())
                cursor = connection.execute(
                    """
                    INSERT INTO users (username, password_hash, password_salt, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (normalized_username, password_hash, password_salt, created_at),
                )
                return {
                    "id": cursor.lastrowid,
                    "username": normalized_username,
                    "password": generated_password,
                    "created": True,
                }

            normalized_username = (username or row["username"]).strip()
            if not normalized_username:
                raise ValueError("Username is required.")
            password_hash, password_salt = self._hash_password(generated_password)
            try:
                connection.execute(
                    """
                    UPDATE users
                    SET username = ?, password_hash = ?, password_salt = ?
                    WHERE id = ?
                    """,
                    (normalized_username, password_hash, password_salt, row["id"]),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError("Username already exists.") from exc

        return {
            "id": row["id"],
            "username": normalized_username,
            "password": generated_password,
            "created": False,
        }

    def list_groups(self) -> list[dict[str, Any]]:
        with self._database() as connection:
            rows = connection.execute(
                """
                SELECT g.*,
                       COUNT(s.id) AS subscription_count
                FROM subscription_groups g
                LEFT JOIN subscriptions s ON s.group_id = g.id
                GROUP BY g.id
                ORDER BY g.name COLLATE NOCASE ASC, g.id ASC
                """
            ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def add_group(self, name: str, description: str = "", color: str = "#0c8d8a") -> dict[str, Any]:
        normalized_name = name.strip()
        if not normalized_name:
            raise ValueError("Group name is required.")

        now = to_iso(utc_now())
        normalized_color = self._normalize_color(color)
        with self._database() as connection:
            try:
                cursor = connection.execute(
                    """
                    INSERT INTO subscription_groups (name, description, color, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (normalized_name, description.strip(), normalized_color, now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError("Group name already exists.") from exc
        return self.get_group(cursor.lastrowid)

    def update_group(
        self,
        group_id: int,
        name: str,
        description: str = "",
        color: str = "#0c8d8a",
    ) -> dict[str, Any]:
        normalized_name = name.strip()
        if not normalized_name:
            raise ValueError("Group name is required.")

        now = to_iso(utc_now())
        normalized_color = self._normalize_color(color)
        with self._database() as connection:
            try:
                cursor = connection.execute(
                    """
                    UPDATE subscription_groups
                    SET name = ?, description = ?, color = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (normalized_name, description.strip(), normalized_color, now, group_id),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError("Group name already exists.") from exc

            if cursor.rowcount == 0:
                raise KeyError("Group not found.")
        return self.get_group(group_id)

    def get_group(self, group_id: int) -> dict[str, Any]:
        with self._database() as connection:
            row = connection.execute(
                """
                SELECT g.*,
                       COUNT(s.id) AS subscription_count
                FROM subscription_groups g
                LEFT JOIN subscriptions s ON s.group_id = g.id
                WHERE g.id = ?
                GROUP BY g.id
                """,
                (group_id,),
            ).fetchone()
        if row is None:
            raise KeyError("Group not found.")
        return _row_to_dict(row)

    def delete_group(self, group_id: int) -> None:
        with self._database() as connection:
            cursor = connection.execute(
                "DELETE FROM subscription_groups WHERE id = ?",
                (group_id,),
            )
            if cursor.rowcount == 0:
                raise KeyError("Group not found.")

    def list_subscriptions(self) -> list[dict[str, Any]]:
        with self._database() as connection:
            rows = connection.execute(
                """
                SELECT s.*,
                       g.name AS group_name,
                       g.color AS group_color
                FROM subscriptions s
                LEFT JOIN subscription_groups g ON g.id = s.group_id
                ORDER BY s.created_at DESC, s.id DESC
                """
            ).fetchall()
            summaries = self._protocol_summary_map(connection)
            latest_log_map = self._latest_refresh_log_map(connection)

        subscriptions: list[dict[str, Any]] = []
        for row in rows:
            item = _row_to_dict(row)
            item["enabled"] = bool(item["enabled"])
            item["source_type"] = item.get("source_type") or "remote"
            item["is_manual"] = item["source_type"] == "manual"
            item["supports_scheduling"] = item["source_type"] == "remote"
            item["protocol_summary"] = summaries.get(item["id"], "")
            item["is_expired"] = self._is_expired(item["expires_at"])
            item["latest_refresh_log"] = latest_log_map.get(item["id"])
            subscriptions.append(item)
        return subscriptions

    def get_subscription(self, subscription_id: int) -> dict[str, Any]:
        with self._database() as connection:
            row = connection.execute(
                """
                SELECT s.*,
                       g.name AS group_name,
                       g.color AS group_color
                FROM subscriptions s
                LEFT JOIN subscription_groups g ON g.id = s.group_id
                WHERE s.id = ?
                """,
                (subscription_id,),
            ).fetchone()
            summaries = self._protocol_summary_map(connection)
            latest_log_map = self._latest_refresh_log_map(connection, subscription_id=subscription_id)

        if row is None:
            raise KeyError("Subscription not found.")

        item = _row_to_dict(row)
        item["enabled"] = bool(item["enabled"])
        item["source_type"] = item.get("source_type") or "remote"
        item["is_manual"] = item["source_type"] == "manual"
        item["supports_scheduling"] = item["source_type"] == "remote"
        item["protocol_summary"] = summaries.get(item["id"], "")
        item["is_expired"] = self._is_expired(item["expires_at"])
        item["latest_refresh_log"] = latest_log_map.get(item["id"])
        return item

    def list_subscription_refresh_logs(self, subscription_id: int, limit: int = 20) -> list[dict[str, Any]]:
        self.get_subscription(subscription_id)
        try:
            normalized_limit = int(limit)
        except (TypeError, ValueError) as exc:
            raise ValueError("Log limit must be a whole number.") from exc
        normalized_limit = max(1, min(normalized_limit, 100))
        with self._database() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM subscription_refresh_logs
                WHERE subscription_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (subscription_id, normalized_limit),
            ).fetchall()
        logs: list[dict[str, Any]] = []
        for row in rows:
            item = _row_to_dict(row)
            item["added_sample"] = self._decode_json_list(item.get("added_sample_json"))
            item["removed_sample"] = self._decode_json_list(item.get("removed_sample_json"))
            logs.append(item)
        return logs

    def bulk_import_subscriptions(
        self,
        raw_text: str,
        *,
        group_id: int | None = None,
        refresh_interval_hours: int = 24,
    ) -> dict[str, Any]:
        lines = [line.strip() for line in str(raw_text or "").splitlines() if line.strip()]
        if not lines:
            raise ValueError("Paste at least one subscription link.")
        normalized_group_id = self._normalize_group_id(group_id)
        normalized_interval = self._normalize_refresh_interval(refresh_interval_hours)
        created: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        for index, line in enumerate(lines, start=1):
            name = ""
            url = line
            if "\t" in line:
                name, url = [part.strip() for part in line.split("\t", 1)]
            elif "," in line and "http" in line.lower():
                maybe_name, maybe_url = [part.strip() for part in line.split(",", 1)]
                if maybe_url.lower().startswith(("http://", "https://")):
                    name, url = maybe_name, maybe_url
            if not url.lower().startswith(("http://", "https://")):
                errors.append({"line": index, "input": line, "error": "订阅链接必须以 http:// 或 https:// 开头。"})
                continue
            try:
                created.append(
                    self.add_subscription(
                        name=name,
                        url=url,
                        group_id=normalized_group_id,
                        refresh_interval_hours=normalized_interval,
                    )
                )
            except Exception as exc:
                errors.append({"line": index, "input": line, "error": str(exc)})
        return {
            "created_count": len(created),
            "error_count": len(errors),
            "created": created,
            "errors": errors,
        }

    def preview_subscription_input(
        self,
        *,
        source_type: str,
        url: str = "",
        manual_content: str = "",
    ) -> dict[str, Any]:
        normalized_source_type = self._normalize_source_type(source_type)
        normalized_url = url.strip()
        normalized_manual_content = self._normalize_manual_content(manual_content)
        if normalized_source_type == "remote":
            if not normalized_url:
                raise ValueError("Subscription URL is required.")
        else:
            if not normalized_manual_content:
                raise ValueError("Manual subscription content is required.")

        result = self._load_subscription_payload(
            normalized_source_type,
            normalized_url,
            normalized_manual_content,
        )
        protocol_counts: dict[str, int] = {}
        for node in result.nodes:
            protocol_counts[node.protocol] = protocol_counts.get(node.protocol, 0) + 1
        warnings: list[str] = []
        if normalized_source_type == "manual":
            warnings.append("手动订阅不会参与自动定时刷新，只会在保存或手动刷新时重新解析。")
        return {
            "source_type": normalized_source_type,
            "source_format": "manual_text" if normalized_source_type == "manual" else result.source_format,
            "stats": {
                "total_nodes": len(result.nodes),
                "protocol_counts": protocol_counts,
            },
            "sample_nodes": [
                {
                    "name": node.name,
                    "protocol": node.protocol,
                    "uri": node.uri,
                }
                for node in result.nodes[:12]
            ],
            "warnings": warnings,
        }

    def add_subscription(
        self,
        name: str,
        url: str,
        group_id: int | None = None,
        expires_at: str | None = None,
        refresh_interval_hours: int = 24,
        refresh_now: bool = True,
        source_type: str = "remote",
        manual_content: str = "",
    ) -> dict[str, Any]:
        normalized_source_type = self._normalize_source_type(source_type)
        normalized_url = url.strip()
        normalized_manual_content = self._normalize_manual_content(manual_content)
        if normalized_source_type == "remote":
            if not normalized_url:
                raise ValueError("Subscription URL is required.")
        else:
            if not normalized_manual_content:
                raise ValueError("Manual subscription content is required.")
            parse_subscription_payload(normalized_manual_content.encode("utf-8"))
            normalized_url = "local://manual"

        normalized_group_id = self._normalize_group_id(group_id)
        normalized_expires_at = normalize_datetime_input(expires_at)
        normalized_refresh_interval = self._normalize_refresh_interval(refresh_interval_hours)
        label = name.strip() or self._default_subscription_name(normalized_url, normalized_source_type)
        now = utc_now()
        next_refresh_at = to_iso(now) if normalized_source_type == "remote" else None
        last_status = "queued" if normalized_source_type == "remote" else "idle"

        with self._database() as connection:
            cursor = connection.execute(
                """
                INSERT INTO subscriptions (
                    name, url, source_type, manual_content, group_id, expires_at, enabled,
                    refresh_interval_hours, last_status, next_refresh_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
                """,
                (
                    label,
                    normalized_url,
                    normalized_source_type,
                    normalized_manual_content,
                    normalized_group_id,
                    normalized_expires_at,
                    normalized_refresh_interval,
                    last_status,
                    next_refresh_at,
                    to_iso(now),
                    to_iso(now),
                ),
            )
        subscription_id = cursor.lastrowid
        if refresh_now or normalized_source_type == "manual":
            self.refresh_subscription(subscription_id, trigger="save")
        return self.get_subscription(subscription_id)

    def update_subscription(
        self,
        subscription_id: int,
        *,
        name: str,
        url: str,
        group_id: int | None,
        expires_at: str | None,
        refresh_interval_hours: int,
        source_type: str = "remote",
        manual_content: str = "",
    ) -> dict[str, Any]:
        current = self.get_subscription(subscription_id)
        normalized_source_type = self._normalize_source_type(source_type)
        normalized_url = url.strip()
        normalized_manual_content = self._normalize_manual_content(manual_content)
        if normalized_source_type == "remote":
            if not normalized_url:
                raise ValueError("Subscription URL is required.")
        else:
            if not normalized_manual_content:
                raise ValueError("Manual subscription content is required.")
            parse_subscription_payload(normalized_manual_content.encode("utf-8"))
            normalized_url = "local://manual"

        normalized_group_id = self._normalize_group_id(group_id)
        normalized_expires_at = normalize_datetime_input(expires_at)
        normalized_refresh_interval = self._normalize_refresh_interval(refresh_interval_hours)
        label = name.strip() or self._default_subscription_name(normalized_url, normalized_source_type)
        now = utc_now()
        should_requeue_remote = (
            normalized_source_type == "remote"
            and current["enabled"]
            and (
                current["source_type"] != "remote"
                or current["url"] != normalized_url
                or current["refresh_interval_hours"] != normalized_refresh_interval
            )
        )
        should_refresh_manual = (
            normalized_source_type == "manual"
            and current["enabled"]
            and (
                current["source_type"] != "manual"
                or current.get("manual_content", "") != normalized_manual_content
            )
        )
        if normalized_source_type == "remote":
            next_refresh_at = to_iso(now) if should_requeue_remote else current["next_refresh_at"]
            last_status = "queued" if should_requeue_remote else current["last_status"]
        else:
            next_refresh_at = None
            last_status = current["last_status"] if current["enabled"] else "idle"

        with self._database() as connection:
            cursor = connection.execute(
                """
                UPDATE subscriptions
                SET name = ?,
                    url = ?,
                    source_type = ?,
                    manual_content = ?,
                    group_id = ?,
                    expires_at = ?,
                    refresh_interval_hours = ?,
                    next_refresh_at = ?,
                    last_status = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    label,
                    normalized_url,
                    normalized_source_type,
                    normalized_manual_content,
                    normalized_group_id,
                    normalized_expires_at,
                    normalized_refresh_interval,
                    next_refresh_at,
                    last_status,
                    to_iso(now),
                    subscription_id,
                ),
            )
            if cursor.rowcount == 0:
                raise KeyError("Subscription not found.")
        if should_refresh_manual:
            return self.refresh_subscription(subscription_id, trigger="update")
        return self.get_subscription(subscription_id)

    def delete_subscription(self, subscription_id: int) -> None:
        with self._database() as connection:
            cursor = connection.execute(
                "DELETE FROM subscriptions WHERE id = ?",
                (subscription_id,),
            )
            if cursor.rowcount == 0:
                raise KeyError("Subscription not found.")

    def set_subscription_enabled(self, subscription_id: int, enabled: bool) -> dict[str, Any]:
        current = self.get_subscription(subscription_id)
        now = utc_now()
        next_refresh_at = to_iso(now) if enabled and current["source_type"] == "remote" else None
        if enabled:
            if current["source_type"] == "remote":
                last_status = "queued"
            else:
                last_status = current["last_status"] if current["last_status"] != "idle" else "ok"
        else:
            last_status = "idle"
        with self._database() as connection:
            cursor = connection.execute(
                """
                UPDATE subscriptions
                SET enabled = ?,
                    next_refresh_at = ?,
                    last_status = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    int(enabled),
                    next_refresh_at,
                    last_status,
                    to_iso(now),
                    subscription_id,
                ),
            )
            if cursor.rowcount == 0:
                raise KeyError("Subscription not found.")
        if enabled and current["source_type"] == "manual" and current.get("manual_content", "").strip():
            return self.refresh_subscription(subscription_id, trigger="enable")
        return self.get_subscription(subscription_id)

    def refresh_due_subscriptions(self) -> list[dict[str, Any]]:
        now = utc_now()
        with self._database() as connection:
            rows = connection.execute(
                """
                SELECT id
                FROM subscriptions
                WHERE enabled = 1
                  AND source_type = 'remote'
                  AND next_refresh_at IS NOT NULL
                  AND next_refresh_at <= ?
                ORDER BY next_refresh_at ASC, id ASC
                """,
                (to_iso(now),),
            ).fetchall()
        return [self.refresh_subscription(row["id"], trigger="scheduler") for row in rows]

    def refresh_all(self) -> list[dict[str, Any]]:
        with self._database() as connection:
            rows = connection.execute(
                "SELECT id FROM subscriptions WHERE enabled = 1 ORDER BY id ASC"
            ).fetchall()
        return [self.refresh_subscription(row["id"], trigger="bulk") for row in rows]

    def refresh_subscription(self, subscription_id: int, *, trigger: str = "manual") -> dict[str, Any]:
        with self.refresh_lock:
            subscription = self.get_subscription(subscription_id)
            now = utc_now()
            started_at = now
            node_count_before = int(subscription.get("node_count", 0) or 0)
            previous_uris: set[str] = set()
            previous_sort_order_by_uri: dict[str, int] = {}
            next_sort_order = 0
            with self._database() as connection:
                existing_rows = connection.execute(
                    """
                    SELECT uri, sort_order, id
                    FROM nodes
                    WHERE subscription_id = ?
                    ORDER BY sort_order ASC, id ASC
                    """,
                    (subscription_id,),
                ).fetchall()
                previous_uris = {row["uri"] for row in existing_rows}
                for row in existing_rows:
                    uri = row["uri"]
                    if uri in previous_sort_order_by_uri:
                        continue
                    sort_order = int(row["sort_order"] or 0)
                    previous_sort_order_by_uri[uri] = sort_order
                    next_sort_order = max(next_sort_order, sort_order + 1)

            try:
                payload = self._load_subscription_payload_bytes(
                    subscription["source_type"],
                    subscription["url"],
                    subscription.get("manual_content", ""),
                )
                result = parse_subscription_payload(payload)
            except Exception as exc:
                finished_at = utc_now()
                next_retry = (
                    finished_at + self._failure_backoff(subscription["refresh_interval_hours"])
                    if subscription["source_type"] == "remote"
                    else None
                )
                failure_expression = "consecutive_failures + 1" if subscription["source_type"] == "remote" else "0"
                with self._database() as connection:
                    connection.execute(
                        f"""
                        UPDATE subscriptions
                        SET last_status = 'error',
                            consecutive_failures = {failure_expression},
                            last_error = ?,
                            next_refresh_at = ?,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (str(exc), to_iso(next_retry), to_iso(finished_at), subscription_id),
                    )
                    self._insert_refresh_log(
                        connection,
                        subscription_id=subscription_id,
                        trigger=trigger,
                        status="error",
                        started_at=started_at,
                        finished_at=finished_at,
                        node_count_before=node_count_before,
                        node_count_after=node_count_before,
                        added_sample=[],
                        removed_sample=[],
                        source_format=subscription.get("source_format") or "",
                        error_message=str(exc),
                    )
                return self.get_subscription(subscription_id)

            finished_at = utc_now()
            current_uris = {node.uri for node in result.nodes}
            added_sample = list(current_uris - previous_uris)[:8]
            removed_sample = list(previous_uris - current_uris)[:8]
            used_sort_orders: set[int] = set()
            stable_node_rows: list[tuple[int, str, str, str, int, str | None]] = []
            created_at = to_iso(now)
            for node in result.nodes:
                sort_order = previous_sort_order_by_uri.get(node.uri)
                if sort_order is None or sort_order in used_sort_orders:
                    while next_sort_order in used_sort_orders:
                        next_sort_order += 1
                    sort_order = next_sort_order
                    next_sort_order += 1
                used_sort_orders.add(sort_order)
                stable_node_rows.append(
                    (
                        subscription_id,
                        node.uri,
                        node.name,
                        node.protocol,
                        sort_order,
                        created_at,
                    )
                )
            with self._database() as connection:
                connection.execute(
                    "DELETE FROM nodes WHERE subscription_id = ?",
                    (subscription_id,),
                )
                connection.executemany(
                    """
                    INSERT INTO nodes (subscription_id, uri, name, protocol, sort_order, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    stable_node_rows,
                )
                connection.execute(
                    """
                    UPDATE subscriptions
                    SET last_status = 'ok',
                        consecutive_failures = 0,
                        last_error = NULL,
                        last_updated_at = ?,
                        next_refresh_at = ?,
                        node_count = ?,
                        refresh_count = refresh_count + 1,
                        source_format = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        to_iso(finished_at),
                        (
                            to_iso(finished_at + timedelta(hours=subscription["refresh_interval_hours"]))
                            if subscription["source_type"] == "remote"
                            else None
                        ),
                        len(result.nodes),
                        "manual_text" if subscription["source_type"] == "manual" else result.source_format,
                        to_iso(finished_at),
                        subscription_id,
                    ),
                )
                self._insert_refresh_log(
                    connection,
                    subscription_id=subscription_id,
                    trigger=trigger,
                    status="ok",
                    started_at=started_at,
                    finished_at=finished_at,
                    node_count_before=node_count_before,
                    node_count_after=len(result.nodes),
                    added_sample=added_sample,
                    removed_sample=removed_sample,
                    source_format="manual_text" if subscription["source_type"] == "manual" else result.source_format,
                )
        return self.get_subscription(subscription_id)

    def list_profiles(self, base_url: str) -> list[dict[str, Any]]:
        with self._database() as connection:
            rows = connection.execute(
                "SELECT * FROM merge_profiles ORDER BY created_at ASC, id ASC"
            ).fetchall()
            source_map = self._profile_source_map(connection)
            refresh_counts = self._subscription_refresh_count_map(connection)
            enabled_stats = self._enabled_subscription_stats(connection)
            access_stats = self._profile_access_stats_map(connection)

        default_profile_id = self.get_default_profile()["id"]
        profiles: list[dict[str, Any]] = []
        for row in rows:
            item = _row_to_dict(row)
            item["include_all"] = bool(item["include_all"])
            item["mode"] = "all" if item["include_all"] else "selected"
            item["selected_subscription_ids"] = source_map.get(item["id"], [])
            item["priority_subscription_ids"] = list(item["selected_subscription_ids"])
            item["priority_source_count"] = len(item["priority_subscription_ids"])
            item["source_count"] = (
                enabled_stats["count"]
                if item["include_all"]
                else len(item["selected_subscription_ids"])
            )
            item["source_refresh_count"] = (
                enabled_stats["refresh_count"]
                if item["include_all"]
                else sum(refresh_counts.get(subscription_id, 0) for subscription_id in item["selected_subscription_ids"])
            )
            access_stat = access_stats.get(item["id"], {"access_count_24h": 0, "access_count_7d": 0})
            item["access_count_24h"] = access_stat["access_count_24h"]
            item["access_count_7d"] = access_stat["access_count_7d"]
            item["merged_node_count"] = len(self.get_nodes_for_profile(item["id"]))
            item["export_url"] = f"{base_url}/subscribe/{item['token']}"
            item["plain_export_url"] = f"{base_url}/subscribe/{item['token']}?format=plain"
            item["json_export_url"] = f"{base_url}/subscribe/{item['token']}?format=json"
            item["clash_export_url"] = f"{base_url}/subscribe/{item['token']}?format=clash"
            item["surge_export_url"] = f"{base_url}/subscribe/{item['token']}?format=surge"
            item["singbox_export_url"] = f"{base_url}/subscribe/{item['token']}?format=singbox"
            item["is_default"] = item["id"] == default_profile_id
            profiles.append(item)
        return profiles

    def get_profile(self, profile_id: int, base_url: str | None = None) -> dict[str, Any]:
        with self._database() as connection:
            row = connection.execute(
                "SELECT * FROM merge_profiles WHERE id = ?",
                (profile_id,),
            ).fetchone()
            source_map = self._profile_source_map(connection)
            refresh_counts = self._subscription_refresh_count_map(connection)
            enabled_stats = self._enabled_subscription_stats(connection)
            access_stats = self._profile_access_stats_map(connection, profile_id=profile_id)

        if row is None:
            raise KeyError("Profile not found.")

        item = _row_to_dict(row)
        item["include_all"] = bool(item["include_all"])
        item["mode"] = "all" if item["include_all"] else "selected"
        item["selected_subscription_ids"] = source_map.get(item["id"], [])
        item["priority_subscription_ids"] = list(item["selected_subscription_ids"])
        item["priority_source_count"] = len(item["priority_subscription_ids"])
        item["source_count"] = (
            enabled_stats["count"]
            if item["include_all"]
            else len(item["selected_subscription_ids"])
        )
        item["source_refresh_count"] = (
            enabled_stats["refresh_count"]
            if item["include_all"]
            else sum(refresh_counts.get(subscription_id, 0) for subscription_id in item["selected_subscription_ids"])
        )
        access_stat = access_stats.get(item["id"], {"access_count_24h": 0, "access_count_7d": 0})
        item["access_count_24h"] = access_stat["access_count_24h"]
        item["access_count_7d"] = access_stat["access_count_7d"]
        item["merged_node_count"] = len(self._get_nodes_for_profile_record(item))
        if base_url:
            item["export_url"] = f"{base_url}/subscribe/{item['token']}"
            item["plain_export_url"] = f"{base_url}/subscribe/{item['token']}?format=plain"
            item["json_export_url"] = f"{base_url}/subscribe/{item['token']}?format=json"
            item["clash_export_url"] = f"{base_url}/subscribe/{item['token']}?format=clash"
            item["surge_export_url"] = f"{base_url}/subscribe/{item['token']}?format=surge"
            item["singbox_export_url"] = f"{base_url}/subscribe/{item['token']}?format=singbox"
        return item

    def get_default_profile(self) -> dict[str, Any]:
        with self._database() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM merge_profiles
                ORDER BY include_all DESC, created_at ASC, id ASC
                LIMIT 1
                """
            ).fetchone()
            source_map = self._profile_source_map(connection)
            refresh_counts = self._subscription_refresh_count_map(connection)
            enabled_stats = self._enabled_subscription_stats(connection)
            access_stats = self._profile_access_stats_map(connection)

        if row is None:
            raise KeyError("Profile not found.")

        item = _row_to_dict(row)
        item["include_all"] = bool(item["include_all"])
        item["mode"] = "all" if item["include_all"] else "selected"
        item["selected_subscription_ids"] = source_map.get(item["id"], [])
        item["priority_subscription_ids"] = list(item["selected_subscription_ids"])
        item["priority_source_count"] = len(item["priority_subscription_ids"])
        item["source_count"] = (
            enabled_stats["count"]
            if item["include_all"]
            else len(item["selected_subscription_ids"])
        )
        item["source_refresh_count"] = (
            enabled_stats["refresh_count"]
            if item["include_all"]
            else sum(refresh_counts.get(subscription_id, 0) for subscription_id in item["selected_subscription_ids"])
        )
        access_stat = access_stats.get(item["id"], {"access_count_24h": 0, "access_count_7d": 0})
        item["access_count_24h"] = access_stat["access_count_24h"]
        item["access_count_7d"] = access_stat["access_count_7d"]
        return item

    def add_profile(
        self,
        name: str,
        description: str = "",
        mode: str = "selected",
        subscription_ids: Iterable[int] | None = None,
        exclude_keywords: str = "",
        exclude_protocols: str = "",
    ) -> dict[str, Any]:
        normalized_name = name.strip()
        if not normalized_name:
            raise ValueError("Profile name is required.")

        include_all = mode == "all"
        normalized_subscription_ids = self._normalize_subscription_ids(subscription_ids or [])
        if not include_all and not normalized_subscription_ids:
            raise ValueError("Select at least one subscription for this profile.")

        now = to_iso(utc_now())
        token = secrets.token_urlsafe(24)
        normalized_keywords = self._normalize_keywords(exclude_keywords)
        normalized_protocols = self._normalize_protocols(exclude_protocols)

        with self._database() as connection:
            cursor = connection.execute(
                """
                INSERT INTO merge_profiles (
                    name, description, token, exclude_keywords, exclude_protocols, include_all, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_name,
                    description.strip(),
                    token,
                    normalized_keywords,
                    normalized_protocols,
                    int(include_all),
                    now,
                    now,
                ),
            )
            self._replace_profile_sources(
                connection,
                cursor.lastrowid,
                normalized_subscription_ids,
            )
        return self.get_profile(cursor.lastrowid)

    def update_profile(
        self,
        profile_id: int,
        *,
        name: str,
        description: str = "",
        mode: str = "selected",
        subscription_ids: Iterable[int] | None = None,
        exclude_keywords: str = "",
        exclude_protocols: str = "",
    ) -> dict[str, Any]:
        normalized_name = name.strip()
        if not normalized_name:
            raise ValueError("Profile name is required.")

        include_all = mode == "all"
        normalized_subscription_ids = self._normalize_subscription_ids(subscription_ids or [])
        if not include_all and not normalized_subscription_ids:
            raise ValueError("Select at least one subscription for this profile.")

        normalized_keywords = self._normalize_keywords(exclude_keywords)
        normalized_protocols = self._normalize_protocols(exclude_protocols)
        with self._database() as connection:
            cursor = connection.execute(
                """
                UPDATE merge_profiles
                SET name = ?,
                    description = ?,
                    exclude_keywords = ?,
                    exclude_protocols = ?,
                    include_all = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    normalized_name,
                    description.strip(),
                    normalized_keywords,
                    normalized_protocols,
                    int(include_all),
                    to_iso(utc_now()),
                    profile_id,
                ),
            )
            if cursor.rowcount == 0:
                raise KeyError("Profile not found.")
            self._replace_profile_sources(
                connection,
                profile_id,
                normalized_subscription_ids,
            )
        return self.get_profile(profile_id)

    def delete_profile(self, profile_id: int) -> None:
        with self._database() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM merge_profiles").fetchone()
            if row["count"] <= 1:
                raise ValueError("At least one merge profile must remain.")

            cursor = connection.execute(
                "DELETE FROM merge_profiles WHERE id = ?",
                (profile_id,),
            )
            if cursor.rowcount == 0:
                raise KeyError("Profile not found.")

    def regenerate_profile_token(self, profile_id: int) -> dict[str, Any]:
        token = secrets.token_urlsafe(24)
        with self._database() as connection:
            cursor = connection.execute(
                """
                UPDATE merge_profiles
                SET token = ?, updated_at = ?
                WHERE id = ?
                """,
                (token, to_iso(utc_now()), profile_id),
            )
            if cursor.rowcount == 0:
                raise KeyError("Profile not found.")
        return self.get_profile(profile_id)

    def clone_profile(self, profile_id: int, name: str = "") -> dict[str, Any]:
        source_profile = self.get_profile(profile_id)
        clone_name = name.strip() or f"{source_profile['name']} 副本"
        return self.add_profile(
            name=clone_name,
            description=source_profile.get("description", ""),
            mode=source_profile["mode"],
            subscription_ids=source_profile.get("selected_subscription_ids", []),
            exclude_keywords=source_profile.get("exclude_keywords", ""),
            exclude_protocols=source_profile.get("exclude_protocols", ""),
        )

    def get_nodes_for_profile(self, profile_id: int) -> list[NodeEntry]:
        profile = self.get_profile(profile_id)
        return self._get_nodes_for_profile_record(profile)

    def get_nodes(
        self,
        subscription_ids: Iterable[int] | None = None,
        *,
        only_enabled: bool = True,
    ) -> list[NodeEntry]:
        filters: list[str] = []
        params: list[Any] = []

        if only_enabled:
            filters.append("s.enabled = 1")

        normalized_ids = (
            self._normalize_subscription_ids(subscription_ids or [])
            if subscription_ids is not None
            else None
        )
        if normalized_ids is not None:
            if not normalized_ids:
                return []
            placeholders = ", ".join("?" for _ in normalized_ids)
            filters.append(f"s.id IN ({placeholders})")
            params.extend(normalized_ids)

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        with self._database() as connection:
            rows = connection.execute(
                f"""
                SELECT n.id AS node_id, n.subscription_id, n.uri, n.name, n.protocol, n.sort_order
                FROM nodes n
                INNER JOIN subscriptions s ON s.id = n.subscription_id
                {where_clause}
                ORDER BY s.id ASC, n.sort_order ASC, n.id ASC
                """,
                params,
            ).fetchall()

        if normalized_ids is not None:
            source_order = {subscription_id: index for index, subscription_id in enumerate(normalized_ids)}
            sorted_rows = sorted(
                rows,
                key=lambda row: (
                    source_order.get(int(row["subscription_id"]), len(source_order)),
                    int(row["sort_order"] or 0),
                    int(row["node_id"]),
                ),
            )
        else:
            sorted_rows = rows
        return [NodeEntry(uri=row["uri"], name=row["name"], protocol=row["protocol"]) for row in sorted_rows]

    def preview_profile_nodes(
        self,
        profile_id: int,
        *,
        search: str = "",
        protocol: str = "",
        limit: int = 200,
    ) -> dict[str, Any]:
        profile = self.get_profile(profile_id)
        nodes = self._get_nodes_for_profile_record(profile)
        search_value = search.strip().lower()
        protocol_value = protocol.strip().lower()

        filtered_nodes: list[NodeEntry] = []
        for node in nodes:
            if protocol_value and node.protocol.lower() != protocol_value:
                continue
            if search_value:
                haystack = f"{node.name} {node.protocol} {node.uri}".lower()
                if search_value not in haystack:
                    continue
            filtered_nodes.append(node)

        clamped_limit = max(1, min(int(limit), 500))
        items = [
            {
                "name": node.name,
                "protocol": node.protocol,
                "uri": node.uri,
            }
            for node in filtered_nodes[:clamped_limit]
        ]
        protocol_counts: dict[str, int] = {}
        for node in filtered_nodes:
            protocol_counts[node.protocol] = protocol_counts.get(node.protocol, 0) + 1

        return {
            "profile": {
                "id": profile["id"],
                "name": profile["name"],
                "mode": profile["mode"],
            },
            "filters": {
                "search": search,
                "protocol": protocol_value,
                "limit": clamped_limit,
            },
            "stats": {
                "total_nodes": len(nodes),
                "matched_nodes": len(filtered_nodes),
                "returned_nodes": len(items),
                "truncated": len(filtered_nodes) > clamped_limit,
                "protocol_counts": protocol_counts,
            },
            "items": items,
        }

    def build_merged_subscription(self, profile_id: int, format_name: str = "base64") -> str:
        nodes = self.get_nodes_for_profile(profile_id)
        if format_name not in SUPPORTED_EXPORT_FORMATS:
            raise ValueError("Unsupported subscription format.")
        profile = self.get_profile(profile_id)
        return build_export(
            nodes,
            profile=profile,
            format_name=format_name,
            generated_at=to_iso(utc_now()) or "",
        )

    def get_public_subscription(self, token: str, format_name: str = "base64") -> str:
        with self._database() as connection:
            row = connection.execute(
                "SELECT id FROM merge_profiles WHERE token = ?",
                (token,),
            ).fetchone()
        if row is None:
            raise PermissionError("Invalid subscription token.")
        payload = self.build_merged_subscription(row["id"], format_name=format_name)
        accessed_at = to_iso(utc_now())
        with self._database() as connection:
            connection.execute(
                """
                UPDATE merge_profiles
                SET access_count = access_count + 1
                WHERE id = ?
                """,
                (row["id"],),
            )
            connection.execute(
                """
                INSERT INTO profile_access_logs (profile_id, format_name, accessed_at)
                VALUES (?, ?, ?)
                """,
                (row["id"], format_name, accessed_at),
            )
        return payload

    def get_dashboard_state(self, base_url: str) -> dict[str, Any]:
        groups = self.list_groups()
        subscriptions = self.list_subscriptions()
        profiles = self.list_profiles(base_url)
        default_profile = self.get_default_profile()
        alerts = self.get_health_alerts()

        with self._database() as connection:
            cached = connection.execute("SELECT COUNT(*) AS count FROM nodes").fetchone()
            enabled = connection.execute(
                "SELECT COUNT(*) AS count FROM subscriptions WHERE enabled = 1"
            ).fetchone()
            refresh_totals = connection.execute(
                "SELECT COALESCE(SUM(refresh_count), 0) AS count FROM subscriptions"
            ).fetchone()
            access_totals = connection.execute(
                "SELECT COALESCE(SUM(access_count), 0) AS count FROM merge_profiles"
            ).fetchone()
            refresh_window_stats = self._refresh_log_window_stats(connection)
            access_window_stats = self._profile_access_window_stats(connection)

        return {
            "stats": {
                "subscriptions": len(subscriptions),
                "enabled_subscriptions": enabled["count"],
                "groups": len(groups),
                "profiles": len(profiles),
                "cached_nodes": cached["count"],
                "default_merged_nodes": len(self.get_nodes_for_profile(default_profile["id"])),
                "alerts": len(alerts),
                "subscription_refreshes": refresh_totals["count"],
                "subscription_refreshes_24h": refresh_window_stats["refresh_count_24h"],
                "subscription_refreshes_7d": refresh_window_stats["refresh_count_7d"],
                "profile_accesses": access_totals["count"],
                "profile_accesses_24h": access_window_stats["access_count_24h"],
                "profile_accesses_7d": access_window_stats["access_count_7d"],
            },
            "overview": {
                "default_profile_id": default_profile["id"],
            },
            "settings": {
                **self.get_advanced_filter_settings(),
                "global_exclude_keywords": self.get_exclude_keywords(),
                "default_theme": self.get_default_theme(),
                "panel_port": self.get_panel_port(),
                "schema": self.get_schema_status(),
                "cleanup": self.get_cleanup_settings(),
                "notifications": self.get_notification_settings(),
            },
            "alerts": alerts,
            "groups": groups,
            "subscriptions": subscriptions,
            "profiles": profiles,
        }

    def apply_automatic_maintenance(self) -> dict[str, int]:
        settings = self.get_cleanup_settings()
        actions = {"disabled_expired": 0, "paused_failures": 0}
        subscriptions = self.list_subscriptions()
        for item in subscriptions:
            if not item["enabled"]:
                continue
            if settings["auto_disable_expired"] and item["is_expired"]:
                self.set_subscription_enabled(item["id"], False)
                actions["disabled_expired"] += 1
                continue
            if (
                item["source_type"] == "remote"
                and settings["pause_failures_threshold"] > 0
                and item["consecutive_failures"] >= settings["pause_failures_threshold"]
            ):
                self.set_subscription_enabled(item["id"], False)
                actions["paused_failures"] += 1
        return actions

    def get_health_alerts(self) -> list[dict[str, Any]]:
        alerts: list[dict[str, Any]] = []
        now = utc_now()
        subscriptions = self.list_subscriptions()

        for item in subscriptions:
            if item["is_expired"]:
                alerts.append(
                    {
                        "severity": "danger",
                        "type": "expired",
                        "subscription_id": item["id"],
                        "title": f"{item['name']} 已到期",
                        "detail": "建议尽快续费或禁用，避免主订阅里长期保留失效来源。",
                    }
                )

            if item["source_type"] == "remote" and item["enabled"] and item["consecutive_failures"] >= 3:
                alerts.append(
                    {
                        "severity": "danger" if item["consecutive_failures"] >= 5 else "warning",
                        "type": "refresh_failures",
                        "subscription_id": item["id"],
                        "title": f"{item['name']} 连续刷新失败 {item['consecutive_failures']} 次",
                        "detail": item["last_error"] or "请检查订阅地址、网络连通性或机场状态。",
                    }
                )

            if item["enabled"] and item["expires_at"] and not item["is_expired"]:
                expires_at = self._parse_datetime(item["expires_at"])
                if expires_at is not None:
                    remaining = expires_at - now
                    if remaining <= timedelta(days=7):
                        alerts.append(
                            {
                                "severity": "warning",
                                "type": "expiring_soon",
                                "subscription_id": item["id"],
                                "title": f"{item['name']} 即将到期",
                                "detail": f"到期时间：{item['expires_at']}",
                            }
                        )

            if item["source_type"] == "remote" and item["enabled"] and item["last_updated_at"]:
                last_updated = self._parse_datetime(item["last_updated_at"])
                if last_updated is not None:
                    stale_after = timedelta(hours=max(item["refresh_interval_hours"] * 2, 24))
                    if now - last_updated > stale_after:
                        alerts.append(
                            {
                                "severity": "warning",
                                "type": "stale_cache",
                                "subscription_id": item["id"],
                                "title": f"{item['name']} 的节点缓存较旧",
                                "detail": f"上次成功更新时间：{item['last_updated_at']}",
                            }
                        )

            if item["enabled"] and item["last_status"] == "ok" and item["node_count"] == 0:
                alerts.append(
                    {
                        "severity": "warning",
                        "type": "empty_nodes",
                        "subscription_id": item["id"],
                        "title": f"{item['name']} 当前没有节点",
                        "detail": "订阅抓取成功但节点数量为 0，请确认机场订阅内容是否正常。",
                    }
                )

        alerts.sort(key=self._alert_sort_key)
        return alerts[:24]

    def dispatch_health_notifications(self, *, force: bool = False, test_message: str = "") -> dict[str, Any]:
        settings = self.get_notification_settings()
        if not settings["telegram_bot_token"] and not settings["webhook_url"]:
            return {"sent": 0, "candidates": 0, "configured": False}

        now = utc_now()
        if test_message:
            payload = {
                "severity": "warning",
                "type": "test",
                "title": "Lulynx SubHub 测试通知",
                "detail": test_message,
                "sent_at": to_iso(now),
            }
            self._send_notification_payload(payload, settings)
            return {"sent": 1, "candidates": 1, "configured": True}

        threshold = self._notification_severity_rank(settings["min_severity"])
        alerts = [
            alert for alert in self.get_health_alerts()
            if self._notification_severity_rank(alert.get("severity", "warning")) <= threshold
        ]
        if not alerts:
            return {"sent": 0, "candidates": 0, "configured": True}

        with self._database() as connection:
            rows = connection.execute(
                "SELECT alert_key, fingerprint, last_sent_at FROM alert_notifications"
            ).fetchall()
        sent_map = {row["alert_key"]: row for row in rows}
        cooldown = timedelta(minutes=settings["cooldown_minutes"])
        due_alerts: list[tuple[str, str, dict[str, Any]]] = []
        for alert in alerts:
            alert_key = f"{alert.get('type', 'alert')}:{alert.get('subscription_id', 0)}"
            fingerprint = json.dumps(
                {
                    "severity": alert.get("severity"),
                    "title": alert.get("title"),
                    "detail": alert.get("detail"),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            existing = sent_map.get(alert_key)
            if existing is None:
                due_alerts.append((alert_key, fingerprint, alert))
                continue
            last_sent = self._parse_datetime(existing["last_sent_at"])
            if force or existing["fingerprint"] != fingerprint or last_sent is None or now - last_sent >= cooldown:
                due_alerts.append((alert_key, fingerprint, alert))

        for alert_key, fingerprint, alert in due_alerts:
            payload = {
                **alert,
                "sent_at": to_iso(now),
            }
            self._send_notification_payload(payload, settings)
            with self._database() as connection:
                connection.execute(
                    """
                    INSERT INTO alert_notifications (alert_key, fingerprint, last_sent_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(alert_key) DO UPDATE
                    SET fingerprint = excluded.fingerprint,
                        last_sent_at = excluded.last_sent_at
                    """,
                    (alert_key, fingerprint, to_iso(now)),
                )

        return {
            "sent": len(due_alerts),
            "candidates": len(alerts),
            "configured": True,
        }

    def preview_backup_restore(self, backup: dict[str, Any]) -> dict[str, Any]:
        settings, users, groups, subscriptions, nodes, profiles, profile_sources = self._coerce_backup_sections(backup)
        current_counts = self._current_backup_counts()
        backup_counts = {
            "users": len(users),
            "groups": len(groups),
            "subscriptions": len(subscriptions),
            "manual_subscriptions": sum(
                1 for item in subscriptions if str(item.get("source_type", "remote")).strip().lower() == "manual"
            ),
            "nodes": len(nodes),
            "profiles": len(profiles),
            "profile_sources": len(profile_sources),
            "settings": len(settings),
        }
        warnings = [
            "恢复会覆盖当前面板中的订阅、分组、主订阅和节点缓存。",
            "恢复完成后当前登录会话会失效，需要重新登录。",
        ]
        if not users:
            warnings.append("备份里没有管理员账号，恢复后请用后端重置命令重新生成管理员。")
        if not profiles:
            warnings.append("备份里没有主订阅，恢复后系统会自动补一个默认主订阅。")
        if not subscriptions:
            warnings.append("备份里没有订阅源，恢复后面板会是空库状态。")
        return {
            "valid": True,
            "version": int(backup.get("version", 0)),
            "backup_counts": backup_counts,
            "current_counts": current_counts,
            "warnings": warnings,
        }

    def export_backup(self) -> dict[str, Any]:
        with self._database() as connection:
            return {
                "version": 3,
                "exported_at": to_iso(utc_now()),
                "settings": {
                    row["key"]: row["value"]
                    for row in connection.execute(
                        "SELECT key, value FROM settings ORDER BY key ASC"
                    ).fetchall()
                    if row["key"] != "session_secret"
                },
                "users": [
                    _row_to_dict(row)
                    for row in connection.execute(
                        "SELECT id, username, password_hash, password_salt, created_at FROM users ORDER BY id ASC"
                    ).fetchall()
                ],
                "groups": [
                    _row_to_dict(row)
                    for row in connection.execute(
                        """
                        SELECT id, name, description, color, created_at, updated_at
                        FROM subscription_groups
                        ORDER BY id ASC
                        """
                    ).fetchall()
                ],
                "subscriptions": [
                    _row_to_dict(row)
                    for row in connection.execute(
                        """
                        SELECT id, name, url, source_type, manual_content, group_id, expires_at,
                               enabled, refresh_interval_hours, last_status, consecutive_failures,
                               last_error, last_updated_at, next_refresh_at, node_count, refresh_count, source_format,
                               created_at, updated_at
                        FROM subscriptions
                        ORDER BY id ASC
                        """
                    ).fetchall()
                ],
                "nodes": [
                    _row_to_dict(row)
                    for row in connection.execute(
                        """
                        SELECT id, subscription_id, uri, name, protocol, sort_order, created_at
                        FROM nodes
                        ORDER BY subscription_id ASC, sort_order ASC, id ASC
                        """
                    ).fetchall()
                ],
                "profiles": [
                    _row_to_dict(row)
                    for row in connection.execute(
                        """
                        SELECT id, name, description, token, exclude_keywords, exclude_protocols, include_all, access_count, created_at, updated_at
                        FROM merge_profiles
                        ORDER BY id ASC
                        """
                    ).fetchall()
                ],
                "profile_sources": [
                    _row_to_dict(row)
                    for row in connection.execute(
                        """
                        SELECT profile_id, subscription_id, sort_order
                        FROM merge_profile_sources
                        ORDER BY profile_id ASC, sort_order ASC, subscription_id ASC
                        """
                    ).fetchall()
                ],
            }

    def restore_backup(self, backup: dict[str, Any]) -> dict[str, Any]:
        settings, users, groups, subscriptions, nodes, profiles, profile_sources = self._coerce_backup_sections(backup)

        with self._database() as connection:
            connection.execute("DELETE FROM alert_notifications")
            connection.execute("DELETE FROM merge_profile_sources")
            connection.execute("DELETE FROM nodes")
            connection.execute("DELETE FROM merge_profiles")
            connection.execute("DELETE FROM subscriptions")
            connection.execute("DELETE FROM subscription_groups")
            connection.execute("DELETE FROM users")
            connection.execute("DELETE FROM settings")

            for key, value in settings.items():
                if key == "session_secret":
                    continue
                connection.execute(
                    "INSERT INTO settings (key, value) VALUES (?, ?)",
                    (str(key), str(value)),
                )

            for user in users:
                connection.execute(
                    """
                    INSERT INTO users (id, username, password_hash, password_salt, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        int(user["id"]),
                        str(user["username"]),
                        str(user["password_hash"]),
                        str(user["password_salt"]),
                        str(user["created_at"]),
                    ),
                )

            for group in groups:
                connection.execute(
                    """
                    INSERT INTO subscription_groups (id, name, description, color, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(group["id"]),
                        str(group["name"]),
                        str(group.get("description", "")),
                        str(group.get("color", "#0c8d8a")),
                        str(group["created_at"]),
                        str(group.get("updated_at", group["created_at"])),
                    ),
                )

            for subscription in subscriptions:
                connection.execute(
                    """
                    INSERT INTO subscriptions (
                        id, name, url, source_type, manual_content, group_id, expires_at, enabled,
                        refresh_interval_hours, last_status, consecutive_failures, last_error,
                        last_updated_at, next_refresh_at, node_count, refresh_count, source_format, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(subscription["id"]),
                        str(subscription["name"]),
                        str(subscription["url"]),
                        str(subscription.get("source_type", "remote")),
                        str(subscription.get("manual_content", "")),
                        subscription.get("group_id"),
                        subscription.get("expires_at"),
                        int(bool(subscription.get("enabled", 1))),
                        int(subscription.get("refresh_interval_hours", 24)),
                        str(subscription.get("last_status", "idle")),
                        int(subscription.get("consecutive_failures", 0)),
                        subscription.get("last_error"),
                        subscription.get("last_updated_at"),
                        subscription.get("next_refresh_at"),
                        int(subscription.get("node_count", 0)),
                        int(subscription.get("refresh_count", 0)),
                        subscription.get("source_format"),
                        str(subscription.get("created_at", to_iso(utc_now()))),
                        str(subscription.get("updated_at", to_iso(utc_now()))),
                    ),
                )

            for node in nodes:
                connection.execute(
                    """
                    INSERT INTO nodes (id, subscription_id, uri, name, protocol, sort_order, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(node["id"]),
                        int(node["subscription_id"]),
                        str(node["uri"]),
                        str(node["name"]),
                        str(node["protocol"]),
                        int(node.get("sort_order", 0)),
                        str(node.get("created_at", to_iso(utc_now()))),
                    ),
                )

            for profile in profiles:
                connection.execute(
                    """
                    INSERT INTO merge_profiles (
                        id, name, description, token, exclude_keywords, exclude_protocols, include_all, access_count, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(profile["id"]),
                        str(profile["name"]),
                        str(profile.get("description", "")),
                        str(profile["token"]),
                        str(profile.get("exclude_keywords", "")),
                        str(profile.get("exclude_protocols", "")),
                        int(bool(profile.get("include_all", 0))),
                        int(profile.get("access_count", 0)),
                        str(profile.get("created_at", to_iso(utc_now()))),
                        str(profile.get("updated_at", to_iso(utc_now()))),
                    ),
                )

            for link in profile_sources:
                connection.execute(
                    """
                    INSERT INTO merge_profile_sources (profile_id, subscription_id, sort_order)
                    VALUES (?, ?, ?)
                    """,
                    (
                        int(link["profile_id"]),
                        int(link["subscription_id"]),
                        int(link.get("sort_order", 0)),
                    ),
                )

        self._ensure_runtime_settings()
        self._ensure_default_profile()
        return {
            "restored_at": to_iso(utc_now()),
            "counts": {
                "users": len(users),
                "groups": len(groups),
                "subscriptions": len(subscriptions),
                "nodes": len(nodes),
                "profiles": len(profiles),
            },
        }

    def _coerce_backup_sections(
        self,
        backup: dict[str, Any],
    ) -> tuple[dict[str, Any], list[Any], list[Any], list[Any], list[Any], list[Any], list[Any]]:
        if not isinstance(backup, dict):
            raise ValueError("Backup payload must be an object.")
        if int(backup.get("version", 0)) not in {1, 2, 3}:
            raise ValueError("Unsupported backup version.")
        settings = backup.get("settings", {})
        users = backup.get("users", [])
        groups = backup.get("groups", [])
        subscriptions = backup.get("subscriptions", [])
        nodes = backup.get("nodes", [])
        profiles = backup.get("profiles", [])
        profile_sources = backup.get("profile_sources", [])
        if not isinstance(settings, dict):
            raise ValueError("Invalid backup: settings must be an object.")
        for key, value in (
            ("users", users),
            ("groups", groups),
            ("subscriptions", subscriptions),
            ("nodes", nodes),
            ("profiles", profiles),
            ("profile_sources", profile_sources),
        ):
            if not isinstance(value, list):
                raise ValueError(f"Invalid backup: {key} must be a list.")
        return settings, users, groups, subscriptions, nodes, profiles, profile_sources

    def _current_backup_counts(self) -> dict[str, int]:
        with self._database() as connection:
            return {
                "users": connection.execute("SELECT COUNT(*) AS count FROM users").fetchone()["count"],
                "groups": connection.execute("SELECT COUNT(*) AS count FROM subscription_groups").fetchone()["count"],
                "subscriptions": connection.execute("SELECT COUNT(*) AS count FROM subscriptions").fetchone()["count"],
                "nodes": connection.execute("SELECT COUNT(*) AS count FROM nodes").fetchone()["count"],
                "profiles": connection.execute("SELECT COUNT(*) AS count FROM merge_profiles").fetchone()["count"],
            }

    def _download_subscription(self, url: str) -> bytes:
        request = Request(
            url,
            headers={
                "User-Agent": "LulynxSubHub/2.0",
                "Accept": "text/plain, */*",
            },
        )
        with urlopen(request, timeout=20) as response:
            return response.read()

    def _load_subscription_payload_bytes(self, source_type: str, url: str, manual_content: str) -> bytes:
        if source_type == "manual":
            return str(manual_content or "").encode("utf-8")
        return self._download_subscription(url)

    def _load_subscription_payload(self, source_type: str, url: str, manual_content: str) -> Any:
        payload = self._load_subscription_payload_bytes(source_type, url, manual_content)
        return parse_subscription_payload(payload)

    def _send_notification_payload(self, payload: dict[str, Any], settings: dict[str, Any]) -> None:
        message = self._format_notification_message(payload)
        telegram_bot_token = settings.get("telegram_bot_token", "")
        telegram_chat_id = settings.get("telegram_chat_id", "")
        webhook_url = settings.get("webhook_url", "")
        delivered = False
        errors: list[str] = []
        if telegram_bot_token and telegram_chat_id:
            try:
                self._send_telegram_message(message, telegram_bot_token, telegram_chat_id)
                delivered = True
            except Exception as exc:
                errors.append(f"telegram: {exc}")
        if webhook_url:
            try:
                self._send_webhook_payload(payload, webhook_url)
                delivered = True
            except Exception as exc:
                errors.append(f"webhook: {exc}")
        if not delivered:
            raise RuntimeError("; ".join(errors) or "No notification channel is configured.")

    def _send_telegram_message(self, message: str, bot_token: str, chat_id: str) -> None:
        request = Request(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            data=json.dumps({"chat_id": chat_id, "text": message}).encode("utf-8"),
            headers={
                "User-Agent": "LulynxSubHub/2.0",
                "Content-Type": "application/json; charset=utf-8",
            },
            method="POST",
        )
        with urlopen(request, timeout=20) as response:
            response.read()

    def _send_webhook_payload(self, payload: dict[str, Any], webhook_url: str) -> None:
        request = Request(
            webhook_url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "User-Agent": "LulynxSubHub/2.0",
                "Content-Type": "application/json; charset=utf-8",
            },
            method="POST",
        )
        with urlopen(request, timeout=20) as response:
            response.read()

    def _format_notification_message(self, payload: dict[str, Any]) -> str:
        severity = "高优先级" if payload.get("severity") == "danger" else "提醒"
        detail = str(payload.get("detail") or "").strip()
        parts = [
            "Lulynx SubHub 健康提醒",
            f"级别：{severity}",
            f"标题：{payload.get('title', '')}",
        ]
        if detail:
            parts.append(f"详情：{detail}")
        if payload.get("sent_at"):
            parts.append(f"时间：{payload['sent_at']}")
        return "\n".join(parts)

    def _failure_backoff(self, refresh_interval_hours: int) -> timedelta:
        minutes = min(60, max(10, refresh_interval_hours * 15))
        return timedelta(minutes=minutes)

    def _normalize_group_id(self, group_id: int | None) -> int | None:
        if group_id in (None, "", 0, "0"):
            return None
        try:
            normalized_group_id = int(group_id)
        except (TypeError, ValueError) as exc:
            raise ValueError("Invalid group id.") from exc

        with self._database() as connection:
            row = connection.execute(
                "SELECT id FROM subscription_groups WHERE id = ?",
                (normalized_group_id,),
            ).fetchone()
        if row is None:
            raise ValueError("Group not found.")
        return normalized_group_id

    def _normalize_subscription_ids(self, subscription_ids: Iterable[int]) -> list[int]:
        normalized_ids: list[int] = []
        seen: set[int] = set()
        for raw_id in subscription_ids:
            try:
                subscription_id = int(raw_id)
            except (TypeError, ValueError) as exc:
                raise ValueError("Invalid subscription id.") from exc
            if subscription_id in seen:
                continue
            seen.add(subscription_id)
            normalized_ids.append(subscription_id)

        if not normalized_ids:
            return []

        with self._database() as connection:
            rows = connection.execute(
                f"SELECT id FROM subscriptions WHERE id IN ({', '.join('?' for _ in normalized_ids)})",
                normalized_ids,
            ).fetchall()
        existing_ids = {row["id"] for row in rows}
        missing = [subscription_id for subscription_id in normalized_ids if subscription_id not in existing_ids]
        if missing:
            raise ValueError("One or more selected subscriptions do not exist.")
        return normalized_ids

    def _replace_profile_sources(
        self,
        connection: sqlite3.Connection,
        profile_id: int,
        subscription_ids: Iterable[int],
    ) -> None:
        connection.execute(
            "DELETE FROM merge_profile_sources WHERE profile_id = ?",
            (profile_id,),
        )
        rows = [
            (profile_id, subscription_id, sort_order)
            for sort_order, subscription_id in enumerate(subscription_ids)
        ]
        if rows:
            connection.executemany(
                """
                INSERT INTO merge_profile_sources (profile_id, subscription_id, sort_order)
                VALUES (?, ?, ?)
                """,
                rows,
            )

    def _profile_source_map(self, connection: sqlite3.Connection) -> dict[int, list[int]]:
        rows = connection.execute(
            """
            SELECT profile_id, subscription_id
            FROM merge_profile_sources
            ORDER BY profile_id ASC, sort_order ASC, subscription_id ASC
            """
        ).fetchall()
        mapping: dict[int, list[int]] = {}
        for row in rows:
            mapping.setdefault(row["profile_id"], []).append(row["subscription_id"])
        return mapping

    def _enabled_subscription_ids(self, connection: sqlite3.Connection) -> list[int]:
        rows = connection.execute(
            """
            SELECT id
            FROM subscriptions
            WHERE enabled = 1
            ORDER BY created_at ASC, id ASC
            """
        ).fetchall()
        return [int(row["id"]) for row in rows]

    def _enabled_subscription_count(self) -> int:
        with self._database() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM subscriptions WHERE enabled = 1"
            ).fetchone()
        return row["count"]

    def _enabled_subscription_stats(self, connection: sqlite3.Connection) -> dict[str, int]:
        row = connection.execute(
            """
            SELECT COUNT(*) AS count,
                   COALESCE(SUM(refresh_count), 0) AS refresh_count
            FROM subscriptions
            WHERE enabled = 1
            """
        ).fetchone()
        return {
            "count": int(row["count"] or 0),
            "refresh_count": int(row["refresh_count"] or 0),
        }

    def _latest_refresh_log_map(
        self,
        connection: sqlite3.Connection,
        *,
        subscription_id: int | None = None,
    ) -> dict[int, dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if subscription_id is not None:
            clauses.append("subscription_id = ?")
            params.append(subscription_id)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = connection.execute(
            f"""
            SELECT l.*
            FROM subscription_refresh_logs l
            INNER JOIN (
                SELECT subscription_id, MAX(id) AS max_id
                FROM subscription_refresh_logs
                {where_clause}
                GROUP BY subscription_id
            ) latest ON latest.max_id = l.id
            ORDER BY l.subscription_id ASC
            """,
            params,
        ).fetchall()
        result: dict[int, dict[str, Any]] = {}
        for row in rows:
            item = _row_to_dict(row)
            item["added_sample"] = self._decode_json_list(item.get("added_sample_json"))
            item["removed_sample"] = self._decode_json_list(item.get("removed_sample_json"))
            result[int(item["subscription_id"])] = item
        return result

    def _profile_access_stats_map(
        self,
        connection: sqlite3.Connection,
        *,
        profile_id: int | None = None,
    ) -> dict[int, dict[str, int]]:
        now = utc_now()
        cutoff_24h = to_iso(now - timedelta(days=1))
        cutoff_7d = to_iso(now - timedelta(days=7))
        clauses: list[str] = []
        params: list[Any] = [cutoff_24h, cutoff_7d]
        if profile_id is not None:
            clauses.append("profile_id = ?")
            params.append(profile_id)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = connection.execute(
            f"""
            SELECT profile_id,
                   SUM(CASE WHEN accessed_at >= ? THEN 1 ELSE 0 END) AS access_count_24h,
                   SUM(CASE WHEN accessed_at >= ? THEN 1 ELSE 0 END) AS access_count_7d
            FROM profile_access_logs
            {where_clause}
            GROUP BY profile_id
            ORDER BY profile_id ASC
            """,
            params,
        ).fetchall()
        return {
            int(row["profile_id"]): {
                "access_count_24h": int(row["access_count_24h"] or 0),
                "access_count_7d": int(row["access_count_7d"] or 0),
            }
            for row in rows
        }

    def _profile_access_window_stats(self, connection: sqlite3.Connection) -> dict[str, int]:
        now = utc_now()
        cutoff_24h = to_iso(now - timedelta(days=1))
        cutoff_7d = to_iso(now - timedelta(days=7))
        row = connection.execute(
            """
            SELECT SUM(CASE WHEN accessed_at >= ? THEN 1 ELSE 0 END) AS access_count_24h,
                   SUM(CASE WHEN accessed_at >= ? THEN 1 ELSE 0 END) AS access_count_7d
            FROM profile_access_logs
            """,
            (cutoff_24h, cutoff_7d),
        ).fetchone()
        return {
            "access_count_24h": int(row["access_count_24h"] or 0),
            "access_count_7d": int(row["access_count_7d"] or 0),
        }

    def _refresh_log_window_stats(self, connection: sqlite3.Connection) -> dict[str, int]:
        now = utc_now()
        cutoff_24h = to_iso(now - timedelta(days=1))
        cutoff_7d = to_iso(now - timedelta(days=7))
        row = connection.execute(
            """
            SELECT SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS refresh_count_24h,
                   SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS refresh_count_7d
            FROM subscription_refresh_logs
            WHERE status = 'ok'
            """,
            (cutoff_24h, cutoff_7d),
        ).fetchone()
        return {
            "refresh_count_24h": int(row["refresh_count_24h"] or 0),
            "refresh_count_7d": int(row["refresh_count_7d"] or 0),
        }

    def _subscription_refresh_count_map(self, connection: sqlite3.Connection) -> dict[int, int]:
        rows = connection.execute(
            "SELECT id, refresh_count FROM subscriptions ORDER BY id ASC"
        ).fetchall()
        return {int(row["id"]): int(row["refresh_count"] or 0) for row in rows}

    def _alert_sort_key(self, alert: dict[str, Any]) -> tuple[int, str]:
        return (self._notification_severity_rank(alert.get("severity", "info")), alert.get("title", ""))

    def _insert_refresh_log(
        self,
        connection: sqlite3.Connection,
        *,
        subscription_id: int,
        trigger: str,
        status: str,
        started_at: datetime,
        finished_at: datetime,
        node_count_before: int,
        node_count_after: int,
        added_sample: list[str],
        removed_sample: list[str],
        source_format: str,
        error_message: str | None = None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO subscription_refresh_logs (
                subscription_id, trigger, status, started_at, finished_at, duration_ms,
                node_count_before, node_count_after, added_count, removed_count, source_format,
                error_message, added_sample_json, removed_sample_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                subscription_id,
                str(trigger or "manual"),
                status,
                to_iso(started_at),
                to_iso(finished_at),
                max(0, int((finished_at - started_at).total_seconds() * 1000)),
                max(0, int(node_count_before)),
                max(0, int(node_count_after)),
                max(0, len(added_sample)),
                max(0, len(removed_sample)),
                source_format or None,
                error_message,
                json.dumps(added_sample[:8], ensure_ascii=False),
                json.dumps(removed_sample[:8], ensure_ascii=False),
                to_iso(finished_at),
            ),
        )

    def _decode_json_list(self, raw_value: Any) -> list[str]:
        if not raw_value:
            return []
        try:
            decoded = json.loads(str(raw_value))
        except (TypeError, ValueError):
            return []
        if not isinstance(decoded, list):
            return []
        return [str(item) for item in decoded[:8]]

    def _protocol_set(self, raw_protocols: str) -> set[str]:
        return {item.strip().lower() for item in split_keywords(raw_protocols) if item.strip()}

    def _parse_rename_rules(self, raw_rules: str) -> list[tuple[re.Pattern[str], str]]:
        rules: list[tuple[re.Pattern[str], str]] = []
        for line in str(raw_rules or "").splitlines():
            candidate = line.strip()
            if not candidate or "=>" not in candidate:
                continue
            pattern, replacement = [part.strip() for part in candidate.split("=>", 1)]
            if not pattern:
                continue
            try:
                rules.append((re.compile(pattern, re.IGNORECASE), replacement))
            except re.error:
                continue
        return rules

    def _apply_rename_rules(self, name: str, rules: list[tuple[re.Pattern[str], str]]) -> str:
        updated = str(name or "")
        for pattern, replacement in rules:
            updated = pattern.sub(replacement, updated)
        cleaned = " ".join(updated.split())
        return cleaned or name

    def _dedup_key(self, node: NodeEntry, dedup_strategy: str) -> str:
        if dedup_strategy == "name_protocol":
            return f"{node.name.strip().lower()}|{node.protocol.strip().lower()}"
        if dedup_strategy == "name":
            return node.name.strip().lower()
        return node.uri

    def _get_nodes_for_profile_record(self, profile: dict[str, Any]) -> list[NodeEntry]:
        subscription_ids = self._resolve_profile_subscription_ids(profile)
        nodes = self.get_nodes(subscription_ids=subscription_ids, only_enabled=True)
        global_filter_settings = self.get_advanced_filter_settings()
        keywords = split_keywords(global_filter_settings["exclude_keywords"]) + split_keywords(profile["exclude_keywords"])
        protocol_blacklist = self._protocol_set(global_filter_settings["exclude_protocols"]) | self._protocol_set(
            profile.get("exclude_protocols", "")
        )
        filtered_nodes = [
            node for node in filter_nodes(nodes, keywords)
            if node.protocol.lower() not in protocol_blacklist
        ]

        rename_rules = self._parse_rename_rules(global_filter_settings["rename_rules"])
        renamed_nodes = [
            NodeEntry(
                uri=node.uri,
                name=self._apply_rename_rules(node.name, rename_rules),
                protocol=node.protocol,
            )
            for node in filtered_nodes
        ]

        deduped: list[NodeEntry] = []
        seen: set[str] = set()
        dedup_strategy = global_filter_settings["dedup_strategy"]
        for node in renamed_nodes:
            dedup_key = self._dedup_key(node, dedup_strategy)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            deduped.append(node)
        return deduped

    def _resolve_profile_subscription_ids(self, profile: dict[str, Any]) -> list[int] | None:
        selected_ids = [int(subscription_id) for subscription_id in profile.get("selected_subscription_ids", [])]
        if not profile["include_all"]:
            return selected_ids
        with self._database() as connection:
            enabled_ids = self._enabled_subscription_ids(connection)
        prioritized = [subscription_id for subscription_id in selected_ids if subscription_id in enabled_ids]
        remaining = [subscription_id for subscription_id in enabled_ids if subscription_id not in prioritized]
        return prioritized + remaining

    def _protocol_summary_map(self, connection: sqlite3.Connection) -> dict[int, str]:
        rows = connection.execute(
            """
            SELECT subscription_id,
                   GROUP_CONCAT(protocol || ':' || protocol_count, ', ') AS protocols
            FROM (
                SELECT subscription_id, protocol, COUNT(*) AS protocol_count
                FROM nodes
                GROUP BY subscription_id, protocol
                ORDER BY protocol
            )
            GROUP BY subscription_id
            """
        ).fetchall()
        return {row["subscription_id"]: row["protocols"] for row in rows}

    def _default_subscription_name(self, url: str, source_type: str = "remote") -> str:
        if source_type == "manual":
            return "本地手动订阅"
        parsed = urlparse(url)
        if parsed.hostname:
            return parsed.hostname
        return "New subscription"

    def _normalize_source_type(self, source_type: str) -> str:
        normalized = str(source_type or "remote").strip().lower()
        if normalized not in {"remote", "manual"}:
            raise ValueError("Subscription source type must be remote or manual.")
        return normalized

    def _normalize_manual_content(self, manual_content: str) -> str:
        return "\n".join(line.strip() for line in str(manual_content or "").splitlines() if line.strip())

    def _normalize_theme_name(self, theme: str) -> str:
        normalized = str(theme or "classic").strip().lower()
        if normalized not in {"classic", "industrial-light"}:
            return "classic"
        return normalized

    def _normalize_refresh_interval(self, refresh_interval_hours: int) -> int:
        try:
            normalized = int(refresh_interval_hours)
        except (TypeError, ValueError) as exc:
            raise ValueError("Refresh interval must be a whole number of hours.") from exc
        return max(1, min(normalized, 168))

    def _normalize_keywords(self, raw_keywords: str) -> str:
        return "\n".join(split_keywords(raw_keywords))

    def _normalize_protocols(self, raw_protocols: str) -> str:
        normalized: list[str] = []
        seen: set[str] = set()
        for item in split_keywords(raw_protocols):
            protocol = item.strip().lower()
            if protocol not in SUPPORTED_PROTOCOLS:
                continue
            if protocol in seen:
                continue
            seen.add(protocol)
            normalized.append(protocol)
        return "\n".join(normalized)

    def _normalize_dedup_strategy(self, dedup_strategy: str) -> str:
        normalized = str(dedup_strategy or "uri").strip().lower()
        if normalized not in {"uri", "name_protocol", "name"}:
            raise ValueError("Dedup strategy must be uri, name_protocol, or name.")
        return normalized

    def _normalize_rename_rules(self, raw_rules: str) -> str:
        normalized: list[str] = []
        for line in str(raw_rules or "").splitlines():
            candidate = line.strip()
            if not candidate:
                continue
            if "=>" not in candidate:
                raise ValueError("Rename rules must use the format pattern => replacement.")
            pattern, replacement = [part.strip() for part in candidate.split("=>", 1)]
            if not pattern:
                raise ValueError("Rename rule pattern cannot be empty.")
            try:
                re.compile(pattern, re.IGNORECASE)
            except re.error as exc:
                raise ValueError(f"Invalid rename rule regex: {pattern}") from exc
            normalized.append(f"{pattern} => {replacement}")
        return "\n".join(normalized)

    def _normalize_color(self, color: str | None) -> str:
        candidate = (color or "").strip()
        if len(candidate) == 7 and candidate.startswith("#"):
            hex_part = candidate[1:]
            if all(character in "0123456789abcdefABCDEF" for character in hex_part):
                return candidate.lower()
        return "#0c8d8a"

    def _notification_severity_rank(self, severity: str) -> int:
        order = {"danger": 0, "warning": 1, "info": 2}
        return order.get(str(severity or "info").strip().lower(), 9)

    def _is_expired(self, expires_at: str | None) -> bool:
        if not expires_at:
            return False
        try:
            expires = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        except ValueError:
            return False
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=UTC)
        return expires <= utc_now()

    def _parse_datetime(self, raw_value: str | None) -> datetime | None:
        if not raw_value:
            return None
        try:
            value = datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
        except ValueError:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def _hash_password(self, password: str, salt: str | None = None) -> tuple[str, str]:
        salt = salt or secrets.token_hex(16)
        derived = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            bytes.fromhex(salt),
            210000,
        )
        return derived.hex(), salt
