#!/usr/bin/env bash
set -Eeuo pipefail

APP_NAME="Lulynx SubHub"
APP_SLUG="lulynx-subhub"
REPO_OWNER="WhitecrowAurora"
REPO_NAME="Lulynx-SubHub"
DEFAULT_REF="main"

INSTALL_DIR="/opt/${APP_SLUG}"
DATA_DIR="/var/lib/${APP_SLUG}"
SERVICE_NAME="${APP_SLUG}"
SERVICE_USER="${APP_SLUG}"
BIND_HOST="127.0.0.1"
PANEL_PORT="8787"
DB_FILENAME="subpanel.db"
SYSTEMD_ENABLED="1"
AUTO_START="1"
REF="${DEFAULT_REF}"

TMP_DIR=""
SOURCE_DIR=""
PYTHON_BIN=""

log() {
  printf '[install] %s\n' "$*"
}

warn() {
  printf '[warn] %s\n' "$*" >&2
}

die() {
  printf '[error] %s\n' "$*" >&2
  exit 1
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

cleanup() {
  if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  fi
}

trap cleanup EXIT

usage() {
  cat <<'EOF'
Lulynx SubHub 一键安装脚本

用法:
  bash install.sh [选项]
  curl -fsSL https://raw.githubusercontent.com/WhitecrowAurora/Lulynx-SubHub/main/install.sh | bash -s -- [选项]

可选参数:
  --install-dir DIR      安装目录，默认 /opt/lulynx-subhub
  --data-dir DIR         数据目录，默认 /var/lib/lulynx-subhub
  --service-name NAME    systemd 服务名，默认 lulynx-subhub
  --service-user USER    systemd 运行用户，默认 lulynx-subhub
  --bind-host HOST       监听地址，默认 127.0.0.1
  --port PORT            面板端口，默认 8787
  --db-name NAME         数据库文件名，默认 subpanel.db
  --ref REF              安装的 GitHub 分支 / 标签 / 提交，默认 main
  --no-systemd           不创建 systemd 服务，只复制文件并输出手动启动命令
  --skip-start           创建服务但不立即启动
  --help                 显示帮助

说明:
  - 默认安装方式更适合反向代理场景，面板会监听 127.0.0.1。
  - 如果你想直接从公网访问，请显式传入 --bind-host 0.0.0.0。
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --install-dir)
        INSTALL_DIR="${2:-}"
        shift 2
        ;;
      --data-dir)
        DATA_DIR="${2:-}"
        shift 2
        ;;
      --service-name)
        SERVICE_NAME="${2:-}"
        shift 2
        ;;
      --service-user)
        SERVICE_USER="${2:-}"
        shift 2
        ;;
      --bind-host)
        BIND_HOST="${2:-}"
        shift 2
        ;;
      --port)
        PANEL_PORT="${2:-}"
        shift 2
        ;;
      --db-name)
        DB_FILENAME="${2:-}"
        shift 2
        ;;
      --ref)
        REF="${2:-}"
        shift 2
        ;;
      --no-systemd)
        SYSTEMD_ENABLED="0"
        shift
        ;;
      --skip-start)
        AUTO_START="0"
        shift
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        die "未知参数: $1"
        ;;
    esac
  done
}

validate_inputs() {
  [[ -n "${INSTALL_DIR}" ]] || die "安装目录不能为空。"
  [[ -n "${DATA_DIR}" ]] || die "数据目录不能为空。"
  [[ -n "${SERVICE_NAME}" ]] || die "服务名不能为空。"
  [[ -n "${SERVICE_USER}" ]] || die "服务用户不能为空。"
  [[ -n "${BIND_HOST}" ]] || die "监听地址不能为空。"
  [[ -n "${DB_FILENAME}" ]] || die "数据库文件名不能为空。"
  [[ "${PANEL_PORT}" =~ ^[0-9]+$ ]] || die "端口必须是数字。"
  (( PANEL_PORT >= 1 && PANEL_PORT <= 65535 )) || die "端口必须在 1-65535 之间。"
}

ensure_runtime_requirements() {
  have_cmd python3 || die "未找到 python3，请先安装 Python 3.10 或更高版本。"
  PYTHON_BIN="$(command -v python3)"

  if [[ "${SYSTEMD_ENABLED}" == "1" ]] && ! have_cmd systemctl; then
    warn "当前系统没有 systemctl，已自动切换为手动启动模式。"
    SYSTEMD_ENABLED="0"
  fi

  if [[ "$(id -u)" -ne 0 ]]; then
    if [[ "${SYSTEMD_ENABLED}" == "1" ]]; then
      die "创建 systemd 服务需要 root 权限。请使用 root 运行，或加上 --no-systemd。"
    fi
    case "${INSTALL_DIR}" in
      /opt/*|/usr/*|/var/*|/etc/*)
        die "当前不是 root，无法写入 ${INSTALL_DIR}。请改用可写目录，或使用 root。"
        ;;
    esac
    case "${DATA_DIR}" in
      /var/*|/etc/*|/usr/*)
        die "当前不是 root，无法写入 ${DATA_DIR}。请改用可写目录，或使用 root。"
        ;;
    esac
  fi
}

discover_local_source_dir() {
  local script_dir
  if [[ -n "${BASH_SOURCE[0]:-}" ]]; then
    script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
    if [[ -f "${script_dir}/app.py" && -f "${script_dir}/manager.py" && -d "${script_dir}/static" ]]; then
      printf '%s\n' "${script_dir}"
      return 0
    fi
  fi
  return 1
}

download_source_dir() {
  local archive_url archive_path extracted_dir

  if have_cmd curl; then
    :
  elif have_cmd wget; then
    :
  else
    die "远程安装需要 curl 或 wget。"
  fi
  have_cmd tar || die "远程安装需要 tar。"

  TMP_DIR="$(mktemp -d)"
  archive_path="${TMP_DIR}/source.tar.gz"
  archive_url="https://codeload.github.com/${REPO_OWNER}/${REPO_NAME}/tar.gz/${REF}"

  log "正在从 GitHub 下载 ${REPO_OWNER}/${REPO_NAME}@${REF} ..."
  if have_cmd curl; then
    curl -fsSL "${archive_url}" -o "${archive_path}"
  else
    wget -qO "${archive_path}" "${archive_url}"
  fi

  tar -xzf "${archive_path}" -C "${TMP_DIR}"
  extracted_dir="$(find "${TMP_DIR}" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
  [[ -n "${extracted_dir}" ]] || die "下载成功，但未能解压出源码目录。"
  printf '%s\n' "${extracted_dir}"
}

resolve_source_dir() {
  if SOURCE_DIR="$(discover_local_source_dir)"; then
    log "检测到本地源码目录: ${SOURCE_DIR}"
    return
  fi
  SOURCE_DIR="$(download_source_dir)"
  log "已使用远程源码目录: ${SOURCE_DIR}"
}

prepare_directories() {
  mkdir -p "${INSTALL_DIR}" "${INSTALL_DIR}/static" "${INSTALL_DIR}/examples" "${DATA_DIR}"
}

copy_runtime_files() {
  local file

  for file in app.py manager.py parsers.py exporters.py README.md LICENSE; do
    [[ -f "${SOURCE_DIR}/${file}" ]] || die "缺少源码文件: ${file}"
    install -m 0644 "${SOURCE_DIR}/${file}" "${INSTALL_DIR}/${file}"
  done

  [[ -f "${SOURCE_DIR}/install.sh" ]] || die "缺少安装脚本 install.sh"
  install -m 0755 "${SOURCE_DIR}/install.sh" "${INSTALL_DIR}/install.sh"

  cp -a "${SOURCE_DIR}/static/." "${INSTALL_DIR}/static/"
  cp -a "${SOURCE_DIR}/examples/." "${INSTALL_DIR}/examples/"
}

choose_nologin_shell() {
  local shell_path
  for shell_path in /usr/sbin/nologin /sbin/nologin /bin/false; do
    if [[ -x "${shell_path}" ]]; then
      printf '%s\n' "${shell_path}"
      return
    fi
  done
  printf '/bin/false\n'
}

ensure_service_user() {
  local nologin_shell

  if [[ "${SYSTEMD_ENABLED}" != "1" || "$(id -u)" -ne 0 ]]; then
    return
  fi

  if id -u "${SERVICE_USER}" >/dev/null 2>&1; then
    return
  fi

  have_cmd useradd || die "未找到 useradd，无法创建服务用户 ${SERVICE_USER}。"
  nologin_shell="$(choose_nologin_shell)"
  useradd --system --home-dir "${DATA_DIR}" --shell "${nologin_shell}" --create-home "${SERVICE_USER}" >/dev/null 2>&1 || \
    useradd --system --home "${DATA_DIR}" --shell "${nologin_shell}" "${SERVICE_USER}"
}

apply_permissions() {
  if [[ "$(id -u)" -ne 0 ]]; then
    return
  fi

  chmod -R a+rX "${INSTALL_DIR}"
  if [[ "${SYSTEMD_ENABLED}" == "1" ]]; then
    chown -R root:root "${INSTALL_DIR}"
    chown -R "${SERVICE_USER}:${SERVICE_USER}" "${DATA_DIR}"
    chmod 0750 "${DATA_DIR}"
  fi
}

write_systemd_service() {
  local service_file db_path
  service_file="/etc/systemd/system/${SERVICE_NAME}.service"
  db_path="${DATA_DIR}/${DB_FILENAME}"

  cat > "${service_file}" <<EOF
[Unit]
Description=${APP_NAME}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${INSTALL_DIR}
ExecStart=${PYTHON_BIN} ${INSTALL_DIR}/app.py --host ${BIND_HOST} --port ${PANEL_PORT} --db ${db_path}
Restart=on-failure
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable "${SERVICE_NAME}" >/dev/null
  if [[ "${AUTO_START}" == "1" ]]; then
    systemctl restart "${SERVICE_NAME}"
    systemctl is-active --quiet "${SERVICE_NAME}" || die "服务已创建，但启动失败。请执行 journalctl -u ${SERVICE_NAME} -n 100 查看日志。"
  fi
}

print_summary() {
  local db_path service_file access_hint ip_hint

  db_path="${DATA_DIR}/${DB_FILENAME}"
  service_file="/etc/systemd/system/${SERVICE_NAME}.service"

  printf '\n'
  log "${APP_NAME} 安装完成。"
  printf '安装目录: %s\n' "${INSTALL_DIR}"
  printf '数据目录: %s\n' "${DATA_DIR}"
  printf '数据库路径: %s\n' "${db_path}"
  printf '监听地址: %s:%s\n' "${BIND_HOST}" "${PANEL_PORT}"

  if [[ "${SYSTEMD_ENABLED}" == "1" ]]; then
    printf 'systemd 服务: %s\n' "${SERVICE_NAME}"
    printf '服务文件: %s\n' "${service_file}"
    printf '常用命令:\n'
    printf '  systemctl status %s\n' "${SERVICE_NAME}"
    printf '  journalctl -u %s -f\n' "${SERVICE_NAME}"
  else
    printf '手动启动命令:\n'
    printf '  cd %s && %s app.py --host %s --port %s --db %s\n' "${INSTALL_DIR}" "${PYTHON_BIN}" "${BIND_HOST}" "${PANEL_PORT}" "${db_path}"
  fi

  if [[ "${BIND_HOST}" == "127.0.0.1" ]]; then
    printf '本机健康检查:\n'
    printf '  curl http://127.0.0.1:%s/healthz\n' "${PANEL_PORT}"
    printf '初始化入口:\n'
    printf '  通过反向代理后的域名访问 /setup，或先在本机打开 http://127.0.0.1:%s/setup\n' "${PANEL_PORT}"
  else
    ip_hint="$(hostname -I 2>/dev/null | awk '{print $1}')"
    printf '初始化入口:\n'
    if [[ -n "${ip_hint}" ]]; then
      printf '  http://%s:%s/setup\n' "${ip_hint}" "${PANEL_PORT}"
    fi
    printf '  http://<你的服务器IP>:%s/setup\n' "${PANEL_PORT}"
  fi

  printf '提示:\n'
  printf '  - 首次打开会进入 Web 初始化向导。\n'
  printf '  - 如果你要走 Nginx 反代，建议保持监听 127.0.0.1。\n'
  printf '  - 当前前端不支持部署在 /sub/ 这类子路径，建议直接挂在域名根路径。\n'
}

main() {
  parse_args "$@"
  validate_inputs
  ensure_runtime_requirements
  resolve_source_dir
  prepare_directories
  copy_runtime_files
  ensure_service_user
  apply_permissions

  if [[ "${SYSTEMD_ENABLED}" == "1" ]]; then
    write_systemd_service
  fi

  print_summary
}

main "$@"
