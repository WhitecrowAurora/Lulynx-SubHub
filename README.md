# Lulynx SubHub

Lulynx SubHub 是一个自托管的代理订阅聚合面板，用来集中管理多个远程订阅、本地手动节点和多个主订阅输出。

它适合这类场景：

- 手里有多个机场订阅，需要统一管理
- 想按设备、地区、用途拆分多个主订阅
- 想自动刷新、过滤、去重、重命名节点
- 想把最终结果导出成 Base64、Clash、Surge、sing-box 等格式
- 想要一个不依赖额外运行时依赖、直接用 Python 标准库就能跑起来的面板

## 主要特性

- 首次启动自带 Web 初始化向导
- 管理多个远程订阅链接
- 支持本地手动节点，一行一个 URI
- 订阅支持分组、到期时间、禁用、删除、手动刷新
- 添加前可预检订阅，先查看解析结果
- 内置倒计时轮询刷新，不依赖 `cron`
- 支持创建多个主订阅
- 主订阅可选择特定来源，或合并全部启用订阅
- 支持将指定来源在主订阅结果中置顶
- 支持关键词过滤、协议排除、去重策略和节点重命名规则
- 支持节点预览、搜索、协议筛选
- 支持刷新日志、访问统计、订阅健康提醒
- 支持 Telegram / Webhook 通知
- 支持备份导出、恢复预检和一键恢复
- 支持数据库迁移命令
- 提供 Linux 一键安装脚本

## 支持协议

- `ss`
- `ssr`
- `vmess`
- `vless`
- `trojan`
- `hy2`
- `hysteria2`
- `anytls`

## 一键安装

如果你想直接从 GitHub 安装，可以执行：

```bash
curl -fsSL https://raw.githubusercontent.com/WhitecrowAurora/Lulynx-SubHub/main/install.sh | bash
```

上面这条命令默认会：

- 从 GitHub 下载 `main` 分支源码
- 安装到 `/opt/lulynx-subhub`
- 数据目录放到 `/var/lib/lulynx-subhub`
- 创建 `lulynx-subhub` systemd 服务
- 默认监听 `127.0.0.1:8787`

如果你希望直接从公网访问，而不是先走 Nginx 反代，可以这样：

```bash
curl -fsSL https://raw.githubusercontent.com/WhitecrowAurora/Lulynx-SubHub/main/install.sh | bash -s -- --bind-host 0.0.0.0 --port 8787
```

如果你已经把仓库克隆到本地，也可以直接在仓库目录执行：

```bash
bash install.sh
```

安装脚本支持这些常用参数：

- `--install-dir DIR` 自定义安装目录
- `--data-dir DIR` 自定义数据目录
- `--service-name NAME` 自定义 systemd 服务名
- `--service-user USER` 自定义服务运行用户
- `--bind-host HOST` 自定义监听地址
- `--port PORT` 自定义端口
- `--db-name NAME` 自定义数据库文件名
- `--ref REF` 指定安装的 GitHub 分支 / 标签 / 提交
- `--no-systemd` 不创建 systemd，只复制文件并输出手动启动命令
- `--skip-start` 创建服务但不立即启动

## 首次初始化

安装完成后，首次访问面板会自动进入初始化向导。

初始化向导目前支持一次性设置：

- 管理员用户名和密码
- 面板监听端口
- 默认主题
- 全局过滤关键词
- 全局排除协议
- 去重策略
- 节点重命名规则
- 自动清理策略

如果安装脚本使用默认监听 `127.0.0.1`，你可以：

- 先通过 Nginx 反代到域名，再访问 `https://你的域名/setup`
- 或在服务器本机访问 `http://127.0.0.1:8787/setup`

## 手动运行

不使用一键安装脚本时，也可以直接运行：

```bash
python app.py --host 0.0.0.0 --port 8787 --db data/subpanel.db
```

默认情况下：

- 监听地址：`127.0.0.1`
- 端口：优先使用面板设置中保存的端口，默认 `8787`
- 数据库：`data/subpanel.db`

## 反向代理说明

推荐把 Lulynx SubHub 挂在域名根路径上，例如：

- `https://sub.example.com/`

当前前端不支持部署到子路径，例如：

- `https://example.com/sub/`

如果使用 Nginx 反代，请保留这些请求头：

- `Host`
- `X-Forwarded-Host`
- `X-Forwarded-Proto`
- `X-Forwarded-For`
- `X-Real-IP`

这样面板生成出来的公开订阅链接才会带上正确的域名和协议。

## 导出格式

每个主订阅都支持以下公开导出地址：

```text
/subscribe/<token>
/subscribe/<token>?format=plain
/subscribe/<token>?format=json
/subscribe/<token>?format=clash
/subscribe/<token>?format=surge
/subscribe/<token>?format=singbox
```

对应格式如下：

- `base64`：常见客户端订阅
- `plain`：便于排查的原始节点文本
- `json`：调试和脚本处理
- `clash`：Clash / Mihomo 代理列表
- `surge`：Surge `[Proxy]` 配置片段
- `singbox`：sing-box `outbounds` JSON

## 常用命令

重置管理员账号：

```bash
python app.py --reset-admin
python app.py --reset-admin --admin-username admin --admin-password new-password-123
```

执行数据库迁移：

```bash
python app.py --migrate-db --db data/subpanel.db
```

如果你是通过一键安装脚本部署的，常用 systemd 命令通常是：

```bash
systemctl status lulynx-subhub
journalctl -u lulynx-subhub -f
```

## 目录结构

```text
install.sh           Linux 一键安装脚本
app.py               HTTP 服务入口
manager.py           SQLite、刷新调度和聚合逻辑
parsers.py           订阅解码与节点解析
exporters.py         多格式导出逻辑
static/              面板页面与前端资源
examples/            批量导入、过滤规则示例
tests/               单元测试
```

## 开发与测试

运行测试：

```bash
python -m unittest discover -s tests -v
```

仓库内附带这些示例文件：

- `examples/bulk-import.txt`
- `examples/global-filters.txt`
- `examples/rename-rules.txt`

## 安全说明

- 不要把 `data/`、SQLite 数据库、备份文件提交到仓库
- 不要公开真实订阅 URL、Token、Webhook 地址、通知密钥
- 备份不会导出运行时 `session_secret`
- 如果你直接暴露公网，请尽快完成首次初始化并设置强密码

## 已知限制

- `Clash / Mihomo`、`Surge`、`sing-box` 导出优先覆盖常见协议和常见参数，不保证所有边缘参数都完全映射
- `Surge` 导出会跳过当前不兼容的协议
- 当前只有管理员登录，不包含多用户权限系统
- 当前前端不支持子路径部署

## 开源协议

本项目基于 `GNU Affero General Public License v3.0` 开源。
