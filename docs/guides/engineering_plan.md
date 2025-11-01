# Xdata Python 库工程化指导与迭代规划（uv + hatch-vcs + Towncrier）

本文档为维护者提供一套可执行、可验证的工程体系。所有规范均基于 uv、hatch-vcs 与 Towncrier，并以 GitHub Actions + PyPI Trusted Publishing 实现可信发布。

## 0. 适用范围与读者
- 适用项目：`cryptoservice`（`src` 布局，纯 Python，支持 Python 3.10–3.12）。
- 读者对象：维护者、贡献者、CI 运维人员。
- 文档风格：可执行、可验证，每项规范尽量给出验收标准与对应命令。

## 1. 当前工程基线（截至 2025-10-30）
- 依赖与构建：使用 uv (`uv sync`, `uv build`)；`pyproject.toml` 已启用 `hatch-vcs`（`dynamic = ["version"]`，`[tool.hatch.version] source = "vcs"`）。
- 变更与发布：`[tool.towncrier]` 已配置，碎片目录 `changes/`；`release.yml` 的 `notes` 作业完成以下任务：
  1. 通过 OIDC 发布到 PyPI。
  2. `uvx towncrier` 生成 GitHub Release Notes。
  3. 回写 `CHANGELOG.md`。
  4. 可选发送 Slack/飞书通知。
- 质量基线：`ruff`（target 3.10）、`mypy`（py3.10）、`pytest`（`asyncio_mode = auto`，覆盖率上报）。
- CI：`ci.yml` 使用 uv 同步依赖，执行 lint/type/test，矩阵覆盖 3.10/3.11/3.12。

> 注：本文档规划基于上述基线分阶段增强。

## 2. 总体原则
1. Trunk-based Development：常驻分支仅 `main`，短分支开发后 squash 合并，保持线性历史。
2. Tag-driven Release：所有稳定版/预发行通过 Git Tag 触发发布，版本号遵循 PEP 440。
3. Changeset-first：每个 PR 必须附带 Towncrier 碎片；发布依赖碎片生成可审计的变更说明。
4. 可复现：CI 本地统一使用 uv；锁文件与固定 Python 矩阵确保结果可复现。
5. 最小权限：PyPI 通过 Trusted Publishing（OIDC）实现无长期凭据；分支保护与强制 CI 绿。

## 3. 分支与提交流程
### 3.1 分支
- 命名：`feat/<slug>`、`fix/<slug>`、`chore/<slug>`、`docs/<slug>`、`refactor/<slug>`。
- 合并策略：Squash merge；禁止直接向 `main` 推送（需保护规则）。

### 3.2 提交信息（Conventional Commits）
- 格式：`type(scope): summary`，其中 `type ∈ {feat, fix, perf, refactor, docs, chore, test, build}`。
- 与 Towncrier 类型映射：
  - `feat` → `feature`
  - `fix` → `fix`
  - `perf` → `perf`
  - `refactor` → `refactor`
  - `docs` → `docs`
  - `chore` → `chore`
- 破坏性变更：在 commit/PR/碎片标题前加 `BREAKING` 或 `feat!`/`fix!`，Towncrier 类型使用 `breaking`。

## 4. 版本与发布策略
### 4.1 版本规则（PEP 440）
- 稳定版：`vX.Y.Z`。
- 预发行：`vX.Y.ZaN`/`bN`/`rcN`。
- 版本跃迁：`fix` → patch；`feat`/`perf`/非破坏性 `refactor` → minor；有 breaking 变更 → major。

### 4.2 发布流程（自动化）
1. PR 合并至 `main`，CI 通过；PR 必须包含 `changes/*.md` 碎片。
2. 生成/更新 `CHANGELOG` 的时机：
   - GitHub Release：`notes` 作业以 `--draft` 生成本次说明。
   - `CHANGELOG.md`：`notes` 作业随后以 `--yes` 正式落盘并清空已消费碎片。
3. 打 tag：维护者本地或发布脚本执行：

   ```bash
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git push origin vX.Y.Z
   ```

4. `release.yml`：
   - `uv build` → 通过 OIDC 发布到 PyPI。
   - 调用 `uvx towncrier` 生成 GitHub Release、更新 `CHANGELOG.md`、触发可选通知。

### 4.3 紧急回滚与补丁
- Yank：若发布存在问题，可在 PyPI 将版本标记为 yanked（默认安装将跳过）。
- 补丁：修复合并至 `main` 后发布 `vX.Y.(Z+1)`。
- 旧分支维护：仅在需要长期维护旧大版本时临时创建 `release/X.Y` 分支。

## 5. Changeset（Towncrier）规范
### 5.1 命名与目录
- 目录：`changes/`。
- 命名：`<id-or-topic>.<type>.md`，示例：
  - `412.fix.md`
  - `add-endpoint.feature.md`
  - `improve-parse-speed.perf.md`
  - `v2-deprecations.breaking.md`

### 5.2 内容规范
- 1–3 行完整句，说明做了什么、为什么、影响面；必要时给出迁移指引链接。
- 不写 PR/issue 链接（由 `issue_format = "#{issue}"` 自动插入）。

### 5.3 模板示例

```
Add REST /symbols endpoint with pagination support to reduce memory footprint on large universes.
```

### 5.4 校验
- CI 增设 `changeset.yml`：若 PR 未包含 `changes/*.md` 且未打 `skip-changelog` 标签则失败。

## 6. 依赖与环境管理
### 6.1 uv 使用规范
- 安装依赖：

  ```bash
  uv sync --group dev --group test
  ```

- 锁定：`uv lock` 生成锁文件，需提交以保障 CI 可重现。
- 构建：`uv build`；发布前执行 `uvx twine check dist/*` 校验产物。

### 6.2 Python 版本矩阵
- 运行/测试：3.10、3.11、3.12。
- Lint/Type 检查目标版本：3.10，确保向下兼容。

### 6.3 可选依赖（Extras）
- 定义 `test`、`dev`、`docs`、`dev-all`。按模块拆分重依赖，减少默认安装体积。

## 7. 质量门禁（QA Gates）
### 7.1 Lint（ruff）
- 命令：

  ```bash
  uv run ruff format --check .
  uv run ruff check .
  ```

- 规则集：`E,F,B,I,N,D,UP,S,C,SIM`；测试/类型目录的忽略规则在 `pyproject.toml` 中维护。

### 7.2 类型检查（mypy）
- 命令：

  ```bash
  uv run mypy src
  ```

- 策略：`ignore_missing_imports = true` 起步；逐步引入 stub 并缩窄到局部模块。
- 目标：公共 API（`src/cryptoservice/` 顶层模块）类型覆盖率 ≥ 90%。

### 7.3 测试（pytest）
- 命令：

  ```bash
  uv run pytest -q
  ```

- 配置：`asyncio_mode = auto`。
- 结构：单元测试覆盖纯函数与边界条件；集成测试对外部 API/DB 交互使用 mock/fake。
- 覆盖率：在 `pyproject.toml` 的 `addopts` 中设置 `--cov-fail-under=85`（可逐步提升）。
- 外部 API：统一封装 `BinanceClient` 抽象，测试中使用 `FakeBinanceClient` 或 `responses`/`httpx_mock`。

### 7.4 性能回归（可选）
- 引入 `pytest-benchmark`；为关键路径设定阈值基线与告警。

## 8. 安全与合规
- 代码：启用 `ruff` 的 `S` 规则；避免 `eval`/`exec` 和硬编码密钥。
- 依赖安全：在 CI 中增加 `uvx pip-audit`（或使用 GitHub Advanced Security 依赖扫描）。
- 机密：开启 GitHub Secret Scanning；引入 `pre-commit` 的 `detect-secrets`/`gitleaks` 钩子。
- 供应链：使用 Trusted Publishing (OIDC) 发布；不在仓库或 CI 存储长期 PyPI Token。

## 9. CI/CD 设计与运行手册
### 9.1 `ci.yml`
- 触发：`push` 与 `pull_request`。
- Python 矩阵：3.10、3.11、3.12。
- 步骤：checkout → setup-uv → `uv sync --all-extras` → `ruff` → `mypy` → `pytest`。

### 9.2 `release.yml`
- 触发：`push` tag 匹配 `v*`。
- 发布流程：`uv build` → `pypa/gh-action-pypi-publish`（OIDC）。
- Release Notes：`uvx towncrier --draft` 生成说明，随后 `--yes` 回写 `CHANGELOG.md`，附加可选通知。

### 9.3 常见故障排查
- PyPI 返回 403：检查 PyPI Trusted Publisher 绑定与 `permissions: id-token: write`。
- Towncrier 输出为空：确认 PR 是否包含 `changes/*.md`。
- 版本号异常：确保 Tag 命名为 `vX.Y.Z`，并设置 `fetch-depth: 0`。

## 10. 文档工程
- 使用 MkDocs Material + `mkdocstrings[python]`。
- 最小结构：`docs/index.md`、`docs/usage/*.md`、`docs/api.md`（基于 docstring 自动生成）。
- 常用命令：

  ```bash
  uv run mkdocs serve
  uv run mkdocs build
  ```

- 规范：公共 API 必须提供 Google 风格 docstring（`ruff` 的 `pydocstyle` 已配置）。

## 11. 包结构与可发布性
- 布局：`src/cryptoservice/`。
- 类型提示：在包根放置 `py.typed` 并确保打包包含。
- 入口点：如需命令行，使用 `project.scripts` 或 `gui-scripts` 声明。
- 构建产物：sdist + wheel；发布前执行 `uvx twine check dist/*`。

## 12. 观测性与错误设计（建议）
- 日志：统一采用 `structlog` 抽象，必要时输出 JSON，保证字段稳定。
- 错误：定义公共异常层级（`CryptoserviceError` → 子类），为常见错误提供错误码与上下文。
- 指标（可选）：通过 `prometheus_client` 暴露关键调用耗时、错误率。

## 13. 里程碑式迭代计划（12 周）
- Phase 0（已完成）：uv 构建与 CI 统一、hatch-vcs、release + notes、Towncrier 配置、质量工具基线。

### Phase 1：质量固化（第 1–2 周）
- 补齐公共 API 模块的 Google 风格 docstring。
- 在 `pyproject.toml` 增加 `--cov-fail-under=80` 并开启模块级覆盖率报告。
- 引入 `pre-commit`：`ruff`、`detect-secrets`/`gitleaks`，并在 CI 校验。
- 验收：CI 全绿；文档生成无缺失；新增 PR 覆盖率不下降（可选引入 `diff-cover`）。

### Phase 2：测试与稳定性（第 3–5 周）
- 抽象外部依赖（Binance 客户端）并提供 Fake。
- 为所有 I/O 路径补充集成测试（使用 fake/fixture，避免真实 API）。
- 覆盖率门槛提升至 `--cov-fail-under=85`。
- 验收：关键路径（解析、下单计划、持久化）具备失败与边界测试。

### Phase 3：发布体验（第 6–8 周）
- 引入 `Makefile` 或 `nox` 快捷命令（参见附录）。
- 完成 GitHub Issue/PR 模板与贡献指南（`CONTRIBUTING.md`）。
- 打通文档站（GitHub Pages）。
- 验收：单条命令完成本地 Lint/Type/Test/Build；贡献者可按文档完成一次 PR 与试发 RC。

### Phase 4：安全与供应链（第 9–12 周）
- 在 CI 增加 `uvx pip-audit`。
- 开启 GitHub Dependabot 与 Dependency Review。
- 可选：为发布产物附加 SBOM（`cyclonedx-bom`）。
- 验收：CI 对高危漏洞阻断发布；依赖更新具备审阅与自动 PR 流程。

## 14. 运行手册（Cheat-sheet）
### 本地开发

```bash
uv sync --group dev --group test
uv run ruff format --check . && uv run ruff check .
uv run mypy src
uv run pytest -q
```

### 预览发布说明

```bash
uvx towncrier build --draft --version 1.16.3 | less
```

### 发版（稳定/预发行）

```bash
git tag -a v1.16.3 -m "v1.16.3"
git push origin v1.16.3
# GitHub Actions 自动执行：build → PyPI → Release Notes → 更新 CHANGELOG → 通知
```

### 回滚与补丁
1. 在 PyPI 将版本标记为 yanked。
2. 修复合并至 `main`。
3. 发布 `vX.Y.(Z+1)`。

## 15. 附录
### 15.1 PR 模板示例（`.github/PULL_REQUEST_TEMPLATE.md`）

```
## 变更
- [x] 类型：feat / fix / perf / refactor / docs / chore
- [x] 关联 issue：#123

## 风险与回滚
- 影响面：
- 回滚策略：

## 校验
- [x] Lint / Type / Test 通过
- [x] 添加 changes/*.md 变更碎片（或加 skip-changelog 标签）
```

### 15.2 Issue 模板（简版）
- Bug：复现步骤、期望、实际、日志、环境。
- Feature：动机、方案、兼容性、验收标准。

### 15.3 `pre-commit` 配置片段（`.pre-commit-config.yaml`）

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.6.9
    hooks:
      - id: ruff
      - id: ruff-format
  - repo: https://github.com/Yelp/detect-secrets
    rev: v1.5.0
    hooks:
      - id: detect-secrets
```

### 15.4 Makefile 片段

```make
.PHONY: setup lint type test build release notes
setup: ; uv sync --group dev --group test
lint: ; uv run ruff format --check . && uv run ruff check .
type: ; uv run mypy src
test: ; uv run pytest -q
build: ; uv build
notes: ; uvx towncrier build --draft --version $(VER)
```

---

本计划面向单人维护的 Python 库，可作为工程化蓝本。建议每季度复盘一次，例如覆盖率阈值、依赖更新策略与版本支持矩阵等，可根据实践动态调整。
