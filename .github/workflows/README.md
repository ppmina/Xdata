# GitHub Actions 工作流说明

## 📋 工作流概览

本项目使用自动化的 CI/CD 流程，从代码提交到发布完全自动化。

### 工作流架构

```
feat 分支 → PR → CI 检测 → 合并到 main → 自动版本 + Tag → 发布到 PyPI
```

## 🔄 工作流详细说明

### 1. **pre-pr.yml** - PR 质量检查

**触发条件:**
- Pull Request 创建或更新时
- 推送到 `main` 分支时

**功能:**
- 多版本 Python 测试 (3.11, 3.12, 3.13)
- 运行 pre-commit hooks
- 代码格式检查 (Ruff format)
- 代码质量检查 (Ruff lint)
- 类型检查 (Mypy)
- 单元测试 (Pytest)

**作用:** 确保合并到 main 的代码质量符合标准

---

### 2. **auto-version-tag.yml** - 自动版本和标签 ⭐ 新增

**触发条件:**
- 推送到 `main` 分支时（排除 tag 推送和 `[skip ci]` 提交）

**功能:**
1. **智能版本检测** - 根据提交信息自动判断版本变更类型：
   - `BREAKING CHANGE` / `breaking:` → Major 版本 (x.0.0)
   - `feat:` / `feature:` → Minor 版本 (0.x.0)
   - `fix:` / `bugfix:` → Patch 版本 (0.0.x)
   - 检查 `changes/` 目录的变更文件

2. **自动版本计算** - 基于当前版本和变更类型计算新版本号

3. **生成 Changelog** - 使用 towncrier 从 `changes/` 目录构建 CHANGELOG.md

4. **提交和标签**:
   - 提交 CHANGELOG 更新到 main 分支 (`[skip ci]`)
   - 创建版本 tag (例如 `v1.17.0`)
   - 推送 tag 到远程仓库

**作用:** 实现从提交到版本发布的自动化

---

### 3. **release.yml** - 发布到 PyPI ⭐ 已优化

**触发条件:**
- 推送 `v*` 格式的 tag 时（由 auto-version-tag.yml 自动触发）

**工作流程:**

#### Job 1: `build` - 构建分发包
- 使用 `uv build` 构建 Python 包
- 上传构建产物到 artifact

#### Job 2: `publish` - 发布到 PyPI
- 下载构建产物
- 使用 PyPI Trusted Publishing (OIDC) 发布
- 支持跳过已存在的版本

#### Job 3: `release-notes` - 创建 GitHub Release
- 从 CHANGELOG.md 提取当前版本的发布说明
- 创建 GitHub Release
- 附加分发包文件
- 发送飞书 Webhook 通知（如果配置）

**作用:** 自动发布包到 PyPI 并创建 GitHub Release

---

## 🚀 完整发布流程示例

### 场景 1: 新功能发布

```bash
# 1. 在 feat 分支开发
git checkout -b feat/add-new-api

# 2. 添加变更文件 (可选，推荐)
echo "Add new trading API endpoint" > changes/123.feature.md

# 3. 提交代码（使用约定式提交）
git commit -m "feat: add new trading API endpoint"

# 4. 推送并创建 PR
git push origin feat/add-new-api

# 5. PR 通过 CI 检查后，合并到 main
# merge PR → main

# 6. 自动触发 auto-version-tag.yml
#    - 检测到 "feat:" 提交
#    - 自动升级 minor 版本: v1.16.3 → v1.17.0
#    - 生成 CHANGELOG
#    - 创建并推送 tag v1.17.0

# 7. 自动触发 release.yml
#    - 构建包
#    - 发布到 PyPI
#    - 创建 GitHub Release
#    - 发送通知
```

### 场景 2: Bug 修复发布

```bash
# 1. 在 fix 分支修复
git checkout -b fix/trading-bug

# 2. 添加变更文件
echo "Fix trading calculation error" > changes/124.fix.md

# 3. 提交修复（使用约定式提交）
git commit -m "fix: correct trading calculation logic"

# 4. 合并到 main
# merge PR → main

# 5. 自动流程
#    - 检测到 "fix:" 提交
#    - 自动升级 patch 版本: v1.17.0 → v1.17.1
#    - 其余步骤同上
```

### 场景 3: 重大变更发布

```bash
# 提交信息包含 BREAKING CHANGE
git commit -m "feat!: redesign API authentication

BREAKING CHANGE: API keys now require prefix 'sk-'"

# 或使用 breaking 类型的变更文件
echo "Redesign API authentication" > changes/125.breaking.md

# 合并后自动升级 major 版本: v1.17.1 → v2.0.0
```

---

## 📝 约定式提交规范

项目遵循 [Conventional Commits](https://www.conventionalcommits.org/) 规范：

### 提交格式
```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### 常用类型
- `feat:` - 新功能 (minor 版本)
- `fix:` - Bug 修复 (patch 版本)
- `docs:` - 文档更新
- `style:` - 代码格式调整
- `refactor:` - 重构
- `perf:` - 性能优化
- `test:` - 测试相关
- `chore:` - 构建/工具链更新
- `BREAKING CHANGE:` - 破坏性变更 (major 版本)

### 示例
```bash
# Minor 版本升级
git commit -m "feat: add WebSocket support for real-time data"

# Patch 版本升级
git commit -m "fix: resolve race condition in async operations"

# Major 版本升级
git commit -m "feat!: change API response format

BREAKING CHANGE: All API responses now return JSON instead of XML"
```

---

## 🔧 配置要求

### GitHub Secrets
项目需要配置以下 secrets：

- `GITHUB_TOKEN` - 自动提供，用于创建 release 和推送代码
- `WEBHOOK_URL` - (可选) 飞书 Webhook URL，用于发布通知

### PyPI Trusted Publishing
项目使用 PyPI Trusted Publishing (OIDC)，无需配置 API Token。

配置步骤：
1. 访问 https://pypi.org/manage/account/publishing/
2. 添加新的 publisher：
   - PyPI Project Name: `cryptoservice`
   - Owner: `your-github-username`
   - Repository: `Xdata`
   - Workflow: `release.yml`
   - Environment: `pypi`

---

## 🎯 最佳实践

### 1. 使用变更文件 (推荐)
在 `changes/` 目录创建变更文件，更好地组织 changelog：

```bash
# 格式: {issue_number}.{type}.md
echo "Your change description" > changes/123.feature.md
echo "Bug fix description" > changes/124.fix.md
```

支持的类型：
- `feature` - 新功能
- `fix` - 修复
- `perf` - 性能优化
- `refactor` - 重构
- `docs` - 文档
- `breaking` - 破坏性变更
- `chore` - 杂项

### 2. 遵循约定式提交
即使不使用变更文件，约定式提交也能触发自动版本升级。

### 3. 跳过 CI（谨慎使用）
在提交信息中添加 `[skip ci]` 可以跳过工作流：

```bash
git commit -m "docs: update README [skip ci]"
```

### 4. 手动触发发布（紧急情况）
如果需要手动发布：

```bash
# 创建版本 tag
git tag -a v1.18.0 -m "Release v1.18.0"

# 推送 tag
git push origin v1.18.0

# 这将触发 release.yml 工作流
```

---

## 🔍 监控和调试

### 查看工作流状态
- GitHub Actions 页面: `https://github.com/your-username/Xdata/actions`
- 每个工作流都有详细的执行日志

### 常见问题排查

**问题 1: 版本没有自动升级**
- 检查提交信息是否符合约定式提交规范
- 确认没有使用 `[skip ci]` 标记
- 查看 auto-version-tag.yml 工作流日志

**问题 2: 发布失败**
- 检查 PyPI Trusted Publishing 配置
- 确认版本号没有重复
- 查看 release.yml 工作流日志

**问题 3: Changelog 没有更新**
- 确认 `changes/` 目录有变更文件
- 检查 towncrier 配置
- 查看工作流日志中的 changelog 生成步骤

---

## 📚 相关文档

- [Conventional Commits](https://www.conventionalcommits.org/)
- [Semantic Versioning](https://semver.org/)
- [Towncrier Documentation](https://towncrier.readthedocs.io/)
- [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/)
- [GitHub Actions](https://docs.github.com/en/actions)

---

## 🔄 工作流更新历史

- **2025-01-30**: 添加自动版本和标签工作流 (auto-version-tag.yml)
- **2025-01-30**: 优化发布工作流 (release.yml)
- **2025-01-30**: 禁用手动发布工作流 (python-publish.yml)
