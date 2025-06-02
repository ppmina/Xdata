# 开发指南

本指南帮助开发者快速设置 CryptoService 开发环境，使用现代工具进行高效开发。

## 🛠️ 工具链

本项目使用现代 Python 开发工具链：

- **uv**: 超快的 Python 包管理器
- **ruff**: 极速的代码检查和格式化工具
- **mypy**: 静态类型检查
- **pytest**: 测试框架
- **pre-commit**: Git 预提交钩子

## 🚀 快速开始

### 1. 安装 uv

uv 是比 pip 更快的包管理器，推荐用于开发。

**自动安装脚本:**
```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
irm https://astral.sh/uv/install.ps1 | iex
```

**手动安装:**
```bash
# macOS (Homebrew)
brew install uv

# 其他平台参考: https://docs.astral.sh/uv/getting-started/installation/
```

### 2. 克隆并设置项目

```bash
# 克隆项目
git clone https://github.com/username/cryptoservice.git
cd cryptoservice

# 创建虚拟环境并同步依赖
uv sync --all-extras --dev

# 激活虚拟环境
source .venv/bin/activate  # Linux/macOS
# 或
.venv\Scripts\activate     # Windows
```

### 3. 安装 pre-commit 钩子

```bash
uv run pre-commit install
```

现在每次提交代码时都会自动运行代码检查。

## 🔍 代码质量工具

### Ruff - 代码检查和格式化

Ruff 替代了多个传统工具（black, isort, flake8 等），提供统一的代码质量管理。

```bash
# 检查代码问题
uv run ruff check src/

# 自动修复可修复的问题
uv run ruff check --fix src/

# 格式化代码
uv run ruff format src/

# 检查格式是否正确（CI 中使用）
uv run ruff format --check src/
```

### MyPy - 类型检查

```bash
# 运行类型检查
uv run mypy src/

# 显示错误代码（调试用）
uv run mypy src/ --show-error-codes
```

### 组合命令

```bash
# 完整的代码质量检查（等同于 CI）
uv run ruff check src/ && \
uv run ruff format --check src/ && \
uv run mypy src/
```

## 🧪 测试

### 运行测试

```bash
# 运行所有测试
uv run pytest

# 运行特定测试文件
uv run pytest tests/test_basic.py

# 运行特定测试函数
uv run pytest tests/test_basic.py::test_universe_config

# 带覆盖率报告
uv run pytest --cov=src/cryptoservice --cov-report=html
```

### 添加新测试

在 `tests/` 目录下创建以 `test_` 开头的文件：

```python
# tests/test_new_feature.py
import pytest
from cryptoservice.models import UniverseConfig

def test_new_feature():
    """测试新功能"""
    config = UniverseConfig(
        start_date="2024-01-01",
        end_date="2024-01-31",
        t1_months=1,
        t2_months=1,
        t3_months=3,
        top_k=10
    )
    assert config.start_date == "2024-01-01"
```

## 📦 依赖管理

### 添加新依赖

```bash
# 添加运行时依赖
uv add pandas>=2.0.0

# 添加开发依赖
uv add --dev pytest>=7.0.0

# 添加可选依赖组
uv add --optional ml scikit-learn
```

### 依赖组说明

- **dev**: 开发工具（ruff, mypy, pre-commit）
- **test**: 测试相关（pytest, pytest-cov）
- **docs**: 文档生成（mkdocs 相关）

## 🔧 配置文件

### pyproject.toml

项目的核心配置文件，包含：

- 包元数据和依赖
- Ruff 配置（代码检查规则）
- MyPy 配置（类型检查）
- Pytest 配置

### .pre-commit-config.yaml

Pre-commit 钩子配置，确保代码质量：

- Ruff 检查和格式化
- MyPy 类型检查
- YAML 语法检查
- 尾随空白处理

## 🚦 CI/CD

### GitHub Actions

`.github/workflows/pr-check.yml` 定义了 CI 流程：

1. **环境设置**: 安装 Python 和 uv
2. **依赖安装**: `uv sync --all-extras --dev`
3. **代码检查**: Ruff + MyPy
4. **测试运行**: pytest
5. **YAML 验证**: 配置文件检查

### 本地模拟 CI

```bash
# 模拟 CI 检查流程
uv sync --all-extras --dev
uv run ruff check src/
uv run ruff format src/ --check
uv run mypy src/
uv run pytest tests/
```

## 💡 开发技巧

### 1. 使用 uv 运行脚本

```bash
# 直接运行 Python 脚本
uv run python demo/universe_demo.py

# 运行单个命令
uv run python -c "import cryptoservice; print('OK')"
```

### 2. 代码组织原则

- **模块化**: 将相关功能组织在一起
- **类型提示**: 使用类型提示提高代码可读性
- **文档字符串**: 为公共 API 编写文档
- **错误处理**: 使用自定义异常类型

### 3. 调试技巧

```bash
# 启用详细日志
export PYTHONPATH=src:$PYTHONPATH
export LOG_LEVEL=DEBUG

# 运行单个模块
uv run python -m cryptoservice.services.market_service
```

## 🔄 工作流程

### 典型开发流程

1. **创建功能分支**:
   ```bash
   git checkout -b feature/new-feature
   ```

2. **开发和测试**:
   ```bash
   # 编写代码
   # 运行测试
   uv run pytest tests/
   ```

3. **代码检查**:
   ```bash
   # 格式化代码
   uv run ruff format src/

   # 检查问题
   uv run ruff check --fix src/

   # 类型检查
   uv run mypy src/
   ```

4. **提交代码**:
   ```bash
   git add .
   git commit -m "feat: add new feature"  # 遵循约定式提交
   ```

5. **推送和 PR**:
   ```bash
   git push origin feature/new-feature
   # 在 GitHub 创建 Pull Request
   ```

## 🆘 常见问题

### Q: uv sync 失败怎么办？
```bash
# 清理缓存重试
uv cache clean
uv sync --all-extras --dev
```

### Q: pre-commit 钩子失败？
```bash
# 手动运行所有钩子
uv run pre-commit run --all-files

# 跳过钩子提交（不推荐）
git commit --no-verify
```

### Q: 类型检查错误？
```bash
# 查看详细错误信息
uv run mypy src/ --show-error-codes --verbose
```

## 📚 更多资源

- [uv 官方文档](https://docs.astral.sh/uv/)
- [Ruff 配置指南](https://docs.astral.sh/ruff/configuration/)
- [MyPy 类型检查指南](https://mypy.readthedocs.io/)
- [约定式提交规范](https://www.conventionalcommits.org/zh-hans/)

---

🎉 现在你已经准备好进行 CryptoService 的开发了！有问题欢迎提 Issue。
