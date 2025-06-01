# 开发指南

本文档提供了使用 uv 包管理器和 ruff 代码检查工具进行项目开发的详细指南。

## 环境设置

### 安装 uv

我们使用 `uv` 作为项目的包管理工具，它比传统的 pip 更快、更可靠。

#### 自动安装

可以使用项目提供的脚本自动安装 uv：

- **macOS/Linux**:
  ```bash
  ./scripts/setup_uv.sh
  ```

- **Windows**:
  ```powershell
  .\scripts\setup_uv.ps1
  ```

#### 手动安装

如果自动脚本不起作用，可以按照以下步骤手动安装：

- **macOS**:
  ```bash
  brew install uv
  ```

- **Linux/macOS 手动安装**:
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

- **Windows**:
  ```powershell
  irm https://astral.sh/uv/install.ps1 | iex
  ```

### 创建虚拟环境

使用 uv 创建和管理虚拟环境：

```bash
# 创建虚拟环境
uv venv .venv

# 激活虚拟环境
# 在 Linux/macOS 上
source .venv/bin/activate
# 在 Windows 上
.\.venv\Scripts\activate
```

### 安装依赖

使用 uv 安装项目依赖：

```bash
# 安装基本依赖
uv pip install -e .

# 安装开发依赖
uv pip install -e ".[dev]"

# 安装测试依赖
uv pip install -e ".[test]"

# 安装全部依赖
uv pip install -e ".[dev,test]"
```

### 同步依赖

推荐使用 uv 的 sync 命令来确保依赖的一致性：

```bash
# 同步所有依赖
uv pip sync -e ".[dev,test]"
```

## 代码质量工具

### 使用 Ruff 进行代码检查

我们使用 `ruff` 作为主要的代码检查和格式化工具。它集成了多种 Python 代码质量检查工具的功能，包括：

- 格式化代码（类似 black）
- 导入排序（类似 isort）
- 代码检查（类似 flake8、pylint）

#### 代码检查

检查代码是否符合规范：

```bash
# 检查整个项目
ruff check .

# 检查特定文件或目录
ruff check src/
```

#### 代码自动修复

修复可自动修复的问题：

```bash
# 自动修复项目中的问题
ruff check --fix .
```

#### 代码格式化

格式化代码：

```bash
# 格式化整个项目
ruff format .

# 格式化特定文件或目录
ruff format src/
```

### 预提交钩子

我们使用 pre-commit 来自动运行代码检查工具。安装 pre-commit 钩子：

```bash
pre-commit install
```

这将确保每次提交代码前自动运行 ruff 进行代码检查和格式化。

## 测试

运行测试：

```bash
# 运行所有测试
pytest

# 运行特定测试文件
pytest tests/test_specific.py

# 运行特定测试函数
pytest tests/test_specific.py::test_function
```

## 项目结构优化建议

为了保持项目结构清晰，我们建议遵循以下原则：

1. **模块化组织**：将相关功能组织到逻辑模块中
2. **关注点分离**：将数据访问、业务逻辑和界面分开
3. **清晰的依赖关系**：避免循环依赖，明确模块间的依赖关系
4. **测试驱动开发**：为每个模块编写单元测试

### 推荐的目录结构

```
project_root/
│
├── src/                       # 源代码
│   └── package_name/          # 主包
│       ├── __init__.py
│       ├── module1/           # 功能模块1
│       │   ├── __init__.py
│       │   └── ...
│       └── module2/           # 功能模块2
│           ├── __init__.py
│           └── ...
│
├── tests/                     # 测试代码
│   ├── __init__.py
│   ├── test_module1/
│   │   ├── __init__.py
│   │   └── ...
│   └── test_module2/
│       ├── __init__.py
│       └── ...
│
├── docs/                      # 文档
├── scripts/                   # 实用脚本
├── pyproject.toml            # 项目配置
├── README.md                 # 项目说明
└── .pre-commit-config.yaml   # 预提交配置
```

## 版本控制最佳实践

1. **语义化版本**：遵循 [语义化版本 2.0.0](https://semver.org/lang/zh-CN/) 规范
2. **提交消息规范**：使用 [约定式提交](https://www.conventionalcommits.org/zh-hans/v1.0.0/) 规范
3. **分支策略**：采用 Git Flow 或类似的分支管理策略

## 常见问题解答

### Q: uv 与 pip 有什么不同？
A: uv 是一个更快、更可靠的包管理器，它使用 Rust 编写，可以并行下载和安装包。它与 pip 兼容，但速度更快。

### Q: 如何在 CI/CD 环境中使用 uv？
A: 可以在 CI/CD 工作流中添加安装和使用 uv 的步骤，类似于本地环境的设置过程。

### Q: 为什么选择 ruff 而不是其他代码检查工具？
A: ruff 集成了多种工具的功能，速度更快，维护更简单。它可以替代 black、isort、flake8 等多个工具，减少了配置和依赖的复杂性。
