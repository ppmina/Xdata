#!/bin/bash
# 脚本用于在 Unix/Linux/macOS 环境下安装和设置 uv 包管理工具

set -e  # 任何命令失败时立即退出

echo "开始安装和设置 uv 包管理工具..."

# 检查操作系统类型
if [[ "$(uname)" == "Darwin" ]]; then
    # macOS
    echo "检测到 macOS 系统"

    # 检查是否已安装 Homebrew
    if command -v brew &>/dev/null; then
        echo "使用 Homebrew 安装 uv..."
        brew install uv
    else
        echo "Homebrew 未安装，使用 curl 安装 uv..."
        curl -LsSf https://astral.sh/uv/install.sh | sh
    fi
elif [[ "$(uname)" == "Linux" ]]; then
    # Linux
    echo "检测到 Linux 系统"
    curl -LsSf https://astral.sh/uv/install.sh | sh
else
    echo "不支持的操作系统: $(uname)"
    exit 1
fi

# 确保 uv 在 PATH 中
export PATH="$HOME/.cargo/bin:$PATH"

# 检查安装
if command -v uv &>/dev/null; then
    echo "uv 已成功安装: $(uv --version)"
else
    echo "uv 安装失败，请检查安装日志"
    exit 1
fi

# 创建虚拟环境
echo "使用 uv 创建虚拟环境..."
uv venv .venv

# 检测当前的 Shell
CURRENT_SHELL=$(basename "$SHELL")
echo "检测到您使用的是 $CURRENT_SHELL shell"

# 根据不同的 Shell 激活虚拟环境
echo "激活虚拟环境..."
if [[ "$CURRENT_SHELL" == "fish" ]]; then
    # 对于 fish shell
    source .venv/bin/activate.fish
else
    # 对于 bash/zsh 等常见 shell
    source .venv/bin/activate
fi

# 使用 uv 同步依赖
echo "安装项目依赖..."
uv pip sync -e ".[dev,test]"

# 安装 pre-commit hooks
echo "安装 pre-commit hooks..."
pre-commit install

echo "设置完成！您现在可以使用 uv 管理项目依赖。"
echo "使用示例:"
echo "  - uv pip install <package>  # 安装包"
echo "  - uv pip freeze             # 显示已安装的包"
echo "  - uv pip sync -e '.[dev]'   # 同步项目的开发依赖"
echo ""
echo "提示：如果您使用 fish shell，请记得用以下命令激活虚拟环境:"
echo "  source .venv/bin/activate.fish"
