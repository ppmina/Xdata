# PowerShell 脚本用于在 Windows 上安装和设置 uv 包管理工具

Write-Host "开始安装和设置 uv 包管理工具..." -ForegroundColor Green

# 检查是否已安装 uv
$uvExists = $null -ne (Get-Command uv -ErrorAction SilentlyContinue)
if (-not $uvExists) {
    Write-Host "正在下载和安装 uv..." -ForegroundColor Yellow

    # 创建临时目录
    $tempDir = Join-Path $env:TEMP "uv-installer"
    if (-not (Test-Path $tempDir)) {
        New-Item -Path $tempDir -ItemType Directory | Out-Null
    }

    # 下载安装程序
    $installerUrl = "https://github.com/astral/uv/releases/latest/download/uv-x86_64-pc-windows-msvc.exe"
    $installerPath = Join-Path $tempDir "uv.exe"

    try {
        Invoke-WebRequest -Uri $installerUrl -OutFile $installerPath

        # 添加到用户 PATH
        $userPath = [Environment]::GetEnvironmentVariable("PATH", "User")
        $targetDir = Join-Path $env:USERPROFILE ".uv\bin"

        # 创建目标目录
        if (-not (Test-Path $targetDir)) {
            New-Item -Path $targetDir -ItemType Directory -Force | Out-Null
        }

        # 复制可执行文件
        Copy-Item -Path $installerPath -Destination (Join-Path $targetDir "uv.exe") -Force

        # 更新 PATH（如果需要）
        if ($userPath -notlike "*$targetDir*") {
            [Environment]::SetEnvironmentVariable("PATH", "$userPath;$targetDir", "User")
            $env:PATH = "$env:PATH;$targetDir"
        }

        Write-Host "uv 安装完成" -ForegroundColor Green
    }
    catch {
        Write-Host "安装失败。错误：$_" -ForegroundColor Red
        Write-Host "请访问 https://github.com/astral/uv 手动安装" -ForegroundColor Yellow
        exit 1
    }
    finally {
        # 清理
        Remove-Item -Path $tempDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}
else {
    Write-Host "uv 已安装" -ForegroundColor Green
}

# 检查安装
try {
    $uvVersion = (uv --version)
    Write-Host "uv 版本: $uvVersion" -ForegroundColor Cyan
}
catch {
    Write-Host "无法运行 uv。请确保它在你的 PATH 中" -ForegroundColor Red
    exit 1
}

# 创建虚拟环境
Write-Host "使用 uv 创建虚拟环境..." -ForegroundColor Yellow
uv venv .venv

# 激活虚拟环境
Write-Host "激活虚拟环境..." -ForegroundColor Yellow
& ".\.venv\Scripts\Activate.ps1"

# 使用 uv 同步依赖
Write-Host "安装项目依赖..." -ForegroundColor Yellow
uv pip sync -e ".[dev,test]"

# 安装 pre-commit hooks
Write-Host "安装 pre-commit hooks..." -ForegroundColor Yellow
pre-commit install

Write-Host "设置完成！您现在可以使用 uv 管理项目依赖。" -ForegroundColor Green
Write-Host "使用示例:" -ForegroundColor Cyan
Write-Host "  - uv pip install <package>  # 安装包" -ForegroundColor Cyan
Write-Host "  - uv pip freeze             # 显示已安装的包" -ForegroundColor Cyan
Write-Host "  - uv pip sync -e '.[dev]'   # 同步项目的开发依赖" -ForegroundColor Cyan

Write-Host ""
Write-Host "注意：如果您在 macOS/Linux 上使用 fish shell，请记得用以下命令激活虚拟环境:" -ForegroundColor Yellow
Write-Host "  source .venv/bin/activate.fish" -ForegroundColor Yellow
