"""用于更新项目依赖版本的工具脚本."""

import subprocess
import sys
from pathlib import Path
from typing import Dict

import tomli
import tomli_w


def get_installed_packages() -> Dict[str, str]:
    """获取当前环境中已安装的包及其版本."""
    result = subprocess.run(["pip", "freeze"], capture_output=True, text=True)
    packages = {}
    for line in result.stdout.splitlines():
        if "==" in line:
            name, version = line.split("==")
            packages[name.lower()] = version
    return packages


def update_dependencies(file_path: Path) -> bool:
    """更新 pyproject.toml 中的依赖版本."""
    try:
        # 读取 pyproject.toml
        with open(file_path, "rb") as file:
            data = tomli.load(file)

        if "project" not in data or "dependencies" not in data["project"]:
            print("No dependencies found in pyproject.toml")
            return False

        installed_packages = get_installed_packages()
        dependencies = data["project"]["dependencies"]
        updated = False

        # 更新依赖版本
        new_dependencies = []
        for dep in dependencies:
            pkg_name = dep.split("[")[0].split(">=")[0].split("==")[0].lower()
            if pkg_name in installed_packages:
                new_dep = f"{pkg_name}>={installed_packages[pkg_name]}"
                if new_dep != dep:
                    updated = True
                new_dependencies.append(new_dep)
            else:
                new_dependencies.append(dep)

        if updated:
            data["project"]["dependencies"] = new_dependencies
            with open(file_path, "wb") as file:
                tomli_w.dump(data, file)
            print("Dependencies updated in pyproject.toml")
            return True

        print("No updates needed")
        return False

    except Exception as e:
        print(f"Error updating dependencies: {e}")
        return False


def main() -> None:
    """脚本入口点，处理依赖更新流程."""
    project_root = Path(__file__).parent.parent
    pyproject_path = project_root / "pyproject.toml"

    if not pyproject_path.exists():
        print("pyproject.toml not found")
        sys.exit(1)

    success = update_dependencies(pyproject_path)
    if success:
        print("Dependencies updated in pyproject.toml")
    else:
        print("No updates needed")

    # Always exit with success (0) since both outcomes are valid
    sys.exit(0)


if __name__ == "__main__":
    main()
