import os
import yaml
from typing import Dict, Any


class Config:
    _instance = None
    _config: Dict[str, Any] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        """加载配置文件"""
        config_path = os.path.join(os.path.dirname(__file__), "config.yaml")

        try:
            with open(config_path, "r") as f:
                self._config = yaml.safe_load(f)
        except Exception as e:
            raise Exception(f"Error loading config file: {e}")

    @property
    def binance(self) -> Dict[str, str]:
        """获取 Binance API 配置"""
        return self._config.get("binance", {})

    @property
    def trading(self) -> Dict[str, list]:
        """获取交易配置"""
        return self._config.get("trading", {})

    @property
    def storage(self) -> Dict[str, str]:
        """获取存储配置"""
        return self._config.get("storage", {})

    @property
    def logging(self) -> Dict[str, str]:
        """获取日志配置"""
        return self._config.get("logging", {})


# 创建全局配置实例
config = Config()

__all__ = ["config"]
