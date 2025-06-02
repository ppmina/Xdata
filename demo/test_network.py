#!/usr/bin/env python3
"""
网络连接诊断工具

测试到Binance API的连接状态和可能的限制
"""

import os
import sys
import time
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 禁用SSL警告（仅用于测试）
urllib3.disable_warnings(InsecureRequestWarning)


def test_basic_connectivity():
    """测试基本网络连接"""
    print("🌐 测试基本网络连接...")

    test_urls = [
        "https://httpbin.org/get",
        "https://api.github.com",
        "https://www.google.com",
    ]

    for url in test_urls:
        try:
            response = requests.get(url, timeout=10)
            print(f"✅ {url}: {response.status_code}")
        except Exception as e:
            print(f"❌ {url}: {e}")


def test_binance_connectivity():
    """测试Binance API连接"""
    print("\n🪙 测试Binance API连接...")

    binance_urls = [
        "https://api.binance.com/api/v3/time",  # Spot API
        "https://fapi.binance.com/fapi/v1/time",  # Futures API
        "https://fapi.binance.com/fapi/v1/exchangeInfo",  # 问题API
    ]

    for url in binance_urls:
        print(f"\n🔗 测试: {url}")

        # 测试不同的方法
        methods = [
            ("默认请求", {}),
            ("禁用SSL验证", {"verify": False}),
            ("自定义User-Agent", {"headers": {"User-Agent": "Python/Binance-Test"}}),
            ("增加超时", {"timeout": 30}),
        ]

        for method_name, kwargs in methods:
            try:
                start_time = time.time()
                response = requests.get(url, **kwargs)
                elapsed = time.time() - start_time

                if response.status_code == 200:
                    print(f"   ✅ {method_name}: {response.status_code} ({elapsed:.2f}s)")
                    if "time" in url:
                        data = response.json()
                        print(f"      服务器时间: {data.get('serverTime', 'N/A')}")
                    elif "exchangeInfo" in url:
                        data = response.json()
                        symbols_count = len(data.get("symbols", []))
                        print(f"      交易对数量: {symbols_count}")
                    break
                else:
                    print(f"   ⚠️  {method_name}: {response.status_code}")

            except requests.exceptions.SSLError as e:
                print(f"   🔒 {method_name}: SSL错误 - {str(e)[:100]}...")
            except requests.exceptions.ConnectionError as e:
                print(f"   🚫 {method_name}: 连接错误 - {str(e)[:100]}...")
            except requests.exceptions.Timeout as e:
                print(f"   ⏰ {method_name}: 超时错误 - {str(e)[:100]}...")
            except Exception as e:
                print(f"   ❌ {method_name}: 其他错误 - {str(e)[:100]}...")


def test_with_retry_strategy():
    """测试带重试策略的连接"""
    print("\n🔄 测试重试策略...")

    session = requests.Session()

    # 配置重试策略
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"

    try:
        print(f"🔗 重试策略测试: {url}")
        response = session.get(url, timeout=30)

        if response.status_code == 200:
            data = response.json()
            symbols_count = len(data.get("symbols", []))
            print(f"✅ 重试策略成功: {response.status_code}")
            print(f"   交易对数量: {symbols_count}")
        else:
            print(f"⚠️  重试策略失败: {response.status_code}")

    except Exception as e:
        print(f"❌ 重试策略错误: {e}")


def test_proxy_bypass():
    """测试是否需要代理"""
    print("\n🔀 检查代理设置...")

    proxy_vars = ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]
    has_proxy = False

    for var in proxy_vars:
        value = os.environ.get(var)
        if value:
            print(f"   🔍 发现代理设置: {var}={value}")
            has_proxy = True

    if not has_proxy:
        print("   ✅ 未检测到代理设置")

    return has_proxy


def test_dns_resolution():
    """测试DNS解析"""
    print("\n🌍 测试DNS解析...")

    import socket

    hosts = ["fapi.binance.com", "api.binance.com", "www.binance.com"]

    for host in hosts:
        try:
            ip = socket.gethostbyname(host)
            print(f"✅ {host} -> {ip}")
        except Exception as e:
            print(f"❌ {host}: DNS解析失败 - {e}")


def check_system_info():
    """检查系统信息"""
    print("\n💻 系统信息...")

    import platform
    import ssl

    print(f"   操作系统: {platform.system()} {platform.release()}")
    print(f"   Python版本: {sys.version}")
    print(f"   SSL版本: {ssl.OPENSSL_VERSION}")
    print(f"   Requests版本: {requests.__version__}")


def main():
    """主函数"""
    print("=" * 60)
    print("🔧 Binance API 网络连接诊断工具")
    print("=" * 60)

    check_system_info()
    test_dns_resolution()
    test_proxy_bypass()
    test_basic_connectivity()
    test_binance_connectivity()
    test_with_retry_strategy()

    print("\n" + "=" * 60)
    print("🎯 诊断完成")
    print("=" * 60)

    print("\n💡 解决建议:")
    print("1. 如果SSL错误持续出现，可能是地区网络限制")
    print("2. 可以尝试使用VPN或代理服务器")
    print("3. 检查防火墙设置是否阻止了连接")
    print("4. 确认Binance API在您的地区是否可用")
    print("5. 可以尝试使用测试网络: https://testnet.binancefuture.com")


if __name__ == "__main__":
    main()
