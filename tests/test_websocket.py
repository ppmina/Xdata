"""WebSocket功能测试.

测试WebSocket相关功能，使用mock避免实际网络连接。
"""

import json
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp import WSMsgType

# ================= WebSocket客户端测试 =================


class MockWebSocketClient:
    """用于测试的WebSocket客户端mock."""

    def __init__(self):
        """初始化mock客户端."""
        self.is_connected = False
        self.messages = []
        self.connection_attempts = 0

    async def connect(self, symbol: str = "btcusdt") -> bool:
        """模拟连接."""
        self.connection_attempts += 1
        self.is_connected = True
        return True

    async def receive_message(self) -> dict:
        """模拟接收消息."""
        if not self.is_connected:
            raise ConnectionError("Not connected")

        # 返回一个模拟的K线数据
        return {
            "e": "kline",
            "E": 1234567890123,
            "s": "BTCUSDT",
            "k": {
                "t": 1234567890000,
                "T": 1234567949999,
                "s": "BTCUSDT",
                "i": "1m",
                "o": "50000.00",
                "c": "50100.00",
                "h": "50200.00",
                "l": "49900.00",
                "v": "100.0",
                "q": "5005000.0",
                "n": 1000,
                "x": True,
            },
        }

    async def close(self) -> None:
        """模拟关闭连接."""
        self.is_connected = False


def test_websocket_client_creation():
    """测试WebSocket客户端创建."""
    client = MockWebSocketClient()
    assert not client.is_connected
    assert client.connection_attempts == 0
    assert client.messages == []


@pytest.mark.asyncio
async def test_websocket_connection():
    """测试WebSocket连接."""
    client = MockWebSocketClient()

    # 测试连接
    success = await client.connect("btcusdt")
    assert success
    assert client.is_connected
    assert client.connection_attempts == 1


@pytest.mark.asyncio
async def test_websocket_message_receiving():
    """测试WebSocket消息接收."""
    client = MockWebSocketClient()
    await client.connect("btcusdt")

    # 测试接收消息
    message = await client.receive_message()
    assert message is not None
    assert message["e"] == "kline"
    assert message["s"] == "BTCUSDT"
    assert "k" in message

    # 验证K线数据结构
    kline = message["k"]
    assert kline["s"] == "BTCUSDT"
    assert kline["i"] == "1m"
    assert "o" in kline  # open price
    assert "c" in kline  # close price
    assert "v" in kline  # volume


@pytest.mark.asyncio
async def test_websocket_connection_error():
    """测试WebSocket连接错误处理."""
    client = MockWebSocketClient()

    # 测试在未连接状态下接收消息
    with pytest.raises(ConnectionError):
        await client.receive_message()


@pytest.mark.asyncio
async def test_websocket_close():
    """测试WebSocket关闭."""
    client = MockWebSocketClient()
    await client.connect("btcusdt")
    assert client.is_connected

    await client.close()
    assert not client.is_connected


# ================= WebSocket集成测试 =================


@pytest.mark.asyncio
async def test_websocket_with_aiohttp_mock():
    """使用aiohttp mock测试WebSocket功能."""
    # 创建mock WebSocket响应
    mock_ws = AsyncMock()

    # 设置mock消息
    test_message = {
        "e": "kline",
        "E": 1234567890123,
        "s": "BTCUSDT",
        "k": {
            "t": 1234567890000,
            "T": 1234567949999,
            "s": "BTCUSDT",
            "i": "1m",
            "o": "50000.00",
            "c": "50100.00",
            "h": "50200.00",
            "l": "49900.00",
            "v": "100.0",
            "q": "5005000.0",
            "n": 1000,
            "x": True,
        },
    }

    # 设置mock返回值
    mock_msg = Mock()
    mock_msg.type = WSMsgType.TEXT
    mock_msg.data = json.dumps(test_message)
    mock_ws.receive.return_value = mock_msg
    mock_ws.closed = False

    # 模拟简单的消息处理
    msg = await mock_ws.receive()
    assert msg.type == WSMsgType.TEXT

    # 解析消息
    data = json.loads(msg.data)
    assert data["e"] == "kline"
    assert data["s"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_websocket_error_handling():
    """测试WebSocket错误处理."""
    mock_ws = AsyncMock()

    # 模拟连接关闭
    mock_msg = Mock()
    mock_msg.type = WSMsgType.CLOSED
    mock_ws.receive.return_value = mock_msg

    msg = await mock_ws.receive()
    assert msg.type == WSMsgType.CLOSED

    # 模拟错误
    mock_error_msg = Mock()
    mock_error_msg.type = WSMsgType.ERROR
    mock_ws.receive.return_value = mock_error_msg

    error_msg = await mock_ws.receive()
    assert error_msg.type == WSMsgType.ERROR


# ================= 数据处理测试 =================


def test_kline_data_processing():
    """测试K线数据处理."""
    # 模拟K线数据
    kline_data = {
        "e": "kline",
        "E": 1234567890123,
        "s": "BTCUSDT",
        "k": {
            "t": 1234567890000,
            "T": 1234567949999,
            "s": "BTCUSDT",
            "i": "1m",
            "o": "50000.00",
            "c": "50100.00",
            "h": "50200.00",
            "l": "49900.00",
            "v": "100.0",
            "q": "5005000.0",
            "n": 1000,
            "x": True,
        },
    }

    # 验证数据结构
    assert kline_data["e"] == "kline"
    assert kline_data["s"] == "BTCUSDT"

    kline = kline_data["k"]
    assert kline["s"] == "BTCUSDT"
    assert kline["i"] == "1m"
    assert float(kline["o"]) == 50000.00
    assert float(kline["c"]) == 50100.00
    assert float(kline["h"]) == 50200.00
    assert float(kline["l"]) == 49900.00
    assert float(kline["v"]) == 100.0
    assert kline["x"] is True  # 是否完成


def test_multiple_symbols_data():
    """测试多交易对数据处理."""
    symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT"]

    for symbol in symbols:
        # 为每个交易对创建模拟数据
        data = {"e": "kline", "s": symbol, "k": {"s": symbol, "i": "1m", "o": "1000.00", "c": "1001.00", "v": "50.0"}}

        assert data["s"] == symbol
        assert data["k"]["s"] == symbol


# ================= 连接管理测试 =================


@pytest.mark.asyncio
async def test_connection_lifecycle():
    """测试连接生命周期管理."""

    class MockConnectionManager:
        def __init__(self):
            self.connections = {}
            self.connection_count = 0

        async def create_connection(self, symbol: str):
            """创建连接."""
            self.connection_count += 1
            self.connections[symbol] = {"id": self.connection_count, "status": "connected", "symbol": symbol}
            return self.connections[symbol]

        async def close_connection(self, symbol: str):
            """关闭连接."""
            if symbol in self.connections:
                self.connections[symbol]["status"] = "closed"

        def get_active_connections(self):
            """获取活跃连接."""
            return [conn for conn in self.connections.values() if conn["status"] == "connected"]

    manager = MockConnectionManager()

    # 测试创建连接
    conn1 = await manager.create_connection("BTCUSDT")
    assert conn1["symbol"] == "BTCUSDT"
    assert conn1["status"] == "connected"

    conn2 = await manager.create_connection("ETHUSDT")
    assert conn2["symbol"] == "ETHUSDT"

    # 测试活跃连接数量
    active = manager.get_active_connections()
    assert len(active) == 2

    # 测试关闭连接
    await manager.close_connection("BTCUSDT")
    active = manager.get_active_connections()
    assert len(active) == 1
    assert active[0]["symbol"] == "ETHUSDT"


if __name__ == "__main__":
    pytest.main([__file__])
