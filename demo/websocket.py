"""测试websocket连接binance，使用aiohttp库."""

import asyncio
import contextlib
import json

import aiohttp
from aiohttp.client_ws import ClientWSTimeout
from rich.console import Console

console = Console()


class WebSocketClient:
    """WebSocket客户端."""

    def __init__(self) -> None:
        """初始化WebSocket客户端."""
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self.heartbeat_task: asyncio.Task | None = None
        self.receive_task: asyncio.Task | None = None
        self.is_connected = False
        self.proxy = "http://127.0.0.1:6152"
        self.receive_lock = asyncio.Lock()

    async def connect(self, symbol: str = "btcusdt") -> bool:
        """连接websocket."""
        try:
            url = f"wss://stream.binance.com:9443/ws/{symbol}@kline_1m"
            console.print(f"[yellow]Connecting to {url} through proxy {self.proxy}...[/yellow]")

            connector = aiohttp.TCPConnector(ssl=False)
            async with (
                aiohttp.ClientSession(connector=connector) as session,
                session.ws_connect(
                    url,
                    proxy=self.proxy,
                    proxy_headers={"User-Agent": "Mozilla/5.0"},
                    heartbeat=20,  # Let aiohttp handle keepalive
                    timeout=ClientWSTimeout(),
                ) as ws,
            ):
                self.ws = ws
                self.is_connected = True

                console.print(
                    f"[green]WebSocket Details:\n"
                    f"- URL: {url}\n"
                    f"- State: {'Connected' if not ws.closed else 'Disconnected'}\n"
                    f"- Proxy: {self.proxy}\n"
                    f"- Protocol: {ws.protocol}\n"
                )

                # 只启动接收消息任务
                self.receive_task = asyncio.create_task(self.receive_messages())
                await self.receive_task  # 等待接收任务完成

            return True

        except Exception as e:
            console.print(f"[red]Connection error: {str(e)}[/red]")
            return False

    async def reconnect(self) -> None:
        """重连."""
        await self.close()
        await self.connect()

    async def receive_messages(self) -> None:
        """接收并处理消息."""
        try:
            while self.is_connected and self.ws is not None:
                try:
                    msg = await self.ws.receive()
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        message = json.loads(msg.data)
                        console.print(f"[green]Received: {json.dumps(message, indent=2)}[/green]")
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        console.print("[yellow]WebSocket connection closed[/yellow]")
                        await self.reconnect()
                        break
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        console.print("[red]WebSocket error occurred[/red]")
                        await self.reconnect()
                        break

                except (aiohttp.ClientError, aiohttp.WSServerHandshakeError) as e:
                    console.print(f"[red]Connection error: {str(e)}[/red]")
                    await self.reconnect()
                    break

        except Exception as e:
            console.print(f"[red]Error in message receiving: {str(e)}[/red]")
            await self.reconnect()

    async def close(self) -> None:
        """关闭连接."""
        self.is_connected = False

        if self.receive_task and not self.receive_task.done():
            self.receive_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.receive_task

        if self.ws and not self.ws.closed:
            await self.ws.close()


async def main() -> None:
    """主函数."""
    client = WebSocketClient()
    try:
        await client.connect(symbol="btcusdt")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
