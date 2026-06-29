"""Render 部署入口"""

from server.server import create_app
import os
from aiohttp import web

if __name__ == "__main__":
    app = create_app()
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, host="0.0.0.0", port=port)
