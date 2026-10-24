"""
Vercel Python Serverless Entrypoint for Real-Time Pricing Engine Dashboard.
Serves the live interactive market flow dashboard on Vercel.
"""

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HTML_FILE = os.path.join(BASE_DIR, "index.html")
if not os.path.exists(HTML_FILE):
    HTML_FILE = os.path.join(BASE_DIR, "dashboard", "index.html")


async def app(scope, receive, send):
    """ASGI application serving the trading dashboard."""
    if scope.get("type") == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return

    if scope.get("type") == "http":
        try:
            with open(HTML_FILE, "rb") as f:
                body = f.read()
        except Exception as e:
            body = f"<h1>Error loading dashboard: {e}</h1>".encode("utf-8")

        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [
                [b"content-type", b"text/html; charset=utf-8"],
                [b"cache-control", b"public, max-age=0, must-revalidate"],
            ],
        })
        await send({
            "type": "http.response.body",
            "body": body,
        })


# Standard entrypoint aliases expected by Vercel
handler = app
application = app
