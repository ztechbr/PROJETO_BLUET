from __future__ import annotations

from server import create_app

# WSGI app (usado por gunicorn/uwsgi e também pelo `python app.py`)
app = create_app()

if __name__ == "__main__":
    import os

    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8001")), debug=True)
