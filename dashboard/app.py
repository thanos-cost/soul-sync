import os
import sqlite3

from flask import Flask, redirect, url_for

from db import close_db, get_db


def create_app():
    """Flask application factory."""
    app = Flask(__name__)

    # Database path — injected via environment variable in Docker
    app.config["DB_PATH"] = os.environ.get("DB_PATH", "/data/songs.db")

    # Secret key for flash() session messages — not sensitive since this
    # is a local-only dashboard, but Flask requires it.
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "soulsync-dashboard")

    # Register the per-request DB connection teardown
    app.teardown_appcontext(close_db)

    # Register blueprints
    from routes.sources import bp as sources_bp
    from routes.songs import bp as songs_bp
    from routes.search import bp as search_bp
    from routes.downloads import bp as downloads_bp
    from routes.activity import bp as activity_bp
    from routes.settings import bp as settings_bp

    app.register_blueprint(sources_bp)
    app.register_blueprint(songs_bp)
    app.register_blueprint(search_bp)
    app.register_blueprint(downloads_bp)
    app.register_blueprint(activity_bp)
    app.register_blueprint(settings_bp)

    @app.route("/")
    def index():
        """Redirect to the first source type, or a no-sources page."""
        try:
            db = get_db()
            row = db.execute(
                "SELECT DISTINCT type FROM sources WHERE enabled = 1 AND deleted_at IS NULL ORDER BY type LIMIT 1"
            ).fetchone()
            if row:
                return redirect(url_for("songs.source_view", source_type=row["type"]))
        except sqlite3.OperationalError:
            # DB not yet initialised (sources table doesn't exist yet) — fall through
            pass
        return redirect(url_for("songs.no_sources"))

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
