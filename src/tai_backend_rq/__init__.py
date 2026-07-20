"""RQ execution backend for the TAI ecosystem.

Importing this package registers everything on the global ``tai_app`` handle
as a side-effect — that is how the host discovers the backend (the manifest's
``backend_module: tai_backend_rq`` names this package to import):

* :class:`RqBackend` via ``@tai_app.backends.register_backend`` (the
  ``worker`` / ``beat`` / ``dashboard`` launcher),
* the uniform ``backend_*`` tool surface via ``@tai_app.tools.tool``,
* the ``sync_task`` / ``schedule_task`` / ``async_task`` BACKEND tool
  extensions via ``@tai_app.extensions.extension``.
"""

import tai_backend_rq.extensions
import tai_backend_rq.tools  # noqa: F401  (import-time tool registration)
from tai_backend_rq.backend import RqBackend

__all__ = ["RqBackend"]
