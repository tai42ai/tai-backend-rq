"""RQ execution backend for the TAI ecosystem.

Importing this package registers everything on the global ``tai42_app`` handle
as a side-effect — that is how the host discovers the backend (the manifest's
``backend_module: tai42_backend_rq`` names this package to import):

* :class:`RqBackend` via ``@tai42_app.backends.register_backend`` (the
  ``worker`` / ``beat`` / ``dashboard`` launcher),
* the uniform ``backend_*`` tool surface via ``@tai42_app.tools.tool``,
* the ``sync_task`` / ``schedule_task`` / ``async_task`` BACKEND tool
  extensions via ``@tai42_app.extensions.extension``.
"""

import tai42_backend_rq.extensions
import tai42_backend_rq.tools  # noqa: F401  (import-time tool registration)
from tai42_backend_rq.backend import RqBackend

__all__ = ["RqBackend"]
