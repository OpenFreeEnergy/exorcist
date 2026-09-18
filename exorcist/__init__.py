from importlib.metadata import version
from .taskdb import TaskStatusDB, NoStatusChange
from .models import TaskStatus

__version__ = version("exorcist")
