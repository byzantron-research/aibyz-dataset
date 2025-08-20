"""Namespace package initialiser for the hybrid dataset code.

When this package is added to ``sys.path`` (for example by using
``sys.path.insert(0, "./codes")`` as in the provided tests), this
initializer ensures that the nested ``codes`` subdirectory (which
contains the actual source modules) is also added to ``sys.path``. This
allows top‑level imports such as ``import common`` or ``import collectors``
to resolve correctly without requiring environment‑wide path
manipulation.
"""

import os
import sys

# Determine the absolute path to the nested source directory. The current
# file resides at ``codes/codes/__init__.py``, so we navigate one level up
# (the outer ``codes`` directory) and then down into the inner ``codes``
# subdirectory.
_outer_dir = os.path.dirname(__file__)
_inner_src = os.path.join(_outer_dir, "codes")
if os.path.isdir(_inner_src) and _inner_src not in sys.path:
    sys.path.insert(0, _inner_src)