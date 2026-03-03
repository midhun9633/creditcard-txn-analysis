# tests/conftest.py
import os, sys

# insert project root into sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))