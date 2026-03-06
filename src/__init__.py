"""
Pluggable Brain - Personal External Brain

A personal knowledge management system.
"""
from .brain import Brain, MemoryFragment, Chunk, ChunkType, create_brain
from .openclaw import OpenClawConnector, create_connector
from .email_connector import (
    EmailConnector, 
    EmailProvider, 
    EmailMessage, 
    EmailAccount,
    create_email_connector,
    list_supported_providers,
)

__version__ = "1.0.0"

__all__ = [
    "Brain",
    "MemoryFragment",
    "Chunk",
    "ChunkType",
    "create_brain",
    "OpenClawConnector",
    "create_connector",
    "EmailConnector",
    "EmailProvider",
    "EmailMessage",
    "EmailAccount",
    "create_email_connector",
    "list_supported_providers",
]
