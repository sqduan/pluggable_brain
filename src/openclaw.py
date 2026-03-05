"""
OpenClaw Connector

Integrates with OpenClaw to receive messages from various platforms.
"""
import os
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


# ==================== Data Types ====================

@dataclass
class Message:
    """A message from any source"""
    msg_id: str
    source: str          # email/feishu/discord/telegram
    sender: str
    content: str
    timestamp: datetime
    metadata: Dict[str, Any]


# ==================== Source Connectors ====================

class SourceConnector:
    """Base class for source connectors"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
    
    def fetch_new(self) -> List[Message]:
        """Fetch new messages"""
        raise NotImplementedError


class FeishuConnector(SourceConnector):
    """Feishu (飞书) connector"""
    
    def fetch_new(self) -> List[Message]:
        """Fetch messages from Feishu"""
        # In practice, this would connect to Feishu API
        # For now, read from local files
        messages = []
        
        # Check for new messages in source/feishu
        feishu_dir = Path(self.config.get('source_dir', 'source/feishu'))
        if not feishu_dir.exists():
            return messages
        
        for file in feishu_dir.glob("*.json"):
            try:
                with open(file) as f:
                    data = json.load(f)
                    
                    msg = Message(
                        msg_id=data.get('message_id', file.stem),
                        source='feishu',
                        sender=data.get('sender', 'unknown'),
                        content=data.get('content', ''),
                        timestamp=datetime.fromisoformat(data.get('timestamp', datetime.now().isoformat())),
                        metadata=data
                    )
                    messages.append(msg)
                    
            except Exception as e:
                logger.error(f"Error reading {file}: {e}")
        
        return messages


class EmailConnector(SourceConnector):
    """Email connector"""
    
    def fetch_new(self) -> List[Message]:
        """Fetch emails"""
        messages = []
        
        email_dir = Path(self.config.get('source_dir', 'source/emails'))
        if not email_dir.exists():
            return messages
        
        for file in email_dir.glob("*.json"):
            try:
                with open(file) as f:
                    data = json.load(f)
                    
                    msg = Message(
                        msg_id=data.get('message_id', file.stem),
                        source='email',
                        sender=data.get('from', 'unknown'),
                        content=data.get('body', ''),
                        timestamp=datetime.fromisoformat(data.get('date', datetime.now().isoformat())),
                        metadata={
                            'subject': data.get('subject', ''),
                            'to': data.get('to', []),
                            'labels': data.get('labels', [])
                        }
                    )
                    messages.append(msg)
                    
            except Exception as e:
                logger.error(f"Error reading {file}: {e}")
        
        return messages


class DiscordConnector(SourceConnector):
    """Discord connector"""
    
    def fetch_new(self) -> List[Message]:
        """Fetch Discord messages"""
        messages = []
        
        discord_dir = Path(self.config.get('source_dir', 'source/discord'))
        if not discord_dir.exists():
            return messages
        
        for file in discord_dir.glob("*.json"):
            try:
                with open(file) as f:
                    data = json.load(f)
                    
                    for msg_data in data.get('messages', []):
                        msg = Message(
                            msg_id=msg_data.get('id', file.stem),
                            source='discord',
                            sender=msg_data.get('author', 'unknown'),
                            content=msg_data.get('content', ''),
                            timestamp=datetime.fromisoformat(msg_data.get('timestamp', datetime.now().isoformat())),
                            metadata={'channel': msg_data.get('channel_id')}
                        )
                        messages.append(msg)
                        
            except Exception as e:
                logger.error(f"Error reading {file}: {e}")
        
        return messages


class TelegramConnector(SourceConnector):
    """Telegram connector"""
    
    def fetch_new(self) -> List[Message]:
        """Fetch Telegram messages"""
        messages = []
        
        tg_dir = Path(self.config.get('source_dir', 'source/telegram'))
        if not tg_dir.exists():
            return messages
        
        for file in tg_dir.glob("*.json"):
            try:
                with open(file) as f:
                    data = json.load(f)
                    
                    msg = Message(
                        msg_id=data.get('message_id', file.stem),
                        source='telegram',
                        sender=data.get('from', 'unknown'),
                        content=data.get('text', ''),
                        timestamp=datetime.fromisoformat(data.get('date', datetime.now().isoformat())),
                        metadata=data
                    )
                    messages.append(msg)
                    
            except Exception as e:
                logger.error(f"Error reading {file}: {e}")
        
        return messages


# ==================== OpenClaw Connector ====================

class OpenClawConnector:
    """
    Main connector to OpenClaw.
    
    Monitors multiple sources and ingests into brain.
    """
    
    def __init__(self, brain, source_dir: str = None):
        self.brain = brain
        self.source_dir = Path(source_dir) if source_dir else Path(__file__).parent.parent.parent / "source"
        
        # Create source directories
        for source in ['email', 'feishu', 'discord', 'telegram', 'images', 'audio']:
            (self.source_dir / source).mkdir(parents=True, exist_ok=True)
        
        # Source connectors
        self.connectors = {
            'email': EmailConnector({'source_dir': str(self.source_dir)}),
            'feishu': FeishuConnector({'source_dir': str(self.source_dir)}),
            'discord': DiscordConnector({'source_dir': str(self.source_dir)}),
            'telegram': TelegramConnector({'source_dir': str(self.source_dir)}),
        }
        
        # Track processed messages
        self.processed_file = self.source_dir / "processed.json"
        self._processed_ids = self._load_processed()
    
    def _load_processed(self) -> set:
        """Load processed message IDs"""
        if self.processed_file.exists():
            try:
                with open(self.processed_file) as f:
                    return set(json.load(f))
            except:
                return set()
        return set()
    
    def _save_processed(self):
        """Save processed message IDs"""
        with open(self.processed_file, 'w') as f:
            json.dump(list(self._processed_ids), f)
    
    def sync(self, sources: List[str] = None) -> Dict[str, int]:
        """
        Sync messages from sources to brain.
        
        Args:
            sources: List of sources to sync (default: all)
            
        Returns:
            Dict of source -> count synced
        """
        if sources is None:
            sources = list(self.connectors.keys())
        
        stats = {}
        
        for source in sources:
            if source not in self.connectors:
                logger.warning(f"Unknown source: {source}")
                continue
            
            connector = self.connectors[source]
            messages = connector.fetch_new()
            
            count = 0
            for msg in messages:
                if msg.msg_id in self._processed_ids:
                    continue
                
                # Ingest based on source
                self._ingest_message(msg)
                
                self._processed_ids.add(msg.msg_id)
                count += 1
            
            stats[source] = count
            logger.info(f"Synced {count} messages from {source}")
        
        self._save_processed()
        return stats
    
    def _ingest_message(self, msg: Message):
        """Ingest a message into brain"""
        
        if msg.source == 'email':
            self.brain.ingest_email(
                subject=msg.metadata.get('subject', 'No Subject'),
                body=msg.content,
                email_id=msg.msg_id,
                sender=msg.sender,
                recipients=msg.metadata.get('to', []),
                timestamp=msg.timestamp,
                labels=msg.metadata.get('labels', [])
            )
        
        elif msg.source == 'feishu':
            # Treat Feishu as chat
            self.brain.ingest_chat(
                messages=[{
                    'sender': msg.sender,
                    'content': msg.content,
                    'time': msg.timestamp.strftime('%H:%M')
                }],
                chat_id=msg.msg_id,
                platform='feishu',
                participants=[msg.sender],
                timestamp=msg.timestamp
            )
        
        elif msg.source == 'discord':
            self.brain.ingest_chat(
                messages=[{
                    'sender': msg.sender,
                    'content': msg.content,
                    'time': msg.timestamp.strftime('%H:%M')
                }],
                chat_id=msg.msg_id,
                platform='discord',
                participants=[msg.sender],
                timestamp=msg.timestamp
            )
        
        elif msg.source == 'telegram':
            self.brain.ingest_chat(
                messages=[{
                    'sender': msg.sender,
                    'content': msg.content,
                    'time': msg.timestamp.strftime('%H:%M')
                }],
                chat_id=msg.msg_id,
                platform='telegram',
                participants=[msg.sender],
                timestamp=msg.timestamp
            )
    
    def add_message(self, source: str, content: str, 
                  sender: str = "unknown",
                  metadata: Dict = None):
        """
        Manually add a message to the queue.
        
        This can be called from OpenClaw when new messages arrive.
        """
        msg_id = f"{source}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        msg = Message(
            msg_id=msg_id,
            source=source,
            sender=sender,
            content=content,
            timestamp=datetime.now(),
            metadata=metadata or {}
        )
        
        self._ingest_message(msg)
        self._processed_ids.add(msg_id)
        self._save_processed()
        
        logger.info(f"Added message {msg_id} from {source}")
        
        return msg_id


def create_connector(brain, config: Dict = None) -> OpenClawConnector:
    """Create an OpenClaw connector"""
    return OpenClawConnector(brain, config.get('source_dir') if config else None)


if __name__ == "__main__":
    # Test
    from brain import Brain
    
    brain = Brain()
    connector = OpenClawConnector(brain)
    
    # Sync
    stats = connector.sync()
    print(f"Synced: {stats}")
