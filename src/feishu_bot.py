"""
Feishu Bot Connector - Receive messages from Feishu and ingest into Brain

This module provides:
1. Webhook server for Feishu events (messages, mentions)
2. Message parsing and conversion to chunks
3. Integration with Brain

Usage:
    from pluggable_brain import Brain, FeishuBot
    
    brain = Brain()
    bot = FeishuBot(brain)
    bot.run()  # Start webhook server
"""
import os
import json
import hashlib
import hmac
import time
import threading
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
from flask import Flask, request, jsonify, Response
import logging

logger = logging.getLogger(__name__)

# Optional dependencies
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


# ==================== Feishu Events ====================

class FeishuEventType(Enum):
    """Feishu event types"""
    MESSAGE = "im.message"
    USER = "im.user"
    MESSAGE_REACTION = "im.message_reaction"
    UNKNOWN = "unknown"


class MessageType(Enum):
    """Message types in Feishu"""
    TEXT = "text"
    IMAGE = "image"
    FILE = "file"
    VOICE = "voice"
    MEDIA = "media"
    POST = "post"
    SHARE_USER = "share_user"
    SHARE_CHAT = "share_chat"
    UNKNOWN = "unknown"


# ==================== Data Structures ====================

@dataclass
class FeishuMessage:
    """A message from Feishu"""
    message_id: str
    chat_id: str
    chat_type: str           # group / private
    message_type: str
    sender_id: str
    sender_name: str
    content: str
    raw_content: Dict        # Raw Feishu message payload
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_group(self) -> bool:
        return self.chat_type == "group"


@dataclass  
class FeishuConfig:
    """Feishu bot configuration"""
    app_id: str
    app_secret: str
    verification_token: str
    webhook_path: str = "/webhook/feishu"
    host: str = "0.0.0.0"
    port: int = 5000


# ==================== Feishu Bot ====================

class FeishuBot:
    """
    Feishu Bot - Receives messages from Feishu and stores in Brain.
    
    Usage:
        brain = Brain()
        bot = FeishuBot(
            brain=brain,
            app_id="your_app_id",
            app_secret="your_app_secret", 
            verification_token="your_verification_token"
        )
        bot.run()
    
    Or use the built-in HTTP server:
        bot = FeishuBot(brain, config_path="config/feishu.json")
        bot.start_webhook()
    """
    
    def __init__(
        self,
        brain,
        app_id: str = None,
        app_secret: str = None,
        verification_token: str = None,
        config_path: str = None,
        host: str = "0.0.0.0",
        port: int = 5000,
    ):
        self.brain = brain
        self.config = None
        self.app = None
        self._token_cache = {}
        self._token_expires_at = 0
        
        # Load config
        if config_path:
            self._load_config(config_path)
        elif app_id and app_secret and verification_token:
            self.config = FeishuConfig(
                app_id=app_id,
                app_secret=app_secret,
                verification_token=verification_token,
                host=host,
                port=port,
            )
        else:
            # Try default path
            default_path = Path("~/.config/pluggable_brain/feishu.json").expanduser()
            if default_path.exists():
                self._load_config(str(default_path))
    
    def _load_config(self, config_path: str):
        """Load config from file"""
        with open(config_path) as f:
            data = json.load(f)
            self.config = FeishuConfig(
                app_id=data["app_id"],
                app_secret=data["app_secret"],
                verification_token=data["verification_token"],
                webhook_path=data.get("webhook_path", "/webhook/feishu"),
                host=data.get("host", "0.0.0.0"),
                port=data.get("port", 5000),
            )
    
    # ==================== Token Management ====================
    
    def get_tenant_access_token(self) -> str:
        """Get tenant access token (caches for 2 hours)"""
        now = time.time()
        
        if self._token_cache.get("tenant_access_token") and now < self._token_expires_at:
            return self._token_cache["tenant_access_token"]
        
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        headers = {"Content-Type": "application/json; charset=utf-8"}
        payload = {
            "app_id": self.config.app_id,
            "app_secret": self.config.app_secret,
        }
        
        response = requests.post(url, headers=headers, json=payload)
        data = response.json()
        
        if data.get("code") != 0:
            raise Exception(f"Failed to get token: {data}")
        
        token = data["tenant_access_token"]
        expires_in = data.get("expire", 7200) - 300  # 5 min buffer
        
        self._token_cache["tenant_access_token"] = token
        self._token_expires_at = now + expires_in
        
        return token
    
    def get_user_info(self, user_id: str) -> Dict:
        """Get user info by user_id"""
        token = self.get_tenant_access_token()
        
        url = f"https://open.feishu.cn/open-apis/contact/v3/users/{user_id}"
        headers = {"Authorization": f"Bearer {token}"}
        
        response = requests.get(url, headers=headers)
        data = response.json()
        
        if data.get("code") == 0:
            return data.get("data", {})
        return {"user_id": user_id, "name": "Unknown"}
    
    # ==================== Message Parsing ====================
    
    def parse_message(self, event: Dict) -> Optional[FeishuMessage]:
        """Parse Feishu event to FeishuMessage"""
        try:
            msg_type = event.get("type")
            if msg_type != FeishuEventType.MESSAGE.value:
                return None
            
            message = event.get("message", {})
            
            # Get basic info
            message_id = message.get("message_id")
            chat_id = message.get("chat_id")
            chat_type = message.get("chat_type", "private")
            sender_id = message.get("sender_id", {}).get("user_id", "")
            msg_type = message.get("message_type", "text")
            content = message.get("content", "")
            create_time = message.get("create_time")
            
            # Parse content based on type
            text_content = ""
            if msg_type == "text":
                content_dict = json.loads(content) if content else {}
                text_content = content_dict.get("text", "")
            elif msg_type in ["post", "share_user", "share_chat"]:
                content_dict = json.loads(content) if content else {}
                text_content = json.dumps(content_dict, ensure_ascii=False)
            else:
                text_content = f"[{msg_type}]"
            
            # Get sender name
            sender_name = sender_id
            try:
                user_info = self.get_user_info(sender_id)
                sender_name = user_info.get("name", sender_id)
            except:
                pass
            
            # Parse timestamp
            timestamp = datetime.now()
            if create_time:
                try:
                    timestamp = datetime.fromtimestamp(int(create_time) / 1000)
                except:
                    pass
            
            return FeishuMessage(
                message_id=message_id,
                chat_id=chat_id,
                chat_type=chat_type,
                message_type=msg_type,
                sender_id=sender_id,
                sender_name=sender_name,
                content=text_content,
                raw_content=event,
                timestamp=timestamp,
                metadata={
                    "platform": "feishu",
                    "chat_id": chat_id,
                }
            )
            
        except Exception as e:
            logger.error(f"Error parsing message: {e}")
            return None
    
    # ==================== Webhook Server ====================
    
    def _create_app(self) -> Flask:
        """Create Flask app"""
        app = Flask(__name__)
        app.config["JSON_AS_ASCII"] = False
        
        @app.route(self.config.webhook_path, methods=["POST"])
        def webhook():
            # Verify request
            if not self._verify_request(request):
                return jsonify({"code": 1, "msg": "verification failed"}), 401
            
            event = request.json
            event_type = event.get("type")
            
            # Handle verification challenge
            if event.get("challenge"):
                return jsonify({"challenge": event["challenge"]})
            
            # Process message event
            if event_type == FeishuEventType.MESSAGE.value:
                self._handle_message(event)
            
            return jsonify({"code": 0})
        
        return app
    
    def _verify_request(self, request) -> bool:
        """Verify Feishu request"""
        # Check verification token for URL verification
        token = request.headers.get("X-Lark-Verification-Token", "")
        if token and token == self.config.verification_token:
            return True
        
        # Check signature for events
        signature = request.headers.get("X-Lark-Signature", "")
        timestamp = request.headers.get("X-Lark-Timestamp", "")
        
        if not signature or not timestamp:
            return False
        
        # Build string to sign
        sign_str = timestamp + self.config.app_secret
        my_signature = hmac.new(
            sign_str.encode(),
            digestmod=hashlib.sha256
        ).hexdigest()
        
        return hmac.compare_digest(signature, my_signature)
    
    def _handle_message(self, event: Dict):
        """Handle incoming message event"""
        msg = self.parse_message(event)
        
        if not msg:
            return
        
        # Skip messages from bots (optional)
        if msg.sender_id.startswith("ou_") is False:
            return
        
        # Store in brain
        self._ingest_to_brain(msg)
        
        logger.info(f"Ingested Feishu message: {msg.message_id} from {msg.sender_name}")
    
    def _ingest_to_brain(self, msg: FeishuMessage):
        """Ingest Feishu message to brain"""
        # Use brain's ingest_chat method
        messages = [{
            "sender": msg.sender_name,
            "content": msg.content,
            "timestamp": msg.timestamp.isoformat(),
        }]
        
        self.brain.ingest_chat(
            messages=messages,
            chat_id=msg.chat_id,
            platform="feishu",
            participants=[msg.sender_name],
            timestamp=msg.timestamp,
        )
    
    # ==================== Run Server ====================
    
    def run(self, host: str = None, port: int = None):
        """Run the webhook server"""
        if not self.config:
            raise ValueError("No config provided")
        
        host = host or self.config.host
        port = port or self.config.port
        
        self.app = self._create_app()
        
        logger.info(f"Starting Feishu bot on {host}:{port}")
        self.app.run(host=host, port=port, debug=False)
    
    def start_webhook(self, background: bool = True):
        """Start webhook server (optionally in background)"""
        if background:
            thread = threading.Thread(target=self.run, daemon=True)
            thread.start()
            return thread
        else:
            self.run()
    
    # ==================== Reply to Message ====================
    
    def reply_message(self, message_id: str, content: str):
        """Reply to a Feishu message"""
        token = self.get_tenant_access_token()
        
        url = "https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        payload = {
            "msg_type": "text",
            "content": json.dumps({"text": content}),
        }
        
        response = requests.post(
            url.format(message_id=message_id),
            headers=headers,
            json=payload
        )
        
        return response.json()
    
    def send_message(self, chat_id: str, content: str, msg_type: str = "text"):
        """Send a message to a chat"""
        token = self.get_tenant_access_token()
        
        url = "https://open.feishu.cn/open-apis/im/v1/messages"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        payload = {
            "receive_id": chat_id,
            "msg_type": msg_type,
            "content": json.dumps({"text": content}) if msg_type == "text" else json.dumps(content),
        }
        
        response = requests.post(url, headers=headers, json=payload)
        return response.json()


# ==================== Convenience Functions ====================

def create_feishu_bot(
    brain,
    app_id: str = None,
    app_secret: str = None,
    verification_token: str = None,
    config_path: str = None,
) -> FeishuBot:
    """Create a Feishu bot instance"""
    return FeishuBot(
        brain=brain,
        app_id=app_id,
        app_secret=app_secret,
        verification_token=verification_token,
        config_path=config_path,
    )


def create_config(
    app_id: str,
    app_secret: str,
    verification_token: str,
    output_path: str = "~/.config/pluggable_brain/feishu.json",
) -> str:
    """Create config file"""
    config = {
        "app_id": app_id,
        "app_secret": app_secret,
        "verification_token": verification_token,
        "webhook_path": "/webhook/feishu",
        "host": "0.0.0.0",
        "port": 5000,
    }
    
    path = Path(output_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, indent=2, ensure_ascii=False))
    
    return str(path)
