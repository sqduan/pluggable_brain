"""
Email Connector - Multi-provider email ingestion for Pluggable Brain

Supported providers:
- International: Gmail, Outlook, Yahoo, iCloud, IMAP/SMTP generic
- Chinese: QQ邮箱, 163邮箱, 126邮箱, 阿里云邮箱, Gmail
"""
import os
import pickle
import json
import email
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)

# Email libraries (optional)
try:
    import imaplib
    import smtplib
    import ssl
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.parser import Parser
    from email.policy import default
    HAS_IMAPLIB = True
except ImportError:
    HAS_IMAPLIB = False

try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    HAS_GOOGLE_API = True
except ImportError:
    HAS_GOOGLE_API = False


# ==================== Email Providers ====================

class EmailProvider(Enum):
    GMAIL = "gmail"
    OUTLOOK = "outlook"
    QQ = "qq"
    ALIYUN = "aliyun"
    NETEASE_163 = "163"
    NETEASE_126 = "126"
    YAHOO = "yahoo"
    ICLOUD = "icloud"
    IMAP = "imap"  # Generic IMAP


# Provider configurations
PROVIDER_CONFIGS = {
    EmailProvider.GMAIL: {
        "imap_host": "imap.gmail.com",
        "smtp_host": "smtp.gmail.com",
        "imap_port": 993,
        "smtp_port": 587,
        "use_ssl": True,
    },
    EmailProvider.OUTLOOK: {
        "imap_host": "outlook.office365.com",
        "smtp_host": "smtp.office365.com",
        "imap_port": 993,
        "smtp_port": 587,
        "use_ssl": True,
    },
    EmailProvider.QQ: {
        "imap_host": "imap.qq.com",
        "smtp_host": "smtp.qq.com",
        "imap_port": 993,
        "smtp_port": 587,
        "use_ssl": True,
    },
    EmailProvider.ALIYUN: {
        "imap_host": "imap.aliyun.com",
        "smtp_host": "smtp.aliyun.com",
        "imap_port": 993,
        "smtp_port": 465,
        "use_ssl": True,
    },
    EmailProvider.NETEASE_163: {
        "imap_host": "imap.163.com",
        "smtp_host": "smtp.163.com",
        "imap_port": 993,
        "smtp_port": 465,
        "use_ssl": True,
    },
    EmailProvider.NETEASE_126: {
        "imap_host": "imap.126.com",
        "smtp_host": "smtp.126.com",
        "imap_port": 993,
        "smtp_port": 465,
        "use_ssl": True,
    },
    EmailProvider.YAHOO: {
        "imap_host": "imap.mail.yahoo.com",
        "smtp_host": "smtp.mail.yahoo.com",
        "imap_port": 993,
        "smtp_port": 587,
        "use_ssl": True,
    },
    EmailProvider.ICLOUD: {
        "imap_host": "imap.mail.me.com",
        "smtp_host": "smtp.mail.me.com",
        "imap_port": 993,
        "smtp_port": 587,
        "use_ssl": True,
    },
}


# ==================== Data Structures ====================

@dataclass
class EmailAccount:
    """Email account configuration"""
    email: str
    provider: EmailProvider
    # For OAuth (Gmail, Outlook)
    credentials_path: str = None
    token_path: str = None
    # For IMAP (username/password)
    username: str = None
    # Password is stored separately, not in config file
    use_oauth: bool = False
    
    def to_dict(self) -> dict:
        return {
            "email": self.email,
            "provider": self.provider.value,
            "credentials_path": self.credentials_path,
            "token_path": self.token_path,
            "username": self.username,
            "use_oauth": self.use_oauth,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'EmailAccount':
        return cls(
            email=data["email"],
            provider=EmailProvider(data["provider"]),
            credentials_path=data.get("credentials_path"),
            token_path=data.get("token_path"),
            username=data.get("username"),
            use_oauth=data.get("use_oauth", False),
        )


@dataclass
class EmailMessage:
    """Parsed email message"""
    message_id: str
    subject: str
    from_: str
    to: str
    cc: str = None
    bcc: str = None
    date: datetime = None
    body_text: str = None
    body_html: str = None
    labels: List[str] = field(default_factory=list)
    attachments: List[Dict[str, Any]] = field(default_factory=list)
    thread_id: str = None
    metadata: Dict[str, Any] = field(default_factory=dict)


# ==================== Email Connector ====================

class EmailConnector:
    """
    Connect to email accounts and sync messages.
    
    Usage:
        connector = EmailConnector(config_dir="~/.config/pluggable_brain")
        
        # First time: configure account
        connector.configure_account(
            email="user@gmail.com",
            provider=EmailProvider            credentials_path="path.GMAIL,
/to/credentials.json"
        )
        
        # Connect and sync
        connector.connect()
        
        # Check if full sync needed
        if connector.needs_full_sync():
            # Ask user: "是否全量检索邮箱内容?"
            # If yes:
            messages = connector.fetch_all_emails()
            for msg in messages:
                brain.ingest_email(msg)
    """
    
    def __init__(self, config_dir: str = None):
        self.config_dir = Path(config_dir or os.path.expanduser("~/.config/pluggable_brain"))
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        self.accounts_file = self.config_dir / "email_accounts.pkl"
        self.accounts: Dict[str, EmailAccount] = {}
        self.current_account: EmailAccount = None
        self._imap_connection = None
        self._gmail_service = None
        
        self._load_accounts()
    
    def _load_accounts(self):
        """Load saved accounts"""
        if self.accounts_file.exists():
            try:
                with open(self.accounts_file, 'rb') as f:
                    data = pickle.load(f)
                    self.accounts = {k: EmailAccount.from_dict(v) for k, v in data.items()}
            except Exception as e:
                logger.warning(f"Failed to load accounts: {e}")
    
    def _save_accounts(self):
        """Save accounts to disk"""
        data = {k: v.to_dict() for k, v in self.accounts.items()}
        with open(self.accounts_file, 'wb') as f:
            pickle.dump(data, f)
    
    # ==================== Configuration ====================
    
    def configure_account(
        self,
        email: str,
        provider: EmailProvider = None,
        username: str = None,
        password: str = None,
        credentials_path: str = None,
        token_path: str = None,
    ) -> EmailAccount:
        """
        Configure a new email account.
        
        For Gmail/Outlook with OAuth:
            configure_account(
                email="user@gmail.com",
                provider=EmailProvider.GMAIL,
                credentials_path="path/to/credentials.json",
                token_path="path/to/token.json"
            )
        
        For IMAP with password:
            configure_account(
                email="user@163.com",
                provider=EmailProvider.NETEASE_163,
                username="user@163.com",
                password="xxxx"
            )
        """
        # Auto-detect provider from email
        if provider is None:
            provider = self._detect_provider(email)
        
        # For OAuth providers, token_path defaults
        if provider in [EmailProvider.GMAIL, EmailProvider.OUTLOOK]:
            use_oauth = True
            if token_path is None:
                token_path = str(self.config_dir / f"{email}_token.pkl")
        else:
            use_oauth = False
        
        account = EmailAccount(
            email=email,
            provider=provider,
            username=username or email,
            credentials_path=credentials_path,
            token_path=token_path,
            use_oauth=use_oauth,
        )
        
        self.accounts[email] = account
        self._save_accounts()
        
        # Store password securely (simple encoding - in production use keyring)
        if password:
            self._store_password(email, password)
        
        logger.info(f"Configured email account: {email} ({provider.value})")
        return account
    
    def _detect_provider(self, email: str) -> EmailProvider:
        """Auto-detect email provider from address"""
        domain = email.split("@")[-1].lower()
        
        provider_map = {
            "gmail.com": EmailProvider.GMAIL,
            "googlemail.com": EmailProvider.GMAIL,
            "outlook.com": EmailProvider.OUTLOOK,
            "office365.com": EmailProvider.OUTLOOK,
            "hotmail.com": EmailProvider.OUTLOOK,
            "live.com": EmailProvider.OUTLOOK,
            "qq.com": EmailProvider.QQ,
            "aliyun.com": EmailProvider.ALIYUN,
            "163.com": EmailProvider.NETEASE_163,
            "126.com": EmailProvider.NETEASE_126,
            "yahoo.com": EmailProvider.YAHOO,
            "icloud.com": EmailProvider.ICLOUD,
            "me.com": EmailProvider.ICLOUD,
        }
        
        return provider_map.get(domain, EmailProvider.IMAP)
    
    def _store_password(self, email: str, password: str):
        """Store password securely"""
        import base64
        password_file = self.config_dir / f"{email}.pwd"
        # Simple encoding - not secure for production, use keyring
        encoded = base64.b64encode(password.encode()).decode()
        password_file.write_text(encoded)
    
    def _get_password(self, email: str) -> str:
        """Retrieve stored password"""
        import base64
        password_file = self.config_dir / f"{email}.pwd"
        if password_file.exists():
            encoded = password_file.read_text()
            return base64.b64decode(encoded.encode()).decode()
        return None
    
    def remove_account(self, email: str):
        """Remove an email account"""
        if email in self.accounts:
            del self.accounts[email]
            self._save_accounts()
            # Remove password file
            password_file = self.config_dir / f"{email}.pwd"
            if password_file.exists():
                password_file.unlink()
            logger.info(f"Removed email account: {email}")
    
    def list_accounts(self) -> List[EmailAccount]:
        """List all configured accounts"""
        return list(self.accounts.values())
    
    # ==================== Connection ====================
    
    def connect(self, email: str = None) -> bool:
        """
        Connect to email account.
        
        Returns True if connected successfully.
        """
        email = email or (self.current_account.email if self.current_account else None)
        if not email or email not in self.accounts:
            raise ValueError(f"No account configured for {email}")
        
        self.current_account = self.accounts[email]
        
        if self.current_account.use_oauth:
            return self._connect_oauth()
        else:
            return self._connect_imap()
    
    def _connect_oauth(self) -> bool:
        """Connect using OAuth (Gmail, Outlook)"""
        if not HAS_GOOGLE_API:
            raise ImportError("google-api-python-client required for OAuth. Install: pip install google-api-python-client google-auth-google-auth-oauthlib")
        
        account = self.current_account
        
        if account.provider == EmailProvider.GMAIL:
            return self._connect_gmail()
        elif account.provider == EmailProvider.OUTLOOK:
            # Outlook also uses Gmail API with different scopes
            return self._connect_gmail()
        else:
            raise ValueError(f"OAuth not supported for {account.provider.value}")
    
    def _connect_gmail(self) -> bool:
        """Connect to Gmail using OAuth"""
        account = self.current_account
        
        # Load or obtain token
        creds = None
        if account.token_path and Path(account.token_path).exists():
            import json
            with open(account.token_path) as f:
                token_data = json.load(f)
                from google.oauth2.credentials import Credentials
                creds = Credentials(**token_data)
        
        if not creds or not creds.valid:
            if not account.credentials_path:
                raise ValueError("credentials_path required for first-time OAuth")
            
            flow = InstalledAppFlow.from_client_secrets_file(
                account.credentials_path,
                ['https://www.googleapis.com/auth/gmail.readonly']
            )
            creds = flow.run_local_server(port=0)
            
            # Save token
            if account.token_path:
                import json
                with open(account.token_path, 'w') as f:
                    json.dump({
                        'token': creds.token,
                        'refresh_token': creds.refresh_token,
                        'token_uri': creds.token_uri,
                        'client_id': creds.client_id,
                        'client_secret': creds.client_secret,
                        'scopes': creds.scopes,
                    }, f)
        
        self._gmail_service = build('gmail', 'v1', credentials=creds)
        logger.info(f"Connected to Gmail: {account.email}")
        return True
    
    def _connect_imap(self) -> bool:
        """Connect using IMAP with username/password"""
        if not HAS_IMAPLIB:
            raise ImportError("imaplib required for IMAP. Use standard Python library.")
        
        account = self.current_account
        config = PROVIDER_CONFIGS.get(account.provider, {})
        
        # Get password
        password = self._get_password(account.email)
        if not password:
            raise ValueError(f"No password stored for {account.email}")
        
        # Connect
        self._imap_connection = imaplib.IMAP4_SSL(
            config.get("imap_host", "imap.gmail.com"),
            config.get("imap_port", 993)
        )
        
        self._imap_connection.login(account.username, password)
        logger.info(f"Connected to IMAP: {account.email}")
        return True
    
    def disconnect(self):
        """Disconnect from email server"""
        if self._imap_connection:
            try:
                self._imap_connection.logout()
            except:
                pass
            self._imap_connection = None
        
        self._gmail_service = None
        self.current_account = None
    
    # ==================== Fetch Emails ====================
    
    def fetch_all_emails(
        self,
        max_results: int = None,
        since: datetime = None,
        before: datetime = None,
        label_ids: List[str] = None,
    ) -> List[EmailMessage]:
        """
        Fetch all emails from the account.
        
        Args:
            max_results: Maximum number of emails to fetch (None = all)
            since: Fetch emails after this date
            before: Fetch emails before this date
            label_ids: Gmail labels to filter (e.g., ['INBOX', 'SENT'])
        
        Returns:
            List of EmailMessage objects
        """
        if self._gmail_service:
            return self._fetch_gmail_emails(max_results, since, before, label_ids)
        elif self._imap_connection:
            return self._fetch_imap_emails(max_results, since, before)
        else:
            raise ConnectionError("Not connected. Call connect() first.")
    
    def _fetch_gmail_emails(
        self,
        max_results: int = None,
        since: datetime = None,
        before: datetime = None,
        label_ids: List[str] = None,
    ) -> List[EmailMessage]:
        """Fetch emails from Gmail API"""
        messages = []
        
        query = ""
        if since:
            query += f"after:{since.strftime('%Y/%m/%d')}"
        if before:
            query += f" before:{before.strftime('%Y/%m/%d')}"
        
        # Get message IDs
        results = self._gmail_service.users().messages().list(
            userId='me',
            q=query,
            maxResults=max_results or 500,
            labelIds=label_ids,
        ).execute()
        
        message_ids = results.get('messages', [])
        
        # Fetch each message
        for msg_info in message_ids:
            msg = self._gmail_service.users().messages().get(
                userId='me',
                id=msg_info['id'],
                format='full'
            ).execute()
            
            email_msg = self._parse_gmail_message(msg)
            if email_msg:
                messages.append(email_msg)
        
        logger.info(f"Fetched {len(messages)} emails from Gmail")
        return messages
    
    def _parse_gmail_message(self, msg: dict) -> Optional[EmailMessage]:
        """Parse Gmail API message to EmailMessage"""
        try:
            headers = msg.get('payload', {}).get('headers', {})
            header_dict = {h['name'].lower(): h['value'] for h in headers}
            
            # Extract body
            body_text = None
            body_html = None
            
            parts = msg.get('payload', {}).get('parts', [])
            if not parts:
                # No parts, try body
                body = msg.get('payload', {}).get('body', {})
                if body.get('data'):
                    import base64
                    data = base64.urlsafe_b64decode(body['data'].encode())
                    content_type = header_dict.get('content-type', 'text/plain')
                    if 'text/html' in content_type:
                        body_html = data.decode('utf-8', errors='ignore')
                    else:
                        body_text = data.decode('utf-8', errors='ignore')
            else:
                for part in parts:
                    if part.get('mimeType') == 'text/plain':
                        if part.get('body', {}).get('data'):
                            import base64
                            data = base64.urlsafe_b64decode(part['body']['data'].encode())
                            body_text = data.decode('utf-8', errors='ignore')
                    elif part.get('mimeType') == 'text/html':
                        if part.get('body', {}).get('data'):
                            import base64
                            data = base64.urlsafe_b64decode(part['body']['data'].encode())
                            body_html = data.decode('utf-8', errors='ignore')
            
            # Parse date
            date_str = header_dict.get('date')
            date = None
            if date_str:
                try:
                    from email.utils import parsedate_to_datetime
                    date = parsedate_to_datetime(date_str)
                except:
                    pass
            
            return EmailMessage(
                message_id=msg['id'],
                subject=header_dict.get('subject', '(No Subject)'),
                from_=header_dict.get('from', ''),
                to=header_dict.get('to', ''),
                cc=header_dict.get('cc'),
                bcc=header_dict.get('bcc'),
                date=date,
                body_text=body_text,
                body_html=body_html,
                labels=msg.get('labelIds', []),
                thread_id=msg.get('threadId'),
            )
        except Exception as e:
            logger.warning(f"Failed to parse Gmail message: {e}")
            return None
    
    def _fetch_imap_emails(
        self,
        max_results: int = None,
        since: datetime = None,
        before: datetime = None,
    ) -> List[EmailMessage]:
        """Fetch emails from IMAP"""
        messages = []
        
        # Select inbox
        status, _ = self._imap_connection.select('INBOX')
        if status != 'OK':
            raise Exception("Failed to select INBOX")
        
        # Build search query
        search_criteria = []
        if since:
            search_criteria.append(f'SINCE {since.strftime("%d-%b-%Y")}')
        if before:
            search_criteria.append(f'BEFORE {before.strftime("%d-%b-%Y")}')
        
        if not search_criteria:
            search_criteria = ['ALL']
        
        # Search
        if isinstance(search_criteria[0], str):
            status, message_ids = self._imap_connection.search(None, *search_criteria)
        else:
            status, message_ids = self._imap_connection.search(None, b'ALL')
        
        if status != 'OK':
            return []
        
        ids = message_ids[0].split()
        if max_results:
            ids = ids[-max_results:]
        
        # Fetch each message
        for msg_id in ids:
            status, msg_data = self._imap_connection.fetch(msg_id, '(RFC822)')
            if status != 'OK' or not msg_data:
                continue
            
            raw_email = msg_data[0][1]
            email_msg = self._parse_imap_message(raw_email)
            if email_msg:
                messages.append(email_msg)
        
        logger.info(f"Fetched {len(messages)} emails from IMAP")
        return messages
    
    def _parse_imap_message(self, raw_email: bytes) -> Optional[EmailMessage]:
        """Parse raw IMAP email to EmailMessage"""
        try:
            msg = email.message_from_bytes(raw_email, policy=default)
            
            # Get headers
            def get_header(msg, name):
                return msg.get(name, '')
            
            # Extract body
            body_text = None
            body_html = None
            
            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    if content_type == 'text/plain' and not body_text:
                        try:
                            body_text = part.get_content()
                        except:
                            pass
                    elif content_type == 'text/html' and not body_html:
                        try:
                            body_html = part.get_content()
                        except:
                            pass
            else:
                content_type = msg.get_content_type()
                try:
                    content = msg.get_content()
                    if content_type == 'text/html':
                        body_html = content
                    else:
                        body_text = content
                except:
                    pass
            
            # Parse date
            date_str = get_header(msg, 'Date')
            date = None
            if date_str:
                try:
                    from email.utils import parsedate_to_datetime
                    date = parsedate_to_datetime(date_str)
                except:
                    pass
            
            return EmailMessage(
                message_id=get_header(msg, 'Message-ID'),
                subject=get_header(msg, 'Subject'),
                from_=get_header(msg, 'From'),
                to=get_header(msg, 'To'),
                cc=get_header(msg, 'Cc'),
                bcc=get_header(msg, 'Bcc'),
                date=date,
                body_text=body_text,
                body_html=body_html,
            )
        except Exception as e:
            logger.warning(f"Failed to parse IMAP message: {e}")
            return None
    
    # ==================== Sync Status ====================
    
    def needs_full_sync(self, email: str = None) -> bool:
        """
        Check if a full sync is needed.
        
        Returns True if no previous sync has been done.
        """
        email = email or (self.current_account.email if self.current_account else None)
        if not email:
            return False
        
        sync_file = self.config_dir / f"{email}_sync_status.json"
        return not sync_file.exists()
    
    def get_last_sync(self, email: str = None) -> Optional[datetime]:
        """Get last sync timestamp"""
        email = email or (self.current_account.email if self.current_account else None)
        if not email:
            return None
        
        sync_file = self.config_dir / f"{email}_sync_status.json"
        if sync_file.exists():
            data = json.loads(sync_file.read_text())
            return datetime.fromisoformat(data['last_sync'])
        return None
    
    def update_sync_status(self, email: str = None, full_sync: bool = True):
        """Update sync status after successful sync"""
        email = email or (self.current_account.email if self.current_account else None)
        if not email:
            return
        
        sync_file = self.config_dir / f"{email}_sync_status.json"
        data = {
            'last_sync': datetime.now().isoformat(),
            'full_sync': full_sync,
            'email': email,
        }
        sync_file.write_text(json.dumps(data, indent=2))
    
    # ==================== Integration with Brain ====================
    
    def sync_to_brain(self, brain, email: str = None, full_sync: bool = True) -> int:
        """
        Sync emails directly to brain.
        
        Args:
            brain: Brain instance
            email: Email account (uses current if None)
            full_sync: If True, fetch all emails. If False, only new ones.
        
        Returns:
            Number of emails synced
        """
        email = email or (self.current_account.email if self.current_account else None)
        
        if not self.current_account or self.current_account.email != email:
            self.connect(email)
        
        # Determine date range
        since = None
        if not full_sync:
            since = self.get_last_sync(email)
        
        # Fetch emails
        messages = self.fetch_all_emails(since=since)
        
        # Ingest to brain
        count = 0
        for msg in messages:
            brain.ingest_email(msg)
            count += 1
        
        # Update sync status
        self.update_sync_status(email, full_sync)
        
        logger.info(f"Synced {count} emails to brain")
        return count


# ==================== Convenience Functions ====================

def create_email_connector(config_dir: str = None) -> EmailConnector:
    """Create an email connector instance"""
    return EmailConnector(config_dir)


def list_supported_providers() -> List[str]:
    """List all supported email providers"""
    return [p.value for p in EmailProvider]
