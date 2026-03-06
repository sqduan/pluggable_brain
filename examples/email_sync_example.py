"""
Email Integration Example - How to use email sync with Pluggable Brain

Supported Providers:
- Gmail (OAuth or App Password)
- Outlook (OAuth)
- QQ邮箱 (IMAP)
- 163邮箱 (IMAP)
- 126邮箱 (IMAP)
- 阿里云邮箱 (IMAP)
- Yahoo (IMAP)
- iCloud (IMAP)
- Generic IMAP
"""
from pluggable_brain import (
    Brain, 
    EmailConnector, 
    EmailProvider,
    create_email_connector,
    list_supported_providers,
)


def example_gmail_oauth():
    """Example: Gmail with OAuth (recommended)"""
    
    # 1. Create email connector
    connector = create_email_connector()
    
    # 2. Configure account (first time)
    # You need to download OAuth credentials from Google Cloud Console
    connector.configure_account(
        email="your@gmail.com",
        provider=EmailProvider.GMAIL,
        credentials_path="path/to/credentials.json",
        # Token will be saved automatically after first OAuth flow
    )
    
    # 3. Connect (opens browser for OAuth on first time)
    connector.connect()
    
    # 4. Check if full sync needed
    if connector.needs_full_sync():
        print("首次配置完成！是否全量检索邮箱内容? (y/n)")
        # User input here
        response = input()
        if response.lower() == 'y':
            # Full sync - fetch all emails
            messages = connector.fetch_all_emails()
            print(f"将同步 {len(messages)} 封邮件...")
        else:
            # Only sync new emails
            messages = connector.fetch_all_emails(max_results=100)
    else:
        # Incremental sync - only new emails since last sync
        messages = connector.fetch_all_emails()
    
    # 5. Create brain and ingest emails
    brain = Brain()
    
    for msg in messages:
        brain.ingest_email(email_message=msg)
    
    print(f"已同步 {len(messages)} 封邮件到知识库")


def example_imap_password():
    """Example: IMAP with password (for Chinese email providers)"""
    
    connector = create_email_connector()
    
    # QQ邮箱/163邮箱/126邮箱/阿里云需要使用授权码而非登录密码
    connector.configure_account(
        email="your@163.com",
        provider=EmailProvider.NETEASE_163,
        username="your@163.com",
        password="your_auth_code",  # 邮箱授权码
    )
    
    connector.connect()
    
    # Fetch emails
    messages = connector.fetch_all_emails(max_results=500)
    
    # Ingest to brain
    brain = Brain()
    for msg in messages:
        brain.ingest_email(email_message=msg)


def example_sync_to_brain():
    """Simplified: sync emails directly to brain"""
    
    connector = create_email_connector()
    connector.configure_account(
        email="your@gmail.com",
        provider=EmailProvider.GMAIL,
        credentials_path="path/to/credentials.json",
    )
    
    brain = Brain()
    
    # This handles full/incremental sync automatically
    count = connector.sync_to_brain(
        brain=brain,
        full_sync=True,  # Or ask user
    )
    
    print(f"同步完成: {count} 封邮件")


# ==================== User Interaction ====================

def ask_full_sync() -> bool:
    """Ask user whether to do full sync"""
    print("""
╔════════════════════════════════════════════════════════════╗
║           首次配置完成！                                ║
╠════════════════════════════════════════════════════════════╣
║  请问是否需要全量检索邮箱内容？                        ║
║                                                            ║
║  [Y] 是 - 遍历所有历史邮件，制作记忆碎片              ║
║  [N] 否 - 仅同步新邮件                                  ║
║                                                            ║
║  ⚠️  首次全量同步可能需要较长时间                       ║
╚════════════════════════════════════════════════════════════╝
    """)
    
    response = input("请选择 (Y/N): ").strip().lower()
    return response == 'y'


def main():
    """Interactive email setup"""
    print("=== Pluggable Brain 邮箱同步 ===\n")
    print(f"支持的邮箱: {', '.join(list_supported_providers())}\n")
    
    email = input("请输入邮箱地址: ").strip()
    
    # Auto-detect provider
    connector = create_email_connector()
    provider = connector._detect_provider(email)
    print(f"检测到邮箱类型: {provider.value}")
    
    # Configure
    if provider in [EmailProvider.GMAIL, EmailProvider.OUTLOOK]:
        creds_path = input("请输入 OAuth credentials.json 路径: ").strip()
        connector.configure_account(
            email=email,
            provider=provider,
            credentials_path=creds_path,
        )
    else:
        # IMAP - need password/authorization code
        password = input("请输入邮箱授权码: ").strip()
        connector.configure_account(
            email=email,
            provider=provider,
            username=email,
            password=password,
        )
    
    # Connect
    print("\n连接中...")
    connector.connect()
    print("连接成功！\n")
    
    # Ask about full sync
    if connector.needs_full_sync():
        full_sync = ask_full_sync()
    else:
        full_sync = False
    
    # Sync
    print("\n开始同步邮件...")
    brain = Brain()
    count = connector.sync_to_brain(brain, full_sync=full_sync)
    
    print(f"\n✅ 同步完成！共处理 {count} 封邮件")


if __name__ == "__main__":
    # Run interactive setup
    # main()
    
    # Or run examples
    # example_gmail_oauth()
    # example_imap_password()
    pass
