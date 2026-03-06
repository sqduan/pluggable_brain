"""
Feishu Bot Example - Receive messages from Feishu and store in Brain

Setup Instructions:
==================

1. Create a Feishu app:
   - Go to https://open.feishu.cn/
   - Create a new application
   - Get App ID and App Secret

2. Enable permissions:
   - im:message
   - im:message:send_as_bot
   - contact:user.base:readonly

3. Create webhook:
   - Add event subscription: im.message
   - Add request URL: your-server/webhook/feishu

4. Deploy the bot to a public server

5. Run this script
"""
from pluggable_brain import Brain, FeishuBot, create_feishu_bot, create_config


def example_basic():
    """Basic usage"""
    
    # Create brain
    brain = Brain()
    
    # Create bot
    bot = create_feishu_bot(
        brain=brain,
        app_id="your_app_id",
        app_secret="your_app_secret",
        verification_token="your_verification_token",
    )
    
    # Run server
    bot.run(host="0.0.0.0", port=5000)


def example_with_config():
    """Load config from file"""
    
    brain = Brain()
    
    # Config file path
    bot = create_feishu_bot(
        brain=brain,
        config_path="~/.config/pluggable_brain/feishu.json"
    )
    
    bot.run()


def example_background():
    """Run in background thread"""
    
    brain = Brain()
    
    bot = create_feishu_bot(
        brain=brain,
        config_path="~/.config/pluggable_brain/feishu.json"
    )
    
    # Start in background
    bot.start_webhook(background=True)
    
    # Continue with other tasks
    print("Bot running in background...")
    
    # Query brain anytime
    answer = brain.recall("昨天讨论了什么?")
    print(answer)


def example_create_config():
    """Create config file"""
    
    # Create config file
    config_path = create_config(
        app_id="your_app_id",
        app_secret="your_app_secret", 
        verification_token="your_verification_token",
    )
    
    print(f"Config saved to: {config_path}")


def example_full():
    """Full example with all features"""
    
    # 1. Load brain (with existing memory)
    brain = Brain(knowledge_base_dir="knowledge_base")
    
    # 2. Create bot
    bot = FeishuBot(
        brain=brain,
        config_path="~/.config/pluggable_brain/feishu.json",
    )
    
    # 3. Start webhook
    print("=" * 50)
    print("飞书机器人已启动!")
    print("=" * 50)
    print("发送消息到飞书机器人，消息会自动保存到知识库")
    print("使用 brain.recall() 查询历史消息")
    print("=" * 50)
    
    bot.run(host="0.0.0.0", port=5000)


# ==================== User Interaction ====================

def interactive_setup():
    """Interactive setup guide"""
    print("""
╔════════════════════════════════════════════════════════════╗
║           飞书机器人配置指南                              ║
╠════════════════════════════════════════════════════════════╣
║                                                            ║
║  1. 创建飞书应用:                                          ║
║     - 访问 https://open.feishu.cn/                        ║
║     - 创建新应用，获得 App ID 和 App Secret               ║
║                                                            ║
║  2. 添加权限:                                              ║
║     - im:message (接收消息)                               ║
║     - im:message:send_as_bot (发送消息)                   ║
║     - contact:user.base:readonly (读取用户信息)            ║
║                                                            ║
║  3. 创建事件订阅:                                          ║
║     - 添加事件: im.message                                 ║
║     - 请求URL: 你的服务器/webhook/feishu                  ║
║                                                            ║
║  4. 发布应用                                               ║
║                                                            ║
╚════════════════════════════════════════════════════════════╝
    """)
    
    app_id = input("请输入 App ID: ").strip()
    app_secret = input("请输入 App Secret: ").strip()
    verification_token = input("请输入 Verification Token: ").strip()
    
    # Create config
    config_path = create_config(
        app_id=app_id,
        app_secret=app_secret,
        verification_token=verification_token,
    )
    
    print(f"\n✅ 配置已保存到: {config_path}")
    print("\n运行以下命令启动机器人:")
    print("  python examples/feishu_bot_example.py")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "setup":
            interactive_setup()
        elif sys.argv[1] == "config":
            example_create_config()
    else:
        # Run full example
        # example_full()
        print("Usage:")
        print("  python feishu_bot_example.py        # Run bot")
        print("  python feishu_bot_example.py setup  # Interactive setup")
        print("  python feishu_bot_example.py config # Create config file")
