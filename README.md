# Pluggable Brain - Personal External Brain

> Your second brain that remembers everything.

## Vision

A personal knowledge management system that:
- **Ingests** data from emails, chats, documents, images via OpenClaw
- **Processes** and converts to lightweight standardized format (pickle)
- **Stores** incrementally in knowledge_base
- **Retrieves** answers instantly when you ask questions

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     OpenClaw                                │
│  (Email, Feishu, Discord, Telegram, etc.)                  │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Source Ingestion                          │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐       │
│  │ Email   │  │  Chat   │  │ Document│  │  Image │       │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘       │
└───────┼────────────┼────────────┼────────────┼──────────────┘
        │            │            │            │
        ▼            ▼            ▼            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Brain Core                               │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  1. Parse & Normalize                              │  │
│  │  2. Extract Metadata (time, source, type)          │  │
│  │  3. Convert to Memory Fragment                     │  │
│  │  4. Update Knowledge Base (incremental)             │  │
│  └─────────────────────────────────────────────────────┘  │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                 Knowledge Base                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ memory.pkl   │  │  index.pkl   │  │ timeline.pkl  │   │
│  │ (记忆碎片)    │  │ (快速索引)    │  │ (时间线)      │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
└─────────────────────────────────────────────────────────────┘
                           ▲
                           │
                    ┌──────┴──────┐
                    │  Retrieval  │
                    │  (检索查询)  │
                    └─────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    User Query                              │
│  "What was I doing on March 5, 2025?"                     │
└─────────────────────────────────────────────────────────────┘
```

## Data Structure

### Memory Fragment (记忆碎片)
```python
@dataclass
class MemoryFragment:
    id: str                    # 唯一ID
    content: str               # 核心内容
    source: str               # 来源 (email/chat/doc)
    source_id: str            # 来源原始ID
    timestamp: datetime        # 时间戳
    metadata: dict            # 额外元数据
    tags: List[str]          # 标签
    embedding: np.ndarray     # 向量嵌入 (可选)
```

### Timeline Entry (时间线)
```python
@dataclass
class TimelineEntry:
    date: str                 # YYYY-MM-DD
    fragments: List[str]      # 当日记忆碎片ID
    summary: str             # 当日摘要
```

## Usage

```python
from pluggable_brain import Brain

# 1. Ingest new data (from OpenClaw)
brain = Brain()
brain.ingest_email(email_content)
brain.ingest_chat(chat_messages)
brain.ingest_document(file_path)

# 2. Query your brain
answer = brain.recall("What did I do last week?")
answer = brain.recall("What did we discuss about project X?")
answer = brain.recall_on_date("2025-03-05")

# 3. Manage knowledge base
brain.update_knowledge_base()  # 增量更新
brain.delete_fragment(fragment_id)
brain.search_by_tag("work")
```

## Source Types

| Source | Format | Metadata |
|--------|--------|----------|
| Email | subject, body, sender, recipients, date | thread_id, labels |
| Chat | messages, participants, platform | channel_id, group_name |
| Document | title, content, author | file_type, path |
| Image | description, ocr_text | exif, location |

## Storage

- **Pickle** for speed and lightweight (primary)
- **JSON** for metadata and export
- **SQLite** optional for complex queries

## Features

- [x] Incremental updates (only new data)
- [x] Fast retrieval (<100ms)
- [x] Time-based search
- [x] Tag-based filtering
- [x] Source filtering
- [x] Delete/Update support
- [ ] Semantic search (with embeddings)
- [ ] Auto-tagging with AI
- [ ] Summarization

## Installation

```bash
pip install -r requirements.txt
```

## License

MIT
