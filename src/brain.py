"""
Personal External Brain - Core v2

With Chunk support and OpenClaw integration.
"""
import os
import pickle
import json
import hashlib
import uuid
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field, asdict
from collections import defaultdict
from enum import Enum
import logging

logger = logging.getLogger(__name__)


# ==================== Data Structures ====================

class ChunkType(Enum):
    """Types of chunks"""
    TEXT = "text"           # Plain text
    EMAIL = "email"         # Email content
    CHAT = "chat"           # Chat message
    IMAGE_NAME = "image_name"  # Image filename
    IMAGE_DESC = "image_desc"  # Image description/OCR
    AUDIO_NAME = "audio_name" # Audio filename
    AUDIO_DESC = "audio_desc" # Audio transcription
    DOCUMENT = "document"   # Document content
    LINK = "link"          # URL/link


@dataclass
class Chunk:
    """
    A chunk of information - the basic unit of memory.
    
    For text: a paragraph or logical unit
    For image: (filename, description) as separate chunks
    For audio: (filename, transcription) as separate chunks
    """
    id: str
    content: str                    # The actual content
    chunk_type: ChunkType           # Type of chunk
    parent_id: str = None           # Parent message/document ID
    parent_type: str = None         # email/chat/doc/image/audio
    timestamp: datetime = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    index: int = 0                  # Position in parent
    
    def to_dict(self) -> dict:
        data = asdict(self)
        data['chunk_type'] = self.chunk_type.value
        data['timestamp'] = self.timestamp.isoformat() if self.timestamp else None
        return data
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Chunk':
        data['chunk_type'] = ChunkType(data['chunk_type'])
        if data.get('timestamp'):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)


@dataclass
class MemoryFragment:
    """A collection of chunks from one source"""
    id: str
    chunks: List[Chunk]
    source: str           # email/chat/doc/image/audio
    source_id: str        # original ID from source
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def content(self) -> str:
        """Get combined content of all chunks"""
        return "\n".join(c.content for c in self.chunks)


# ==================== Chunker ====================

class Chunker:
    """
    Splits content into appropriate chunks.
    
    - Text: by paragraphs, sentences, or max length
    - Image: filename + description separately
    - Audio: filename + transcription separately
    """
    
    def __init__(self, 
                 max_chunk_size: int = 500,
                 min_chunk_size: int = 50):
        self.max_chunk_size = max_chunk_size
        self.min_chunk_size = min_chunk_size
    
    def chunk_text(self, text: str, parent_id: str, 
                   chunk_type: ChunkType = ChunkType.TEXT) -> List[Chunk]:
        """Split text into chunks"""
        if not text or not text.strip():
            return []
        
        chunks = []
        
        # Split by paragraphs first
        paragraphs = re.split(r'\n\n+', text)
        
        current_chunk = ""
        chunk_index = 0
        
        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            
            # If single paragraph is too long, split by sentences
            if len(para) > self.max_chunk_size:
                # Save current chunk if not empty
                if current_chunk:
                    chunks.append(self._create_chunk(
                        current_chunk, parent_id, chunk_type, chunk_index
                    ))
                    chunk_index += 1
                    current_chunk = ""
                
                # Split long paragraph
                chunks.extend(self._chunk_long_para(para, parent_id, chunk_type, chunk_index))
                chunk_index += len(chunks)
            
            # Add paragraph to current chunk
            elif len(current_chunk) + len(para) + 1 <= self.max_chunk_size:
                if current_chunk:
                    current_chunk += "\n" + para
                else:
                    current_chunk = para
            
            # Current chunk full, create new
            else:
                if current_chunk:
                    chunks.append(self._create_chunk(
                        current_chunk, parent_id, chunk_type, chunk_index
                    ))
                    chunk_index += 1
                current_chunk = para
        
        # Don't forget last chunk
        if current_chunk:
            chunks.append(self._create_chunk(
                current_chunk, parent_id, chunk_type, chunk_index
            ))
        
        return chunks
    
    def _chunk_long_para(self, text: str, parent_id: str,
                        chunk_type: ChunkType, start_index: int) -> List[Chunk]:
        """Split a long paragraph by sentences"""
        # Split by sentence endings
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        chunks = []
        current = ""
        
        for sentence in sentences:
            if len(current) + len(sentence) <= self.max_chunk_size:
                current += " " + sentence if current else sentence
            else:
                if current:
                    chunks.append(self._create_chunk(
                        current, parent_id, chunk_type, start_index + len(chunks)
                    ))
                current = sentence
        
        if current:
            chunks.append(self._create_chunk(
                current, parent_id, chunk_type, start_index + len(chunks)
            ))
        
        return chunks
    
    def _create_chunk(self, content: str, parent_id: str,
                    chunk_type: ChunkType, index: int) -> Chunk:
        """Create a chunk"""
        chunk_id = self._generate_chunk_id(parent_id, chunk_type.value, index)
        
        return Chunk(
            id=chunk_id,
            content=content.strip(),
            chunk_type=chunk_type,
            parent_id=parent_id,
            parent_type=chunk_type.value,
            index=index
        )
    
    def _generate_chunk_id(self, parent_id: str, chunk_type: str, index: int) -> str:
        """Generate unique chunk ID"""
        raw = f"{parent_id}:{chunk_type}:{index}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
    
    def chunk_email(self, subject: str, body: str, 
                   email_id: str, timestamp: datetime) -> List[Chunk]:
        """Chunk an email into parts"""
        parent_id = email_id
        chunks = []
        
        # Subject as first chunk
        subject_chunk = Chunk(
            id=self._generate_chunk_id(parent_id, "email_subject", 0),
            content=f"Subject: {subject}",
            chunk_type=ChunkType.EMAIL,
            parent_id=parent_id,
            parent_type="email",
            timestamp=timestamp,
            index=0
        )
        chunks.append(subject_chunk)
        
        # Body chunks
        body_chunks = self.chunk_text(body, parent_id, ChunkType.EMAIL)
        for i, chunk in enumerate(body_chunks):
            chunk.index = i + 1
            chunk.timestamp = timestamp
        chunks.extend(body_chunks)
        
        return chunks
    
    def chunk_chat(self, messages: List[Dict],
                   chat_id: str, timestamp: datetime) -> List[Chunk]:
        """Chunk chat messages"""
        chunks = []
        
        for i, msg in enumerate(messages):
            sender = msg.get('sender', 'Unknown')
            content = msg.get('content', '')
            time_str = msg.get('time', '')
            
            # Combine sender + content as one chunk
            chunk_content = f"[{time_str}] {sender}: {content}"
            
            chunk = Chunk(
                id=self._generate_chunk_id(chat_id, "chat", i),
                content=chunk_content,
                chunk_type=ChunkType.CHAT,
                parent_id=chat_id,
                parent_type="chat",
                timestamp=timestamp,
                metadata={"sender": sender},
                index=i
            )
            chunks.append(chunk)
        
        return chunks
    
    def chunk_image(self, image_path: str, description: str = None,
                   ocr_text: str = None) -> List[Chunk]:
        """Chunk an image into filename + description"""
        chunks = []
        
        # Extract filename
        filename = os.path.basename(image_path)
        
        # Image name chunk
        name_chunk = Chunk(
            id=self._generate_chunk_id(image_path, "image_name", 0),
            content=f"Image: {filename}",
            chunk_type=ChunkType.IMAGE_NAME,
            parent_id=image_path,
            parent_type="image",
            metadata={"path": image_path},
            index=0
        )
        chunks.append(name_chunk)
        
        # Description chunk (if provided)
        if description:
            desc_chunk = Chunk(
                id=self._generate_chunk_id(image_path, "image_desc", 1),
                content=description,
                chunk_type=ChunkType.IMAGE_DESC,
                parent_id=image_path,
                parent_type="image",
                index=1
            )
            chunks.append(desc_chunk)
        
        # OCR text chunk (if available)
        if ocr_text:
            ocr_chunks = self.chunk_text(ocr_text, image_path, ChunkType.IMAGE_DESC)
            for i, chunk in enumerate(ocr_chunks):
                chunk.index = i + 2
            chunks.extend(ocr_chunks)
        
        return chunks
    
    def chunk_audio(self, audio_path: str, transcription: str = None) -> List[Chunk]:
        """Chunk audio into filename + transcription"""
        chunks = []
        
        # Audio filename
        filename = os.path.basename(audio_path)
        
        name_chunk = Chunk(
            id=self._generate_chunk_id(audio_path, "audio_name", 0),
            content=f"Audio: {filename}",
            chunk_type=ChunkType.AUDIO_NAME,
            parent_id=audio_path,
            parent_type="audio",
            metadata={"path": audio_path},
            index=0
        )
        chunks.append(name_chunk)
        
        # Transcription
        if transcription:
            trans_chunks = self.chunk_text(transcription, audio_path, ChunkType.AUDIO_DESC)
            for i, chunk in enumerate(trans_chunks):
                chunk.index = i + 1
            chunks.extend(trans_chunks)
        
        return chunks
    
    def chunk_document(self, title: str, content: str,
                     doc_id: str, timestamp: datetime) -> List[Chunk]:
        """Chunk a document"""
        chunks = []
        
        # Title as first chunk
        title_chunk = Chunk(
            id=self._generate_chunk_id(doc_id, "doc_title", 0),
            content=f"Title: {title}",
            chunk_type=ChunkType.DOCUMENT,
            parent_id=doc_id,
            parent_type="document",
            timestamp=timestamp,
            index=0
        )
        chunks.append(title_chunk)
        
        # Content chunks
        content_chunks = self.chunk_text(content, doc_id, ChunkType.DOCUMENT)
        for i, chunk in enumerate(content_chunks):
            chunk.index = i + 1
            chunk.timestamp = timestamp
        chunks.extend(content_chunks)
        
        return chunks


# ==================== Brain Core ====================

class Brain:
    """
    Personal External Brain with Chunk Support
    """
    
    def __init__(self, base_path: str = None):
        if base_path is None:
            base_path = Path(__file__).parent.parent
        self.base_path = Path(base_path)
        
        # Directories
        self.source_dir = self.base_path / "source"
        self.knowledge_dir = self.base_path / "knowledge_base"
        
        self.source_dir.mkdir(parents=True, exist_ok=True)
        self.knowledge_dir.mkdir(parents=True, exist_ok=True)
        
        # Files
        self.fragments_file = self.knowledge_dir / "fragments.pkl"
        self.chunks_file = self.knowledge_dir / "chunks.pkl"
        self.timeline_file = self.knowledge_dir / "timeline.pkl"
        
        # In-memory
        self._fragments: Dict[str, MemoryFragment] = {}
        self._chunks: Dict[str, Chunk] = {}
        self._timeline: Dict[str, List[str]] = defaultdict(list)  # date -> chunk_ids
        self._chunk_index: Dict[str, List[str]] = defaultdict(list)  # keyword -> chunk_ids
        
        # Chunker
        self.chunker = Chunker()
        
        # Load
        self._load()
        
        logger.info(f"Brain initialized with {len(self._chunks)} chunks")
    
    def _load(self):
        """Load from disk"""
        # Load fragments
        if self.fragments_file.exists():
            with open(self.fragments_file, 'rb') as f:
                self._fragments = pickle.load(f)
        
        # Load chunks
        if self.chunks_file.exists():
            with open(self.chunks_file, 'rb') as f:
                self._chunks = pickle.load(f)
        
        # Load timeline
        if self.timeline_file.exists():
            with open(self.timeline_file, 'rb') as f:
                self._timeline = pickle.load(f)
        
        # Rebuild chunk index
        self._rebuild_index()
    
    def _save(self):
        """Save to disk"""
        with open(self.fragments_file, 'wb') as f:
            pickle.dump(self._fragments, f)
        
        with open(self.chunks_file, 'wb') as f:
            pickle.dump(self._chunks, f)
        
        with open(self.timeline_file, 'wb') as f:
            pickle.dump(dict(self._timeline), f)
    
    def _rebuild_index(self):
        """Rebuild search index from chunks"""
        self._chunk_index.clear()
        
        for chunk in self._chunks.values():
            # Index by content words
            words = re.findall(r'\w+', chunk.content.lower())
            for word in words:
                if len(word) > 2:  # Skip short words
                    self._chunk_index[word].append(chunk.id)
    
    # ==================== Ingestion ====================
    
    def ingest(self, fragment: MemoryFragment) -> str:
        """Ingest a memory fragment with chunks"""
        # Store fragment
        self._fragments[fragment.id] = fragment
        
        # Store and index chunks
        for chunk in fragment.chunks:
            self._chunks[chunk.id] = chunk
            
            # Index by content words
            words = re.findall(r'\w+', chunk.content.lower())
            for word in words:
                if len(word) > 2:
                    self._chunk_index[word].append(chunk.id)
            
            # Timeline
            if chunk.timestamp:
                date_key = chunk.timestamp.strftime('%Y-%m-%d')
                self._timeline[date_key].append(chunk.id)
        
        self._save()
        logger.info(f"Ingested fragment {fragment.id} with {len(fragment.chunks)} chunks")
        return fragment.id
    
    def ingest_email(self, subject: str = None, body: str = None,
                    email_id: str = None, sender: str = None, recipients: List[str] = None,
                    timestamp: datetime = None, labels: List[str] = None,
                    # New: accept EmailMessage object directly
                    email_message: Any = None) -> str:
        """Ingest an email
        
        Can be called with either:
        - Individual parameters: ingest_email(subject, body, email_id, sender, ...)
        - EmailMessage object: ingest_email(email_message=email_msg)
        """
        # Support EmailMessage object from email_connector
        if email_message is not None:
            subject = email_message.subject
            body = email_message.body_text or email_message.body_html or ""
            email_id = email_message.message_id
            sender = email_message.from_
            recipients = [email_message.to] if email_message.to else []
            timestamp = email_message.date or datetime.now()
            labels = email_message.labels
        
        # Defaults
        subject = subject or "(No Subject)"
        body = body or ""
        email_id = email_id or str(uuid.uuid4())
        timestamp = timestamp or datetime.now()
        # Create chunks
        chunks = self.chunker.chunk_email(subject, body, email_id, timestamp)
        
        # Add metadata
        for chunk in chunks:
            chunk.metadata = {
                "sender": sender,
                "recipients": recipients,
                "labels": labels or []
            }
            chunk.tags = ["email"] + (labels or [])
        
        fragment = MemoryFragment(
            id=email_id,
            chunks=chunks,
            source="email",
            source_id=email_id,
            timestamp=timestamp,
            metadata={"sender": sender, "recipients": recipients}
        )
        
        return self.ingest(fragment)
    
    def ingest_chat(self, messages: List[Dict], chat_id: str,
                   platform: str, participants: List[str],
                   timestamp: datetime) -> str:
        """Ingest chat messages"""
        chunks = self.chunker.chunk_chat(messages, chat_id, timestamp)
        
        for chunk in chunks:
            chunk.tags = ["chat", platform]
            chunk.metadata = {"platform": platform, "participants": participants}
        
        fragment = MemoryFragment(
            id=chat_id,
            chunks=chunks,
            source="chat",
            source_id=chat_id,
            timestamp=timestamp,
            metadata={"platform": platform, "participants": participants}
        )
        
        return self.ingest(fragment)
    
    def ingest_image(self, image_path: str, description: str = None,
                    ocr_text: str = None, timestamp: datetime = None) -> str:
        """Ingest an image"""
        chunks = self.chunker.chunk_image(image_path, description, ocr_text)
        
        for chunk in chunks:
            chunk.timestamp = timestamp or datetime.now()
            chunk.tags = ["image"]
        
        fragment = MemoryFragment(
            id=image_path,
            chunks=chunks,
            source="image",
            source_id=image_path,
            timestamp=timestamp or datetime.now(),
            metadata={"path": image_path}
        )
        
        return self.ingest(fragment)
    
    def ingest_audio(self, audio_path: str, transcription: str = None,
                    timestamp: datetime = None) -> str:
        """Ingest audio"""
        chunks = self.chunker.chunk_audio(audio_path, transcription)
        
        for chunk in chunks:
            chunk.timestamp = timestamp or datetime.now()
            chunk.tags = ["audio"]
        
        fragment = MemoryFragment(
            id=audio_path,
            chunks=chunks,
            source="audio",
            source_id=audio_path,
            timestamp=timestamp or datetime.now(),
            metadata={"path": audio_path}
        )
        
        return self.ingest(fragment)
    
    def ingest_document(self, title: str, content: str, doc_id: str,
                       author: str = None, timestamp: datetime = None) -> str:
        """Ingest a document"""
        chunks = self.chunker.chunk_document(title, content, doc_id, 
                                            timestamp or datetime.now())
        
        for chunk in chunks:
            chunk.metadata = {"author": author}
            chunk.tags = ["document"]
        
        fragment = MemoryFragment(
            id=doc_id,
            chunks=chunks,
            source="document",
            source_id=doc_id,
            timestamp=timestamp or datetime.now(),
            metadata={"title": title, "author": author}
        )
        
        return self.ingest(fragment)
    
    # ==================== Retrieval ====================
    
    def search(self, query: str, limit: int = 20) -> List[Chunk]:
        """Search chunks by keyword"""
        query_words = re.findall(r'\w+', query.lower())
        
        # Score chunks
        scores: Dict[str, int] = defaultdict(int)
        
        for word in query_words:
            if word in self._chunk_index:
                for chunk_id in self._chunk_index[word]:
                    scores[chunk_id] += 1
        
        # Sort by score
        sorted_ids = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        # Get chunks
        results = []
        for chunk_id, score in sorted_ids[:limit]:
            if chunk_id in self._chunks:
                results.append(self._chunks[chunk_id])
        
        return results
    
    def search_by_date(self, date: str) -> List[Chunk]:
        """Get all chunks from a specific date"""
        chunk_ids = self._timeline.get(date, [])
        return [self._chunks[cid] for cid in chunk_ids if cid in self._chunks]
    
    def search_by_date_range(self, start_date: str, end_date: str) -> List[Chunk]:
        """Get chunks in date range"""
        results = []
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        current = start
        while current <= end:
            date_key = current.strftime('%Y-%m-%d')
            results.extend(self.search_by_date(date_key))
            current += timedelta(days=1)
        
        return results
    
    def search_by_parent(self, parent_id: str) -> List[Chunk]:
        """Get all chunks from a specific source"""
        return [c for c in self._chunks.values() if c.parent_id == parent_id]
    
    def search_by_type(self, chunk_type: ChunkType) -> List[Chunk]:
        """Get chunks by type"""
        return [c for c in self._chunks.values() if c.chunk_type == chunk_type]
    
    def get_recent(self, days: int = 7, limit: int = 50) -> List[Chunk]:
        """Get recent chunks"""
        cutoff = datetime.now() - timedelta(days=days)
        
        results = [c for c in self._chunks.values() 
                  if c.timestamp and c.timestamp >= cutoff]
        
        results.sort(key=lambda x: x.timestamp, reverse=True)
        return results[:limit]
    
    # ==================== Management ====================
    
    def delete(self, fragment_id: str) -> bool:
        """Delete a fragment and its chunks"""
        if fragment_id not in self._fragments:
            return False
        
        # Delete chunks
        chunk_ids = [c.id for c in self._fragments[fragment_id].chunks]
        for chunk_id in chunk_ids:
            if chunk_id in self._chunks:
                del self._chunks[chunk_id]
        
        # Delete from timeline
        for chunk in self._fragments[fragment_id].chunks:
            if chunk.timestamp:
                date_key = chunk.timestamp.strftime('%Y-%m-%d')
                if date_key in self._timeline:
                    if chunk.id in self._timeline[date_key]:
                        self._timeline[date_key].remove(chunk.id)
        
        # Delete fragment
        del self._fragments[fragment_id]
        
        self._rebuild_index()
        self._save()
        
        return True
    
    def get_stats(self) -> Dict:
        """Get brain statistics"""
        type_counts = defaultdict(int)
        source_counts = defaultdict(int)
        
        for chunk in self._chunks.values():
            type_counts[chunk.chunk_type.value] += 1
            source_counts[chunk.parent_type] += 1
        
        return {
            "total_fragments": len(self._fragments),
            "total_chunks": len(self._chunks),
            "chunk_types": dict(type_counts),
            "sources": dict(source_counts),
            "dates": len(self._timeline)
        }


def create_brain(base_path: str = None) -> Brain:
    """Create a brain instance"""
    return Brain(base_path)


if __name__ == "__main__":
    brain = Brain()
    
    # Test email
    brain.ingest_email(
        subject="Project Update",
        body="We finished the first version! Great work team!",
        email_id="email_001",
        sender="boss@co.com",
        recipients=["team@co.com"],
        timestamp=datetime.now()
    )
    
    # Test image
    brain.ingest_image(
        image_path="/photos/vacation.jpg",
        description="Beach vacation photo",
        ocr_text="Beach with palm trees and blue ocean"
    )
    
    # Search
    print("=== Search: project ===")
    results = brain.search("project")
    for r in results:
        print(f"- {r.chunk_type.value}: {r.content[:60]}")
    
    print("\n=== Stats ===")
    print(brain.get_stats())
