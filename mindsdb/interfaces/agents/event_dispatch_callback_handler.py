import queue
from typing import Any, Dict, List, Optional, Sequence
from uuid import UUID

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.documents import Document


class EventDispatchCallbackHandler(BaseCallbackHandler):
    '''Puts dispatched events onto an event queue to be processed as a streaming chunk'''
    def __init__(self, queue: queue.Queue):
        self.queue = queue

    def on_custom_event(
        self,
        name: str,
        data: Any,
        *,
        run_id: UUID,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        self.queue.put({
            'type': 'event',
            'name': name,
            'data': data
        })

    def on_retriever_end(
        self,
        documents: Sequence[Document],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> Any:
        document_objects = []
        for d in documents:
            document_objects.append({
                'content': d.page_content,
                'metadata': d.metadata
            })
        self.queue.put({
            'type': 'event',
            'name': 'retriever_end',
            'data': {
                'documents': document_objects
            }
        })
