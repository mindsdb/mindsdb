import React, { useState, useRef, useEffect, useCallback } from 'react';
import { ChatDocument } from '../types';
import ChatWindow from '../components/ChatWindow';
import { ArrowLeftIcon } from '../components/icons/ArrowLeftIcon';
import Header from '../components/Header';

interface ChatPageProps {
  documents: ChatDocument[];
  onBack: () => void;
  aiAgentId: string;
  onSendMessage: (message: string, agentId: string) => Promise<string>;
}

const ChatPage: React.FC<ChatPageProps> = ({ documents, onBack, aiAgentId, onSendMessage }) => {
  const [activePdfUrl, setActivePdfUrl] = useState<string>(documents[0]?.paperUrl || '');
  const [leftPanelWidth, setLeftPanelWidth] = useState(60);
  const chatPageRef = useRef<HTMLDivElement>(null);
  const isResizing = useRef(false);

  const handleMouseDown = (e: React.MouseEvent) => {
    e.preventDefault();
    isResizing.current = true;
  };

  const handleMouseUp = useCallback(() => {
    isResizing.current = false;
    document.body.style.cursor = 'default';
  }, []);

  const handleMouseMove = useCallback((e: MouseEvent) => {
    if (!isResizing.current || !chatPageRef.current) return;
    
    document.body.style.cursor = 'col-resize';
    const containerRect = chatPageRef.current.getBoundingClientRect();
    const newWidth = ((e.clientX - containerRect.left) / containerRect.width) * 100;

    // Constrain the width between 20% and 80%
    if (newWidth > 20 && newWidth < 80) {
      setLeftPanelWidth(newWidth);
    }
  }, []);

  useEffect(() => {
    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp);
    
    return () => {
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseup', handleMouseUp);
    };
  }, [handleMouseMove, handleMouseUp]);

  const typeStyles = {
    research: 'bg-blue-100 text-blue-800',
    patent: 'bg-green-100 text-green-800'
  };


  return (
    <div className="bg-white h-screen font-sans flex flex-col">
      <Header showTitle={true} />
      <main ref={chatPageRef} className="flex-1 flex overflow-hidden">
        {/* Left Column: PDF Viewer and Document List */}
        <aside 
          className="flex flex-col bg-white"
          style={{ width: `${leftPanelWidth}%` }}
        >
          <div className="p-4 border-b border-gray-200 flex items-center flex-shrink-0">
            <button onClick={onBack} className="p-2 rounded-full hover:bg-gray-100 mr-2">
              <ArrowLeftIcon className="h-5 w-5 text-gray-600" />
            </button>
            <h2 className="text-lg font-semibold text-gray-800">Selected Documents</h2>
          </div>
          
          <div className="flex-1 flex flex-col min-h-0">
            <nav className="p-2 space-y-1 overflow-y-auto flex-shrink-0" style={{ maxHeight: '30%' }}>
              {documents.map((doc) => (
                <button
                  key={doc.paperId}
                  onClick={() => setActivePdfUrl(doc.paperUrl)}
                  className={`w-full text-left p-2 rounded-md text-sm transition-colors ${
                    activePdfUrl === doc.paperUrl
                      ? 'bg-indigo-50'
                      : 'hover:bg-gray-100'
                  }`}
                >
                  <p className={`font-medium ${activePdfUrl === doc.paperUrl ? 'text-indigo-700' : 'text-gray-800'}`}>
                    {doc.title}
                  </p>
                  <span className={`text-xs font-semibold inline-block py-0.5 px-1.5 uppercase rounded-full mt-1 ${typeStyles[doc.source]}`}>
                    {doc.source}
                  </span>
                </button>
              ))}
            </nav>
            
            <div className="flex-1 p-2 border-t border-gray-200 min-h-0">
              {activePdfUrl ? (
                <iframe
                  src={`https://docs.google.com/gview?url=${encodeURIComponent(activePdfUrl)}&embedded=true`}
                  title="PDF Viewer"
                  className="w-full h-full rounded-md"
                  frameBorder="0"
                />
              ) : (
                <div className="w-full h-full flex items-center justify-center text-gray-500">
                  <p>Select a document to view.</p>
                </div>
              )}
            </div>
          </div>
        </aside>

        {/* Divider */}
        <div
          onMouseDown={handleMouseDown}
          className="w-2 cursor-col-resize bg-gray-200 hover:bg-black-500 transition-colors duration-200"
          style={{ flexShrink: 0 }}
        />

        {/* Right Column: Chat Window */}
        <section 
          className="flex flex-col overflow-hidden"
          style={{ width: `${100 - leftPanelWidth}%` }}
        >
          <ChatWindow documents={documents} aiAgentId={aiAgentId} onSendMessage={onSendMessage} />
        </section>
      </main>
      <footer className="text-center py-4 border-t border-gray-200 flex-shrink-0">
        <p className="text-sm text-gray-500">
          Made with <span className="text-red-500">&hearts;</span> for <a href="https://mindsdb.com/" target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">Mindsdb</a> Hacktoberfest
        </p>
      </footer>
    </div>
  );
};

export default ChatPage;