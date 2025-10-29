import React, { useState, useEffect, useRef } from 'react';
import { ChatDocument } from '../types';
import { SendIcon } from './icons/SendIcon';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface ChatMessage {
  sender: 'user' | 'ai';
  text: string;
}

interface ChatWindowProps {
  documents: ChatDocument[];
  aiAgentId: string;
  onSendMessage: (message: string, agentId: string) => Promise<string>;
}

const MarkdownRenderer: React.FC<{ content: string }> = ({ content }) => {
  return (
    <div className="text-sm">
      <ReactMarkdown
        children={content}
        remarkPlugins={[remarkGfm]}
        components={{
          p: ({node, ...props}) => <p className="mb-2 last:mb-0" {...props} />,
          strong: ({node, ...props}) => <strong className="font-semibold" {...props} />,
          em: ({node, ...props}) => <em className="italic" {...props} />,
          ul: ({node, ...props}) => <ul className="list-disc pl-5 space-y-1" {...props} />,
          code: ({node, inline, className, children, ...props}) => {
            return <code className="bg-gray-200 text-gray-800 px-1 py-0.5 rounded text-xs" {...props}>{children}</code>
          }
        }}
      />
    </div>
  );
};


const ChatWindow: React.FC<ChatWindowProps> = ({ documents, aiAgentId, onSendMessage }) => {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState('');
  const [isAiTyping, setIsAiTyping] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setMessages([
      {
        sender: 'ai',
        text: `Hello! I'm ready to discuss the ${documents.length} documents you've selected. What would you like to know?`,
      },
    ]);
  }, [documents]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, isAiTyping]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim()) return;

    const userMessage: ChatMessage = { sender: 'user', text: input };
    setMessages(prev => [...prev, userMessage]);
    const currentInput = input;
    setInput('');
    setIsAiTyping(true);

    try {
      const aiResponseText = await onSendMessage(currentInput, aiAgentId);
      const aiMessage: ChatMessage = { sender: 'ai', text: aiResponseText };
      setMessages(prev => [...prev, aiMessage]);
    } catch (error) {
      console.error("Failed to send message:", error);
      const errorMessage: ChatMessage = { sender: 'ai', text: "Sorry, I encountered an error. Please try again." };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsAiTyping(false);
    }
  };

  return (
    <div className="flex flex-col h-full">
      <div className="p-4 border-b border-gray-200 text-center flex-shrink-0">
        <h2 className="text-lg font-semibold text-gray-800">Chat with AI Assistant</h2>
      </div>
      <div className="flex-1 overflow-y-auto p-4 min-h-0">
        <div className="space-y-4">
          {messages.map((msg, index) => (
            <div key={index} className={`flex items-start gap-3 ${msg.sender === 'user' ? 'justify-end' : ''}`}>
              {msg.sender === 'ai' && (
                <div className="w-8 h-8 rounded-full bg-white border border-gray-900 flex items-center justify-center text-gray-900 font-bold text-sm flex-shrink-0">
                  AI
                </div>
              )}
              <div
                className={`max-w-md p-3 rounded-lg ${
                  msg.sender === 'user'
                    ? 'bg-black text-white'
                    : 'bg-gray-100 text-gray-800'
                }`}
              >
                {msg.sender === 'ai' ? <MarkdownRenderer content={msg.text} /> : <p className="text-sm">{msg.text}</p>}
              </div>
            </div>
          ))}
          {isAiTyping && (
            <div className="flex items-start gap-3">
              <div className="w-8 h-8 rounded-full bg-white border border-gray-900 flex items-center justify-center text-gray-900 font-bold text-sm flex-shrink-0">
                AI
              </div>
              <div className="max-w-md p-3 rounded-lg bg-gray-100 text-gray-800">
                <div className="flex items-center space-x-1">
                  <span className="h-2 w-2 bg-gray-400 rounded-full animate-bounce [animation-delay:-0.3s]"></span>
                  <span className="h-2 w-2 bg-gray-400 rounded-full animate-bounce [animation-delay:-0.15s]"></span>
                  <span className="h-2 w-2 bg-gray-400 rounded-full animate-bounce"></span>
                </div>
              </div>
            </div>
          )}
          <div ref={messagesEndRef} />
        </div>
      </div>
      <div className="p-4 border-t border-gray-200 bg-white flex-shrink-0">
        <form onSubmit={handleSubmit} className="flex items-center gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about the selected documents..."
            className="w-full px-4 py-2 text-sm bg-gray-100 border border-gray-200 rounded-full focus:outline-none text-gray-900"
          />
          <button
            type="submit"
            className="p-3 bg-black text-white rounded-full hover:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-black disabled:opacity-50"
            disabled={!input.trim() || isAiTyping}
          >
            <SendIcon className="h-5 w-5" />
          </button>
        </form>
      </div>
    </div>
  );
};

export default ChatWindow;