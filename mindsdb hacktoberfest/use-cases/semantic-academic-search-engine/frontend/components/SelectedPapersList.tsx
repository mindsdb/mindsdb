import React, { useState } from 'react';
import { Paper } from '../types';
import { ChatIcon } from './icons/ChatIcon';
import { XIcon } from './icons/XIcon';
import { ChevronDownIcon } from './icons/ChevronDownIcon';
import { ChevronUpIcon } from './icons/ChevronUpIcon';

interface SelectedPapersListProps {
  selectedPapers: Paper[];
  onRemovePaper: (paperId: string) => void;
  onChat: () => void;
  isInitiatingChat: boolean;
  chatError: string | null;
}

const SelectedPapersList: React.FC<SelectedPapersListProps> = ({ selectedPapers, onRemovePaper, onChat, isInitiatingChat, chatError }) => {
  const [isMinimized, setIsMinimized] = useState(false);

  if (selectedPapers.length === 0) {
    return null;
  }

  return (
    <div className="fixed bottom-4 right-4 w-full max-w-sm bg-white border border-gray-200 rounded-lg shadow-xl z-20">
      <div className="p-4 flex justify-between items-center border-b border-gray-200">
        <h3 className="text-lg font-semibold text-gray-800">Selected Papers ({selectedPapers.length})</h3>
        <button
          onClick={() => setIsMinimized(!isMinimized)}
          className="p-1 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-full"
          aria-label={isMinimized ? 'Expand' : 'Minimize'}
        >
          {isMinimized ? <ChevronUpIcon className="h-5 w-5" /> : <ChevronDownIcon className="h-5 w-5" />}
        </button>
      </div>
      {!isMinimized && (
        <>
          <div className="max-h-60 overflow-y-auto p-4 space-y-3">
            {selectedPapers.map(paper => (
              <div key={paper.id} className="flex items-start justify-between">
                <div className="pr-2">
                  <p className="text-sm font-medium text-gray-700">{paper.title}</p>
                  <p className="text-xs text-gray-500">{paper.authors[0]}</p>
                </div>
                <button
                  onClick={() => onRemovePaper(paper.id)}
                  className="p-1 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-full"
                  aria-label="Remove paper"
                >
                  <XIcon className="h-4 w-4" />
                </button>
              </div>
            ))}
          </div>
          <div className="p-4 bg-gray-50 rounded-b-lg">
            {chatError && (
                <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-md">
                    <p className="text-sm text-red-700">{chatError}</p>
                </div>
            )}
            <div className="flex justify-center">
              <button
                onClick={onChat}
                disabled={isInitiatingChat}
                className="flex items-center justify-center gap-2 px-4 py-2 bg-white text-gray-900 border border-gray-900 text-sm font-medium rounded-md hover:bg-gray-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-900 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                <ChatIcon className="h-5 w-5" />
                <span>{isInitiatingChat ? 'Starting Chat...' : 'Chat with AI'}</span>
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
};

export default SelectedPapersList;
