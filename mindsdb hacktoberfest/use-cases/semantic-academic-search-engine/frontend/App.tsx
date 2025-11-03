import React, { useState } from 'react';
import { Paper, FilterState, ChatDocument } from './types';
import SearchPage from './pages/SearchPage';
import axios, { AxiosError, AxiosInstance } from 'axios';
import ChatPage from './pages/ChatPage';
// import { MOCK_PAPERS } from './constants';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;

const apiClient: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

apiClient.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    return Promise.reject(error);
  }
);

const App: React.FC = () => {
  const [view, setView] = useState<'search' | 'chat'>('search');
  
  // Search state
  const [papers, setPapers] = useState<Paper[]>([]);
  const [hasSearched, setHasSearched] = useState<boolean>(false);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedPaperIds, setSelectedPaperIds] = useState<string[]>([]);
  const [filters, setFilters] = useState<FilterState>({
    isHybridSearch: false,
    alpha: 0.5,
    corpus: {
      arxiv: true,
      patent: true,
      biorxiv: true,
      medrxiv: true,
      chemrxiv: true
    },
    publishedYear: '',
    category: '',
  });

  // Chat state
  const [chatDocuments, setChatDocuments] = useState<ChatDocument[]>([]);
  const [aiAgentId, setAiAgentId] = useState<string | null>(null);
  const [isInitiatingChat, setIsInitiatingChat] = useState<boolean>(false);
  const [chatError, setChatError] = useState<string | null>(null);

  const handleSearch = async (query: string, searchFilters: FilterState) => {
    setHasSearched(true);
    setIsLoading(true);
    setError(null);
    setPapers([]);
    
    console.log("Making API call with the following data:");
    console.log("Query:", query);
    console.log("Filters:", searchFilters);

    try {
    const response = await apiClient.post('/search', {
      query,
      filters: searchFilters,
    });
    
    setPapers(response.data);
    
  } catch (e: any) {
    console.error(e);
    setError(`Failed to search papers. ${e.message}`);
  } finally {
    setIsLoading(false);
  }
  };
  
  const handleInitiateChat = async () => {
      if (selectedPaperIds.length === 0) return;
      setIsInitiatingChat(true);
      setChatError(null);
      
      const papersForChat = papers.filter(p => selectedPaperIds.includes(p.id));
      const payload = papersForChat.map(p => ({ id: p.id, source: p.source }));
      
      console.log("Making API call to get PDF URLs with payload:", payload);
      
      try {
    const response = await apiClient.post('/chat/initiate', {
      papers: payload
    });
    
    setAiAgentId(response.data.aiAgentId);
    setChatDocuments(response.data.documents);
    setView('chat');
    
  } catch (e: any) {
    console.error(e);
    setChatError(`Failed to initiate chat session. ${e.message}`);
  } finally {
    setIsInitiatingChat(false);
  }
  };

  const handleSendMessage = async (message: string, agentId: string): Promise<string> => {
    console.log(`Sending message to agent ${agentId}: "${message}"`);
    try {
    const response = await apiClient.post(
      '/chat/completion', 
      {
        query: message,
        agentId: agentId,
      }
    );

    return response.data.answer || 'No response from server.';
  } catch (error: any) {
    console.error('Error sending message:', error);
    return `Error: ${error.response?.data?.message || error.message}`;
  }
  };
  
  const handleFilterChange = (newFilterValue: Partial<FilterState>) => {
    setFilters(prevFilters => ({
      ...prevFilters,
      ...newFilterValue,
    }));
  };

  const handleToggleSelectPaper = (paperId: string) => {
    setSelectedPaperIds(prev => {
      if (prev.includes(paperId)) {
        return prev.filter(id => id !== paperId);
      }
      if (prev.length < 4) {
        return [...prev, paperId];
      }
      return prev;
    });
  };
  
  const handleRemovePaper = (paperId: string) => {
      setSelectedPaperIds(prev => prev.filter(id => id !== paperId));
  };
  
  const handleBackToSearch = () => {
    setView('search');
    setChatDocuments([]);
    setAiAgentId(null);
  };

  if (view === 'chat' && aiAgentId) {
    return <ChatPage documents={chatDocuments} onBack={handleBackToSearch} aiAgentId={aiAgentId} onSendMessage={handleSendMessage} />;
  }

  return (
    <SearchPage
      papers={papers}
      hasSearched={hasSearched}
      isLoading={isLoading}
      error={error}
      selectedPaperIds={selectedPaperIds}
      filters={filters}
      onSearch={handleSearch}
      onFilterChange={handleFilterChange}
      onToggleSelectPaper={handleToggleSelectPaper}
      onRemovePaper={handleRemovePaper}
      onInitiateChat={handleInitiateChat}
      isInitiatingChat={isInitiatingChat}
      chatError={chatError}
    />
  );
};

export default App;
