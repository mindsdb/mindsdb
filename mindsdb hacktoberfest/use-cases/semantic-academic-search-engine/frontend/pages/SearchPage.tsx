import React, { useState } from 'react';
import Header from '../components/Header';
import SearchBar from '../components/SearchBar';
import ResultsGrid from '../components/ResultsGrid';
import SelectedPapersList from '../components/SelectedPapersList';
import { Paper, FilterState } from '../types';

interface SearchPageProps {
  papers: Paper[];
  hasSearched: boolean;
  isLoading: boolean;
  error: string | null;
  selectedPaperIds: string[];
  filters: FilterState;
  onSearch: (query: string, filters: FilterState) => void;
  onFilterChange: (newFilterValue: Partial<FilterState>) => void;
  onToggleSelectPaper: (paperId: string) => void;
  onRemovePaper: (paperId: string) => void;
  onInitiateChat: () => void;
  isInitiatingChat: boolean;
  chatError: string | null;
}

const SearchPage: React.FC<SearchPageProps> = (props) => {
  const [query, setQuery] = useState('');
  const selectedPapers = props.papers.filter(p => props.selectedPaperIds.includes(p.id));
  const isSelectionCapped = props.selectedPaperIds.length >= 4;
  
  const sampleQueries = [
    "Machine Learning for Optics",
    "AI in Healthcare Patents",
    "Random Laser Technology",
  ];

  const handleSampleQueryClick = (sampleQuery: string) => {
    setQuery(sampleQuery);
    props.onSearch(sampleQuery, props.filters);
  };

  return (
    <div className="bg-white min-h-screen font-sans flex flex-col">
      <Header hideBorder={true} />
      <main className={`container mx-auto px-4 flex-grow flex flex-col transition-all duration-700 ease-in-out ${
        !props.hasSearched ? 'pt-[25vh]' : 'pt-8'
      }`}>
        
        <div className="w-full">
          <section className="text-center mb-10">
            <h1 className="text-4xl font-bold text-gray-800 mb-2">Semantica</h1>
            <p className="text-lg text-gray-600">Connecting ideas across the frontiers of knowledge</p>
          </section>
          
          <section className="mb-10">
            <SearchBar 
              query={query}
              onQueryChange={setQuery}
              onSearch={props.onSearch} 
              filters={props.filters}
              onFilterChange={props.onFilterChange}
            />
            {!props.hasSearched && (
              <div className="mt-4 text-center">
                <p className="text-sm text-gray-500 mb-2">Or try a sample search:</p>
                <div className="flex justify-center gap-2 flex-wrap">
                  {sampleQueries.map((q) => (
                    <button
                      key={q}
                      onClick={() => handleSampleQueryClick(q)}
                      className="px-3 py-1 bg-gray-100 text-gray-700 text-sm rounded-full hover:bg-gray-200 transition-colors"
                    >
                      {q}
                    </button>
                  ))}
                </div>
              </div>
            )}
          </section>
        </div>

        <section className="flex-grow pb-6">
          {props.hasSearched && (
            <ResultsGrid 
              papers={props.papers} 
              isLoading={props.isLoading} 
              error={props.error} 
              selectedPapers={props.selectedPaperIds}
              onToggleSelect={props.onToggleSelectPaper}
              isSelectionCapped={isSelectionCapped}
            />
          )}
        </section>
      </main>
      <SelectedPapersList 
        selectedPapers={selectedPapers}
        onRemovePaper={props.onRemovePaper}
        onChat={props.onInitiateChat}
        isInitiatingChat={props.isInitiatingChat}
        chatError={props.chatError}
      />
      <footer className="text-center py-4 border-t border-gray-200">
        <p className="text-sm text-gray-500">
          Made with <span className="text-red-500">&hearts;</span> for <a href="https://mindsdb.com/" target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">Mindsdb</a> Hacktoberfest
        </p>
      </footer>
    </div>
  );
};

export default SearchPage;