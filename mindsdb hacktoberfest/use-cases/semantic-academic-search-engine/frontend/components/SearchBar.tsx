import React, { useState } from 'react';
import { FilterState } from '../types';
import { SearchIcon } from './icons/SearchIcon';
import { SettingsIcon } from './icons/SettingsIcon';

interface SearchBarProps {
  query: string;
  onQueryChange: (query: string) => void;
  onSearch: (query: string, filters: FilterState) => void;
  filters: FilterState;
  onFilterChange: (newFilterValue: Partial<FilterState>) => void;
}

const SearchBar: React.FC<SearchBarProps> = ({ query, onQueryChange, onSearch, filters, onFilterChange }) => {
  const [showFilters, setShowFilters] = useState(false);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (query.trim()) {
      onSearch(query.trim(), filters);
    }
  };

  const handleYearChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    if (/^\d{0,4}$/.test(value)) {
      onFilterChange({ publishedYear: value });
    }
  };

  return (
    <div className="max-w-3xl mx-auto">
      <form onSubmit={handleSubmit} className="flex items-center bg-white border border-gray-200 rounded-full shadow-sm p-2">
        <input
          type="text"
          value={query}
          onChange={(e) => onQueryChange(e.target.value)}
          placeholder="Search for academic papers..."
          className="flex-grow px-4 py-2 bg-transparent focus:outline-none text-gray-900"
        />
        <button
          type="button"
          onClick={() => setShowFilters(!showFilters)}
          className="p-2 text-gray-500 hover:text-gray-800 hover:bg-gray-100 rounded-full transition-colors"
          aria-label="Toggle filters"
        >
          <SettingsIcon className="h-5 w-5" />
        </button>
        <button
          type="submit"
          className="ml-2 p-3 rounded-full text-gray-900 hover:bg-gray-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-900 transition-colors"
          aria-label="Search"
        >
          <SearchIcon className="h-5 w-5" />
        </button>
      </form>

      {showFilters && (
        <div className="mt-4 p-4 bg-gray-50 border border-gray-200 rounded-lg">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <label className="flex items-center space-x-2 text-sm text-gray-900">
                <input
                  type="checkbox"
                  checked={filters.isHybridSearch}
                  onChange={(e) => onFilterChange({ isHybridSearch: e.target.checked })}
                />
                <span>Enable Hybrid Search</span>
              </label>
              {filters.isHybridSearch && (
                <div className="mt-2 pl-6">
                  <label htmlFor="alpha" className="block text-xs text-gray-900">Alpha: {filters.alpha}</label>
                  <input
                    id="alpha"
                    type="range"
                    min="0"
                    max="1"
                    step="0.01"
                    value={filters.alpha}
                    onChange={(e) => onFilterChange({ alpha: parseFloat(e.target.value) })}
                    className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer accent-black"
                  />
                </div>
              )}
            </div>
            <div>
              <p className="text-sm font-medium text-gray-900 mb-2">Corpus</p>
              <div className="space-y-2">
                <label className="flex items-center space-x-2 text-sm text-gray-900">
                  <input
                    type="checkbox"
                    checked={filters.corpus.arxiv}
                    onChange={(e) => onFilterChange({ corpus: { ...filters.corpus, arxiv: e.target.checked } })}
                  />
                  <span>arXiv</span>
                </label>
                <label className="flex items-center space-x-2 text-sm text-gray-900">
                  <input
                    type="checkbox"
                    checked={filters.corpus.patent}
                    onChange={(e) => onFilterChange({ corpus: { ...filters.corpus, patent: e.target.checked } })}
                  />
                  <span>Patents</span>
                </label>
                <label className="flex items-center space-x-2 text-sm text-gray-900">
                  <input
                    type="checkbox"
                    checked={filters.corpus.biorxiv}
                    onChange={(e) => onFilterChange({ corpus: { ...filters.corpus, biorxiv: e.target.checked } })}
                  />
                  <span>Biorxiv</span>
                </label>
                <label className="flex items-center space-x-2 text-sm text-gray-900">
                  <input
                    type="checkbox"
                    checked={filters.corpus.medrxiv}
                    onChange={(e) => onFilterChange({ corpus: { ...filters.corpus, medrxiv: e.target.checked } })}
                  />
                  <span>Medrxiv</span>
                </label>
                <label className="flex items-center space-x-2 text-sm text-gray-900">
                  <input
                    type="checkbox"
                    checked={filters.corpus.chemrxiv}
                    onChange={(e) => onFilterChange({ corpus: { ...filters.corpus, chemrxiv: e.target.checked } })}
                  />
                  <span>Chemrxiv</span>
                </label>
              </div>
            </div>
            <div className="md:col-span-1">
              <label htmlFor="publishedYear" className="block text-sm font-medium text-gray-900 mb-1">Published Year</label>
              <input
                id="publishedYear"
                type="text"
                value={filters.publishedYear}
                onChange={handleYearChange}
                placeholder="e.g., 2023"
                className="w-full px-3 py-2 text-sm bg-white border border-gray-300 rounded-md focus:outline-none focus:ring-1 focus:ring-indigo-500 text-gray-900"
              />
            </div>
            <div className="md:col-span-1">
              <label htmlFor="category" className="block text-sm font-medium text-gray-900 mb-1">Category</label>
              <input
                id="category"
                type="text"
                value={filters.category}
                onChange={(e) => onFilterChange({ category: e.target.value })}
                placeholder="e.g., cs.LG"
                className="w-full px-3 py-2 text-sm bg-white border border-gray-300 rounded-md focus:outline-none focus:ring-1 focus:ring-indigo-500 text-gray-900"
              />
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default SearchBar;