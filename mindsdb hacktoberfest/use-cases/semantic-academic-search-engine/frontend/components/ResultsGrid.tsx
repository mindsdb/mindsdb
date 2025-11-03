import React from 'react';
import { Paper } from '../types';
import PaperCard from './PaperCard';

interface ResultsGridProps {
  papers: Paper[];
  isLoading: boolean;
  error: string | null;
  selectedPapers: string[];
  onToggleSelect: (paperId: string) => void;
  isSelectionCapped: boolean;
}

const ResultsGrid: React.FC<ResultsGridProps> = ({ papers, isLoading, error, selectedPapers, onToggleSelect, isSelectionCapped }) => {
  if (isLoading) {
    return (
      <div className="text-center py-10">
        <p className="text-gray-500">Searching for papers...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="text-center py-10 text-red-500">
        <p>Error: {error}</p>
      </div>
    );
  }

  if (!papers || papers.length === 0) {
    return (
      <div className="text-center py-10">
        <p className="text-gray-500">No papers found. Try another search.</p>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
      {papers.map((paper) => (
        <PaperCard
          key={paper.id}
          paper={paper}
          isSelected={selectedPapers.includes(paper.id)}
          onToggleSelect={onToggleSelect}
          isSelectionCapped={isSelectionCapped}
        />
      ))}
    </div>
  );
};

export default ResultsGrid;