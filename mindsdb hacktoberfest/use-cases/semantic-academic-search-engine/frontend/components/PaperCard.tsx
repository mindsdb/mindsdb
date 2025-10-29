import React from 'react';
import { Paper } from '../types';

interface PaperCardProps {
  paper: Paper;
  isSelected: boolean;
  onToggleSelect: (paperId: string) => void;
  isSelectionCapped: boolean;
}

const PaperCard: React.FC<PaperCardProps> = ({ paper, isSelected, onToggleSelect, isSelectionCapped }) => {
  const typeStyles = {
    arxiv: 'bg-blue-100 text-blue-800',
    patent: 'bg-green-100 text-green-800',
    biorxiv: 'bg-red-100 text-red-800',
    medrxiv: 'bg-yellow-100 text-yellow-800',
    chemrxiv: 'bg-purple-100 text-purple-800'
  };

  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4 flex flex-col h-full shadow-sm hover:shadow-md transition-shadow">
      <div className="flex-grow">
        <span className={`text-xs font-semibold inline-block py-1 px-2 uppercase rounded-full mb-2 ${typeStyles[paper.source]}`}>
          {paper.source}
        </span>
        <h3 className="text-md font-semibold text-gray-800 mb-1">
          <a href={paper.url} target="_blank" rel="noopener noreferrer" className="hover:text-indigo-600 transition-colors">
            {paper.title}
          </a>
        </h3>
        <p className="text-sm text-gray-500 mb-1">{paper.authors} - <span className="italic">{paper.date}</span></p>
        <div className="flex flex-wrap gap-1 mb-3">
          {paper.categories.map(tag => (
            <span key={tag} className="bg-gray-100 text-gray-600 text-xs font-medium px-2 py-0.5 rounded-full">{tag}</span>
          ))}
        </div>
        <p className="text-sm text-gray-600 leading-relaxed">{paper.abstract}</p>
      </div>
      <div className="mt-4 pt-3 border-t border-gray-100 flex justify-center">
        <button
          onClick={() => onToggleSelect(paper.id)}
          disabled={isSelectionCapped && !isSelected}
          className={`px-4 py-1 text-sm font-medium rounded-md transition-colors border border-gray-900 disabled:opacity-50 disabled:cursor-not-allowed ${
            isSelected
              ? 'bg-gray-900 text-white hover:bg-gray-700'
              : 'bg-white text-gray-900 hover:bg-gray-100'
          }`}
        >
          {isSelected ? 'Remove from Chat' : 'Add to Chat'}
        </button>
      </div>
    </div>
  );
};

export default PaperCard;