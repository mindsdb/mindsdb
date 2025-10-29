
export interface Paper {
  id: string;
  title: string;
  authors: string;
  date: string;
  abstract: string;
  categories: string[];
  url: string;
  source: string;
}

export interface FilterState {
  isHybridSearch: boolean;
  alpha: number;
  corpus: {
    arxiv: boolean;
    patent: boolean;
    biorxiv: boolean;
    medrxiv: boolean;
    chemrxiv: boolean;
  };
  publishedYear: string;
  category: string;
}

export interface ChatDocument {
  paperId: string;
  paperUrl: string;
  title: string;
  source: string;
}
