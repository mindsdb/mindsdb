def clean_summary(summary):
    # Remove any unwanted characters or formatting
    cleaned_summary = summary.replace('\n', ' ').strip()
    return cleaned_summary

def format_metadata(metadata):
    # Format the metadata into a structured dictionary
    formatted_metadata = {
        'title': metadata.get('title', ''),
        'authors': metadata.get('authors', []),
        'abstract': clean_summary(metadata.get('abstract', '')),
        'published_date': metadata.get('published_date', ''),
        'doi': metadata.get('doi', '')
    }
    return formatted_metadata

def preprocess_data(papers):
    # Preprocess a list of academic papers
    preprocessed_papers = []
    for paper in papers:
        cleaned_paper = format_metadata(paper)
        preprocessed_papers.append(cleaned_paper)
    return preprocessed_papers