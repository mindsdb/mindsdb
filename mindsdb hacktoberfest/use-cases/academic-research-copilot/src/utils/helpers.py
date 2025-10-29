def clean_text(text):
    # Remove unwanted characters and whitespace
    return ' '.join(text.split())

def extract_keywords(text, num_keywords=5):
    # Simple keyword extraction based on word frequency
    from collections import Counter
    words = text.split()
    word_counts = Counter(words)
    return [word for word, _ in word_counts.most_common(num_keywords)]

def format_reference(authors, title, journal, year):
    # Format a reference string
    authors_str = ', '.join(authors)
    return f"{authors_str}. \"{title}.\" {journal}, {year}."

def validate_email(email):
    # Basic email validation
    import re
    pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(pattern, email) is not None

def generate_summary(text, max_length=100):
    # Generate a simple summary by truncating the text
    return text[:max_length] + ('...' if len(text) > max_length else '')