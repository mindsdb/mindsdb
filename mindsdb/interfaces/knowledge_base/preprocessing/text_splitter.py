from typing import List


class TextSplitter:
    def __init__(
        self,
        chunk_size: int = 1000,
        chunk_overlap: int = 200,
        separators: List[str] = None,
        k_range: float = 0.5,
        k_ratio: float = 1,
    ):
        """
        Split text into chunks. The logic:
         - Get a piece of text with chunk_size and try to find the separator at the end of the piece.
         - The allowed range to find the separator is defined by k_range and k_ratio using formula:
            k_range * chunk_size / (num * k_ratio + 1)
            num - is number of a separator from the list
         - if the separator is not in the rage: switch to the next separator
         - if the found separator is in the middle of the sentence, use overlapping:
            - the found text is the current chunk
            - repeat the search with less strict k_range and k_ratio
            - the found text will be the beginning of the next chunk

        :param chunk_size: size of the chunk, which must not be exceeded
        :param separators: list of separators in order of priority
        :param k_range: defines the range to look for the separator
        :param k_ratio: defines how much to shrink the range for the next separator
        """
        if separators is None:
            separators = ["\n\n", "\n", ". ", " ", ""]
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.separators = separators
        self.k_range = k_range
        self.k_ratio = k_ratio

    def split_text(self, text: str) -> List[str]:
        chunks = []

        while True:
            if len(text) < self.chunk_size:
                chunks.append(text)
                break

            sep, chunk, shift = self.get_next_chunk(text, self.k_range, self.k_ratio)
            chunks.append(chunk)

            text = text[shift:]
        return chunks

    def get_next_chunk(self, text: str, k_range: float, k_ratio: float):
        # returns chunk with separator and shift for the next search iteration

        chunk = text[: self.chunk_size]
        # positions = []
        for i, sep in enumerate(self.separators):
            pos = chunk.rfind(sep)

            vpos = self.chunk_size - pos
            if vpos < k_range * self.chunk_size / (i * k_ratio + 1):
                # The chunk content ends at position 'pos' (before the separator)
                # Calculate shift for the next chunk start position

                if self.chunk_overlap > 0:
                    # With overlap: next chunk should start chunk_overlap characters before
                    # the end of current chunk, ensuring those characters appear in both chunks
                    # Current chunk is text[0:pos], so to overlap by chunk_overlap,
                    # next chunk should start at max(pos - chunk_overlap, 1)
                    shift = max(pos - self.chunk_overlap, 1)

                    # Adjust shift to start at a word boundary
                    # Move shift forward to the next word boundary (space, newline, or punctuation)
                    shift = self._find_word_boundary(text, shift, pos)
                else:
                    # Without overlap: skip past the separator
                    shift = pos + len(sep)

                return sep, chunk[:pos], shift

        raise RuntimeError("Cannot split text")

    def _find_word_boundary(self, text: str, start_pos: int, max_pos: int) -> int:
        """
        Find the nearest word boundary at or after start_pos, but before max_pos.
        A word boundary is defined as the position right after a whitespace or punctuation character.

        :param text: The full text
        :param start_pos: The starting position to search from
        :param max_pos: The maximum position (don't go beyond this)
        :return: Position of word boundary
        """
        # If we're at position 0 or already at a word boundary, return as-is
        if start_pos <= 0:
            return start_pos

        # Check if we're already at a word boundary
        # (current position starts with non-alphanumeric or previous char is non-alphanumeric)
        if start_pos < len(text):
            if not text[start_pos].isalnum():
                # We're at punctuation/space, move to next alphanumeric
                while start_pos < len(text) and start_pos < max_pos and not text[start_pos].isalnum():
                    start_pos += 1
                return start_pos
            elif start_pos > 0 and not text[start_pos - 1].isalnum():
                # Previous char is not alphanumeric, so we're at start of a word
                return start_pos

        # We're in the middle of a word, find the next word boundary
        # First, skip to the end of the current word
        while start_pos < len(text) and start_pos < max_pos and text[start_pos].isalnum():
            start_pos += 1

        # Now skip any whitespace/punctuation to get to the start of the next word
        while start_pos < len(text) and start_pos < max_pos and not text[start_pos].isalnum():
            start_pos += 1

        # If we went beyond max_pos, just return start_pos (better to have some mid-word than no overlap)
        return min(start_pos, max_pos)
