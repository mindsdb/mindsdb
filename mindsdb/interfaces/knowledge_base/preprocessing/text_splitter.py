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
                shift = len(sep) + pos
                if sep.strip(" ") == "":
                    # overlapping
                    sep2, _, shift2 = self.get_next_chunk(text, k_range * 1.5, 0)
                    if sep2.strip(" ") != "":
                        # use shift of previous separator
                        if shift - shift2 < self.chunk_overlap:
                            shift = shift2

                return sep, chunk[:pos], shift

        raise RuntimeError("Cannot split text")
