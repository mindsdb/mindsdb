from langchain.chains.summarize import load_summarize_chain
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.docstore.document import Document
from langchain.prompts import PromptTemplate


def get_map_reduce_chain(llm, prompt_template=None):
    if prompt_template is None:
        prompt_template = """Write a concise summary of the following:
        "{text}"
        CONCISE SUMMARY:"""

    map_prompt = PromptTemplate(template=prompt_template, input_variables=["text"])
    combine_prompt = PromptTemplate(
        template="Write a concise summary of the following summaries:\n{text}\nCONCISE SUMMARY:",
        input_variables=["text"]
    )

    chain = load_summarize_chain(
        llm,
        chain_type="map_reduce",
        map_prompt=map_prompt,
        combine_prompt=combine_prompt,
        verbose=True
    )
    return chain


def generate_summary(text, llm, chunk_size=4000, chunk_overlap=200, prompt_template=None):
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap
    )
    texts = text_splitter.split_text(text)
    docs = [Document(page_content=t) for t in texts]

    chain = get_map_reduce_chain(llm, prompt_template)
    summary = chain.run(docs)
    return summary
