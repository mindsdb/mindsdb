# this dict is used to configure the data loader and match one on one arguments passed by the user in args
data_loaders = {
    "DFReader": {
        # TODO: add parameter of DFReader
    },
    "SimpleWebPageReader": {
        "source_url_link": "<url>",
    },
    "GithubRepositoryReader": {
        "owner": "<owner>",
        "repo": "<repository>",
        "github_token": "<github_token>",
        "branch": "<branch>",
    },
    "YoutubeTranscriptReader": {
        "ytlinks": ["<youtube_link>"],
    },
}
