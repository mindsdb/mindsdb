import os
from llama_hub.github_repo import GithubRepositoryReader


def _get_github_token(args, connection_args):
    """
    API_KEY preference order:
        1. provided at model creation
        2. provided at engine creation
        3. GITHUB_TOKEN env variable

    Note: method is not case-sensitive.
    """
    key = "GITHUB_TOKEN"
    for k in key, key.lower():
        if args.get(k):
            return args[k]

        connection_args = connection_args
        if connection_args.get(k):
            return connection_args[k]

        api_key = os.getenv(k)
        if os.environ.get(k):
            return api_key

    return None


def _get_filter_file_extensions(args):
    """
    Returns file extensions to filter, if Filter type is EXCLUDE the file extensions will be excluded
    from the knowledge source, if Filter type is INCLUDE the file extensions will be included in the
    knowledge source.
    """
    # filter_file_extensions is not provided
    if "filter_file_extensions" not in args:
        return None

    # if filter_type is provided with EXCLUDE or INCLUDE
    if args["filter_file_extensions"] and args["filter_type"].upper() == "INCLUDE":
        filter_file_extensions = args["filter_file_extensions"]
        return (filter_file_extensions, GithubRepositoryReader.FilterType.INCLUDE)
    else:
        filter_file_extensions = args["filter_file_extensions"]
        return (filter_file_extensions, GithubRepositoryReader.FilterType.EXCLUDE)


def _get_filter_directories(args):
    """
    Returns directories to filter, if Filter type is EXCLUDE the directories will be excluded
    from the knowledge source, if Filter type is INCLUDE the directories will be included in the
    knowledge source.
    """
    # filter_directories is not provided
    if "filter_directories" not in args:
        return None

    # if filter_type is provided with EXCLUDE or INCLUDE
    if args["filter_directories"] and args["filter_type"].upper() == "INCLUDE":
        filter_directories = args["filter_directories"]
        return (filter_directories, GithubRepositoryReader.FilterType.INCLUDE)
    else:
        filter_directories = args["filter_directories"]
        return (filter_directories, GithubRepositoryReader.FilterType.EXCLUDE)
