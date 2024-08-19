from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class CreateKnowledgeBase(ASTNode):
    """
    Create a new knowledge base
    """
    def __init__(
        self,
        name,
        model=None,
        storage=None,
        from_select=None,
        params=None,
        if_not_exists=False,
        *args,
        **kwargs,
    ):
        """
        Args:
            name: Identifier -- name of the knowledge base
            model: Identifier -- name of the model to use
            storage: Identifier -- name of the storage to use
            from_select: SelectStatement -- select statement to use as the source of the knowledge base
            params: dict -- additional parameters to pass to the knowledge base. E.g., chunking strategy, etc.
            if_not_exists: bool -- if True, do not raise an error if the knowledge base already exists
        """
        super().__init__(*args, **kwargs)
        self.name = name
        self.model = model
        self.storage = storage
        self.params = params
        self.if_not_exists = if_not_exists
        self.from_query = from_select

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        storage_str = f"{ind} storage={self.storage.to_string()},\n" if self.storage else ""
        model_str = f"{ind} model={self.model.to_string()},\n" if self.model else ""
        out_str = f"""
        {ind}CreateKnowledgeBase(
        {ind}    if_not_exists={self.if_not_exists},
        {ind}    name={self.name.to_string()},
        {ind}    from_query={self.from_query.to_tree(level=level + 1) if self.from_query else None},
        {model_str}{storage_str}{ind}    params={self.params}
        {ind})
        """
        return out_str

    def get_string(self, *args, **kwargs):
        from_query_str = (
            f"FROM ({self.from_query.get_string()})" if self.from_query else ""
        )

        using_ar = []
        if self.storage:
            using_ar.append(f"  STORAGE={self.storage.to_string()}")
        if self.model:
            using_ar.append(f"  MODEL={self.model.to_string()}")

        params = self.params.copy()
        if params:
            using_ar += [f"{k}={repr(v)}" for k, v in params.items()]
        if using_ar:
            using_str = "USING " + ", ".join(using_ar)
        else:
            using_str = ""

        out_str = (
            f"CREATE KNOWLEDGE_BASE {'IF NOT EXISTS ' if self.if_not_exists else ''}{self.name.to_string()} "
            f"{from_query_str} "
            f"{using_str}"
        )

        return out_str

    def __repr__(self) -> str:
        return self.to_tree()


class DropKnowledgeBase(ASTNode):
    """
    Delete a knowledge base
    """
    def __init__(self, name, if_exists=False, *args, **kwargs):
        """
        Args:
            name: Identifier -- name of the knowledge base
            if_exists: bool -- if True, do not raise an error if the knowledge base does not exist
        """
        super().__init__(*args, **kwargs)
        self.name = name
        self.if_exists = if_exists

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        out_str = (
            f"{ind}DropKnowledgeBase("
            f"{ind}    if_exists={self.if_exists},"
            f"name={self.name.to_string()})"
        )
        return out_str

    def get_string(self, *args, **kwargs):
        out_str = f'DROP KNOWLEDGE_BASE {"IF EXISTS " if self.if_exists else ""}{self.name.to_string()}'
        return out_str
