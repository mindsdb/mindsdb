import re
from sly import Lexer
from sly.lex import LexError

"""
Unfortunately we can't inherit from base SQLLexer, because the order of rules is important.
If we do, like in MySQL lexer, the new rules like `DATASOURCE = r'\bDATASOURCE\b'` are added to the end of the rule list.
Then, for an input `DATASOURCE`, the last matched regexp is `STRING`, and the token is incorrectly classified 
as a string.
"""
class MindsDBLexer(Lexer):
    reflags = re.IGNORECASE
    ignore = ' \t\r'
    ignore_multi_comment = r'/\*[\s\S]*?\*/'
    ignore_line_comment = r'--[^\n]*'

    tokens = {
        USE, DROP, CREATE, DESCRIBE, RETRAIN, REPLACE,

        # Misc
        SET, START, TRANSACTION, COMMIT, ROLLBACK, ALTER, EXPLAIN,
        ISOLATION, LEVEL, REPEATABLE, READ, WRITE, UNCOMMITTED, COMMITTED,
        SERIALIZABLE, ONLY, CONVERT, BEGIN,

        # Mindsdb special

        PREDICTOR, PREDICTORS, DATASOURCE, INTEGRATION, INTEGRATIONS,DATASOURCES,
        STREAM, STREAMS, PUBLICATION, PUBLICATIONS, VIEW, VIEWS, DATASETS, DATASET,
        MODEL, MODELS, ML_ENGINE, ML_ENGINES, HANDLERS,
        FINETUNE, EVALUATE,
        LATEST, LAST, HORIZON, USING,
        ENGINE, TRAIN, PREDICT, PARAMETERS, JOB, CHATBOT, EVERY,PROJECT,
        ANOMALY, DETECTION,
        KNOWLEDGE_BASE, KNOWLEDGE_BASES,
        SKILL,
        AGENT,

        # SHOW/DDL Keywords

        SHOW, SCHEMAS, SCHEMA, DATABASES, DATABASE, TABLES, TABLE, FULL, EXTENDED, PROCESSLIST,
        MUTEX, CODE, SLAVE, REPLICA, REPLICAS, CHANNEL, TRIGGERS, TRIGGER, KEYS, STORAGE, LOGS, BINARY,
        MASTER, PRIVILEGES, PROFILES, HOSTS, OPEN, INDEXES,
        VARIABLES, SESSION, STATUS,
        GLOBAL, PROCEDURE, FUNCTION, INDEX, WARNINGS,
        ENGINES, CHARSET, COLLATION, PLUGINS, CHARACTER,
        PERSIST, PERSIST_ONLY,
        IF_EXISTS, IF_NOT_EXISTS, IF, COLUMNS, FIELDS, COLLATE, SEARCH_PATH,
        VARIABLE, SYSTEM_VARIABLE,

        # SELECT Keywords
        WITH, SELECT, DISTINCT, FROM, WHERE, AS,
        LIMIT, OFFSET, ASC, DESC, NULLS_FIRST, NULLS_LAST,
        GROUP_BY, HAVING, ORDER_BY,
        STAR, FOR, UPDATE,

        JOIN, INNER, OUTER, CROSS, LEFT, RIGHT, ON,

        UNION, ALL,

        # CASE
        CASE, ELSE, END, THEN, WHEN,

        # DML
        INSERT, DELETE, INTO, VALUES,

        # Special
        DOT, COMMA, LPAREN, RPAREN, PARAMETER,
        # json
        LBRACE, RBRACE, LBRACKET, RBRACKET, COLON, SEMICOLON,

        # Operators
        PLUS, MINUS, DIVIDE, MODULO,
        EQUALS, NEQUALS, GREATER, GEQ, LESS, LEQ,
        AND, OR, NOT, IS, IS_NOT,
        IN, LIKE, NOT_LIKE, CONCAT, BETWEEN, WINDOW, OVER, PARTITION_BY,
        JSON_GET, JSON_GET_STR, INTERVAL,

        # Data types
        CAST, ID, INTEGER, FLOAT, QUOTE_STRING, DQUOTE_STRING, NULL, TRUE, FALSE,

    }

    RETRAIN = r'\bRETRAIN\b'
    FINETUNE = r'\bFINETUNE\b'
    # Custom commands

    USE = r'\bUSE\b'
    ENGINE = r'\bENGINE\b'
    TRAIN = r'\bTRAIN\b'
    PREDICT = r'\bPREDICT\b'
    DROP = r'\bDROP\b'
    PARAMETERS = r'\bPARAMETERS\b'
    HORIZON = r'\bHORIZON\b'
    USING = r'\bUSING\b'
    VIEW = r'\bVIEW\b'
    VIEWS = r'\bVIEWS\b'
    STREAM = r'\bSTREAM\b'
    STREAMS = r'\bSTREAMS\b'
    PREDICTOR = r'\bPREDICTOR\b'
    PREDICTORS = r'\bPREDICTORS\b'
    DATASOURCE = r'\bDATASOURCE\b'
    INTEGRATION = r'\bINTEGRATION\b'
    INTEGRATIONS = r'\bINTEGRATIONS\b'
    DATASOURCES = r'\bDATASOURCES\b'
    PUBLICATION = r'\bPUBLICATION\b'
    PUBLICATIONS = r'\bPUBLICATIONS\b'
    DATASETS = r'\bDATASETS\b'
    DATASET = r'\bDATASET\b'
    LATEST = r'\bLATEST\b'
    LAST = r'\bLAST\b'
    MODEL = r'\bMODEL\b'
    MODELS = r'\bMODELS\b'
    ML_ENGINE = r'\bML_ENGINE\b'
    ML_ENGINES = r'\bML_ENGINES\b'
    HANDLERS = r'\bHANDLERS\b'
    JOB = r'\bJOB\b'
    CHATBOT = r'\bCHATBOT\b'
    EVERY = r'\bEVERY\b'
    PROJECT = r'\bPROJECT\b'
    EVALUATE = r'\bEVALUATE\b'

    # Typed models
    ANOMALY = r'\bANOMALY\b'
    DETECTION = r'\bDETECTION\b'

    KNOWLEDGE_BASE = r'\bKNOWLEDGE[_|\s]BASE\b'
    KNOWLEDGE_BASES = r'\bKNOWLEDGE[_|\s]BASES\b'
    SKILL = r'\bSKILL\b'
    AGENT = r'\bAGENT\b'

    # Misc
    SET = r'\bSET\b'
    START = r'\bSTART\b'
    TRANSACTION = r'\bTRANSACTION\b'
    COMMIT = r'\bCOMMIT\b'
    ROLLBACK = r'\bROLLBACK\b'
    EXPLAIN = r'\bEXPLAIN\b'
    ALTER = r'\bALTER\b'
    ISOLATION = r'\bISOLATION\b'
    LEVEL = r'\bLEVEL\b'
    REPEATABLE = r'\bREPEATABLE\b'
    READ = r'\bREAD\b'
    WRITE = r'\bWRITE\b'
    UNCOMMITTED = r'\bUNCOMMITTED\b'
    COMMITTED = r'\bCOMMITTED\b'
    SERIALIZABLE = r'\bSERIALIZABLE\b'
    ONLY = r'\bONLY\b'
    CONVERT = r'\bCONVERT\b'
    DESCRIBE = r'\bDESCRIBE\b'
    BEGIN = r'\bBEGIN\b'

    # SHOW
    SHOW = r'\bSHOW\b'
    SCHEMAS = r'\bSCHEMAS\b'
    SCHEMA = r'\bSCHEMA\b'
    DATABASES = r'\bDATABASES\b'
    DATABASE = r'\bDATABASE\b'
    TABLES = r'\bTABLES\b'
    TABLE = r'\bTABLE\b'
    FULL = r'\bFULL\b'
    VARIABLES = r'\bVARIABLES\b'
    SESSION = r'\bSESSION\b'
    STATUS = r'\bSTATUS\b'
    GLOBAL = r'\bGLOBAL\b'
    PROCEDURE = r'\bPROCEDURE\b'
    FUNCTION = r'\bFUNCTION\b'
    INDEX = r'\bINDEX\b'
    CREATE = r'\bCREATE\b'
    WARNINGS = r'\bWARNINGS\b'
    ENGINES = r'\bENGINES\b'
    CHARSET = r'\bCHARSET\b'
    CHARACTER = r'\bCHARACTER\b'
    COLLATION = r'\bCOLLATION\b'
    PLUGINS = r'\bPLUGINS\b'
    PERSIST = r'\bPERSIST\b'
    PERSIST_ONLY = r'\bPERSIST_ONLY\b'
    IF_EXISTS = r'\bIF[\s]+EXISTS\b'
    IF_NOT_EXISTS = r'\bIF[\s]+NOT[\s]+EXISTS\b'
    IF = r'\bIF\b'
    COLUMNS = r'\bCOLUMNS\b'
    FIELDS = r'\bFIELDS\b'
    EXTENDED = r'\bEXTENDED\b'
    PROCESSLIST = r'\bPROCESSLIST\b'
    MUTEX = r'\bMUTEX\b'
    CODE = r'\bCODE\b'
    SLAVE = r'\bSLAVE\b'
    REPLICA = r'\bREPLICA\b'
    REPLICAS = r'\bREPLICAS\b'
    CHANNEL = r'\bCHANNEL\b'
    TRIGGERS = r'\bTRIGGERS\b'
    TRIGGER = r'\bTRIGGER\b'
    KEYS = r'\bKEYS\b'
    STORAGE = r'\bSTORAGE\b'
    LOGS = r'\bLOGS\b'
    BINARY = r'\bBINARY\b'
    MASTER = r'\bMASTER\b'
    PRIVILEGES = r'\bPRIVILEGES\b'
    PROFILES = r'\bPROFILES\b'
    HOSTS = r'\bHOSTS\b'
    OPEN = r'\bOPEN\b'
    INDEXES = r'\bINDEXES\b'
    REPLACE = r'\bREPLACE\b'
    COLLATE = r'\bCOLLATE\b'
    SEARCH_PATH = r'\bSEARCH_PATH\b'

    # SELECT

    ON = r'\bON\b'
    ASC = r'\bASC\b'
    DESC = r'\bDESC\b'
    NULLS_FIRST = r'\bNULLS FIRST\b'
    NULLS_LAST = r'\bNULLS LAST\b'
    WITH = r'\bWITH\b'
    SELECT = r'\bSELECT\b'
    DISTINCT = r'\bDISTINCT\b'
    FROM = r'\bFROM\b'
    AS = r'\bAS\b'
    WHERE = r'\bWHERE\b'
    LIMIT = r'\bLIMIT\b'
    OFFSET = r'\bOFFSET\b'
    GROUP_BY = r'\bGROUP BY\b'
    HAVING = r'\bHAVING\b'
    ORDER_BY = r'\bORDER BY\b'
    STAR = r'\*'
    FOR = r'\bFOR\b'
    UPDATE = r'\bUPDATE\b'

    JOIN = r'\bJOIN\b'
    INNER = r'\bINNER\b'
    OUTER = r'\bOUTER\b'
    CROSS = r'\bCROSS\b'
    LEFT = r'\bLEFT\b'
    RIGHT = r'\bRIGHT\b'

    # UNION

    UNION = r'\bUNION\b'
    ALL = r'\bALL\b'

    # CASE
    CASE = r'\bCASE\b'
    ELSE = r'\bELSE\b'
    END = r'\bEND\b'
    THEN = r'\bTHEN\b'
    WHEN = r'\bWHEN\b'

    # DML
    INSERT = r'\bINSERT\b'
    DELETE = r'\bDELETE\b'
    INTO = r'\bINTO\b'
    VALUES = r'\bVALUES\b'

    # Special
    DOT = r'\.'
    COMMA = r','
    LPAREN = r'\('
    RPAREN = r'\)'
    PARAMETER = r'\?'
    # json
    LBRACE = r'\{'
    RBRACE = r'\}'
    LBRACKET = r'\['
    RBRACKET = r'\]'
    COLON = r'\:'
    SEMICOLON = r'\;'

    # Operators
    JSON_GET = r'->'
    JSON_GET_STR = r'->>'
    PLUS = r'\+'
    MINUS = r'-'
    DIVIDE = r'/'
    MODULO = r'%'
    EQUALS = r'='
    NEQUALS = r'(!=|<>)'
    GEQ = r'>='
    GREATER = r'>'
    LEQ = r'<='
    LESS = r'<'
    AND = r'\bAND\b'
    OR = r'\bOR\b'
    IS_NOT = r'\bIS[\s]+NOT\b'
    NOT_LIKE = r'\bNOT[\s]+LIKE\b'
    NOT = r'\bNOT\b'
    IS = r'\bIS\b'
    LIKE = r'\bLIKE\b'
    IN = r'\bIN\b'
    CAST = r'\bCAST\b'
    CONCAT = r'\|\|'
    BETWEEN = r'\bBETWEEN\b'
    INTERVAL = r'\bINTERVAL\b'
    WINDOW = r'\bWINDOW\b'
    OVER = r'\bOVER\b'
    PARTITION_BY = r'\bPARTITION BY\b'

    # Data types
    NULL = r'\bNULL\b'
    TRUE = r'\bTRUE\b'
    FALSE = r'\bFALSE\b'

    @_(r'(?:([a-zA-Z_$0-9]*[a-zA-Z_$]+[a-zA-Z_$0-9]*)|(?:`([^`]+)`))')
    def ID(self, t):
        return t

    @_(r'\d+\.\d+')
    def FLOAT(self, t):
        return t

    @_(r'\d+')
    def INTEGER(self, t):
        return t

    @_(r"'(?:\\.|[^'])*'")
    def QUOTE_STRING(self, t):
        t.value = t.value.replace('\\"', '"').replace("\\'", "'")
        return t

    @_(r'"(?:\\.|[^"])*"')
    def DQUOTE_STRING(self, t):
        t.value = t.value.replace('\\"', '"').replace("\\'", "'")
        return t

    @_(r'\n+')
    def ignore_newline(self, t):
        self.lineno += len(t.value)

    @_(r'@[a-zA-Z_.$]+',
       r"@'[a-zA-Z_.$][^']*'",
       r"@`[a-zA-Z_.$][^`]*`",
       r'@"[a-zA-Z_.$][^"]*"'
       )
    def VARIABLE(self, t):
        t.value = t.value.lstrip('@')

        if t.value[0] == '"':
            t.value = t.value.strip('\"')
        elif t.value[0] == "'":
            t.value = t.value.strip('\'')
        elif t.value[0] == "`":
            t.value = t.value.strip('`')
        return t

    @_(r'@@[a-zA-Z_.$]+',
       r"@@'[a-zA-Z_.$][^']*'",
       r"@@`[a-zA-Z_.$][^`]*`",
       r'@@"[a-zA-Z_.$][^"]*"'
       )
    def SYSTEM_VARIABLE(self, t):
        t.value = t.value.lstrip('@')

        if t.value[0] == '"':
            t.value = t.value.strip('\"')
        elif t.value[0] == "'":
            t.value = t.value.strip('\'')
        elif t.value[0] == "`":
            t.value = t.value.strip('`')
        return t

    def error(self, t):

        # convert to lines
        lines = []
        shift = 0
        error_line = 0
        error_index = 0
        for i, line in enumerate(self.text.split('\n')):
            if 0 <= t.index - shift < len(line):
                error_line = i
                error_index = t.index - shift
            lines.append(line)
            shift += len(line) + 1

        msgs = [f'Illegal character {t.value[0]!r}:']
        # show error code
        for line in lines[error_line - 1: error_line + 1]:
            msgs.append('>' + line)

        msgs.append('-' * (error_index + 1) + '^')

        raise LexError('\n'.join(msgs), t.value, self.index)
