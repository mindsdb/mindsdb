# -----------------------------------------------------------------------------
# sly: yacc.py
#
# Copyright (C) 2016-2018
# David M. Beazley (Dabeaz LLC)
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# * Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# * Neither the name of the David Beazley or Dabeaz LLC may be used to
#   endorse or promote products derived from this software without
#  specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# -----------------------------------------------------------------------------

import sys
import inspect
from collections import OrderedDict, defaultdict, Counter

__all__        = [ 'Parser' ]

class YaccError(Exception):
    '''
    Exception raised for yacc-related build errors.
    '''
    pass

#-----------------------------------------------------------------------------
#                     === User configurable parameters ===
#
# Change these to modify the default behavior of yacc (if you wish).  
# Move these parameters to the Yacc class itself.
#-----------------------------------------------------------------------------

ERROR_COUNT = 3                # Number of symbols that must be shifted to leave recovery mode
MAXINT = sys.maxsize

# This object is a stand-in for a logging object created by the
# logging module.   SLY will use this by default to create things
# such as the parser.out file.  If a user wants more detailed
# information, they can create their own logging object and pass
# it into SLY.

class SlyLogger(object):
    def __init__(self, f):
        self.f = f

    def debug(self, msg, *args, **kwargs):
        self.f.write((msg % args) + '\n')

    info = debug

    def warning(self, msg, *args, **kwargs):
        self.f.write('WARNING: ' + (msg % args) + '\n')

    def error(self, msg, *args, **kwargs):
        self.f.write('ERROR: ' + (msg % args) + '\n')

    critical = debug


# ----------------------------------------------------------------------
# This class is used to hold non-terminal grammar symbols during parsing.
# It normally has the following attributes set:
#        .type       = Grammar symbol type
#        .value      = Symbol value
#        .lineno     = Starting line number
#        .index      = Starting lex position
# ----------------------------------------------------------------------

class YaccSymbol:
    def __str__(self):
        return self.type

    def __repr__(self):
        return str(self)

# ----------------------------------------------------------------------
# This class is a wrapper around the objects actually passed to each
# grammar rule.   Index lookup and assignment actually assign the
# .value attribute of the underlying YaccSymbol object.
# The lineno() method returns the line number of a given
# item (or 0 if not defined).   
# ----------------------------------------------------------------------

class YaccProduction:
    __slots__ = ('_slice', '_namemap', '_stack')
    def __init__(self, s, stack=None):
        self._slice = s
        self._namemap = { }
        self._stack = stack

    def __getitem__(self, n):
        if n >= 0:
            return self._slice[n].value
        else:
            return self._stack[n].value

    def __setitem__(self, n, v):
        if n >= 0:
            self._slice[n].value = v
        else:
            self._stack[n].value = v

    def __len__(self):
        return len(self._slice)

    @property
    def lineno(self):
        for tok in self._slice:
            lineno = getattr(tok, 'lineno', None)
            if lineno:
                return lineno
        raise AttributeError('No line number found')

    @property
    def index(self):
        for tok in self._slice:
            index = getattr(tok, 'index', None)
            if index is not None:
                return index
        raise AttributeError('No index attribute found')

    @property
    def end(self):
        result = None
        for tok in self._slice:
            r = getattr(tok, 'end', None)
            if r:
                result = r
        return result
    
    def __getattr__(self, name):
        if name in self._namemap:
            return self._namemap[name](self._slice)
        else:
            nameset = '{' + ', '.join(self._namemap) + '}'
            raise AttributeError(f'No symbol {name}. Must be one of {nameset}.')

    def __setattr__(self, name, value):
        if name[:1] == '_':
            super().__setattr__(name, value)
        else:
            raise AttributeError(f"Can't reassign the value of attribute {name!r}")

# -----------------------------------------------------------------------------
#                          === Grammar Representation ===
#
# The following functions, classes, and variables are used to represent and
# manipulate the rules that make up a grammar.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# class Production:
#
# This class stores the raw information about a single production or grammar rule.
# A grammar rule refers to a specification such as this:
#
#       expr : expr PLUS term
#
# Here are the basic attributes defined on all productions
#
#       name     - Name of the production.  For example 'expr'
#       prod     - A list of symbols on the right side ['expr','PLUS','term']
#       prec     - Production precedence level
#       number   - Production number.
#       func     - Function that executes on reduce
#       file     - File where production function is defined
#       lineno   - Line number where production function is defined
#
# The following attributes are defined or optional.
#
#       len       - Length of the production (number of symbols on right hand side)
#       usyms     - Set of unique symbols found in the production
# -----------------------------------------------------------------------------

class Production(object):
    reduced = 0
    def __init__(self, number, name, prod, precedence=('right', 0), func=None, file='', line=0):
        self.name     = name
        self.prod     = tuple(prod)
        self.number   = number
        self.func     = func
        self.file     = file
        self.line     = line
        self.prec     = precedence
        
        # Internal settings used during table construction
        self.len  = len(self.prod)   # Length of the production

        # Create a list of unique production symbols used in the production
        self.usyms = []
        symmap = defaultdict(list)
        for n, s in enumerate(self.prod):
            symmap[s].append(n)
            if s not in self.usyms:
                self.usyms.append(s)

        # Create a name mapping
        # First determine (in advance) if there are duplicate names
        namecount = defaultdict(int)
        for key in self.prod:
            namecount[key] += 1
            if key in _name_aliases:
                for key in _name_aliases[key]:
                    namecount[key] += 1

        # Now, walk through the names and generate accessor functions
        nameuse = defaultdict(int)
        namemap = { }
        for index, key in enumerate(self.prod):
            if namecount[key] > 1:
                k = f'{key}{nameuse[key]}'
                nameuse[key] += 1
            else:
                k = key
            namemap[k] = lambda s,i=index: s[i].value
            if key in _name_aliases:
                for n, alias in enumerate(_name_aliases[key]):
                    if namecount[alias] > 1:
                        k = f'{alias}{nameuse[alias]}'
                        nameuse[alias] += 1
                    else:
                        k = alias
                    # The value is either a list (for repetition) or a tuple for optional 
                    namemap[k] = lambda s,i=index,n=n: ([x[n] for x in s[i].value]) if isinstance(s[i].value, list) else s[i].value[n]

        self.namemap = namemap
                
        # List of all LR items for the production
        self.lr_items = []
        self.lr_next = None

    def __str__(self):
        if self.prod:
            s = '%s -> %s' % (self.name, ' '.join(self.prod))
        else:
            s = f'{self.name} -> <empty>'

        if self.prec[1]:
            s += '  [precedence=%s, level=%d]' % self.prec

        return s

    def __repr__(self):
        return f'Production({self})'

    def __len__(self):
        return len(self.prod)

    def __nonzero__(self):
        raise RuntimeError('Used')
        return 1

    def __getitem__(self, index):
        return self.prod[index]

    # Return the nth lr_item from the production (or None if at the end)
    def lr_item(self, n):
        if n > len(self.prod):
            return None
        p = LRItem(self, n)
        # Precompute the list of productions immediately following.
        try:
            p.lr_after = Prodnames[p.prod[n+1]]
        except (IndexError, KeyError):
            p.lr_after = []
        try:
            p.lr_before = p.prod[n-1]
        except IndexError:
            p.lr_before = None
        return p

# -----------------------------------------------------------------------------
# class LRItem
#
# This class represents a specific stage of parsing a production rule.  For
# example:
#
#       expr : expr . PLUS term
#
# In the above, the "." represents the current location of the parse.  Here
# basic attributes:
#
#       name       - Name of the production.  For example 'expr'
#       prod       - A list of symbols on the right side ['expr','.', 'PLUS','term']
#       number     - Production number.
#
#       lr_next      Next LR item. Example, if we are ' expr -> expr . PLUS term'
#                    then lr_next refers to 'expr -> expr PLUS . term'
#       lr_index   - LR item index (location of the ".") in the prod list.
#       lookaheads - LALR lookahead symbols for this item
#       len        - Length of the production (number of symbols on right hand side)
#       lr_after    - List of all productions that immediately follow
#       lr_before   - Grammar symbol immediately before
# -----------------------------------------------------------------------------

class LRItem(object):
    def __init__(self, p, n):
        self.name       = p.name
        self.prod       = list(p.prod)
        self.number     = p.number
        self.lr_index   = n
        self.lookaheads = {}
        self.prod.insert(n, '.')
        self.prod       = tuple(self.prod)
        self.len        = len(self.prod)
        self.usyms      = p.usyms

    def __str__(self):
        if self.prod:
            s = '%s -> %s' % (self.name, ' '.join(self.prod))
        else:
            s = f'{self.name} -> <empty>'
        return s

    def __repr__(self):
        return f'LRItem({self})'

# -----------------------------------------------------------------------------
# rightmost_terminal()
#
# Return the rightmost terminal from a list of symbols.  Used in add_production()
# -----------------------------------------------------------------------------
def rightmost_terminal(symbols, terminals):
    i = len(symbols) - 1
    while i >= 0:
        if symbols[i] in terminals:
            return symbols[i]
        i -= 1
    return None

# -----------------------------------------------------------------------------
#                           === GRAMMAR CLASS ===
#
# The following class represents the contents of the specified grammar along
# with various computed properties such as first sets, follow sets, LR items, etc.
# This data is used for critical parts of the table generation process later.
# -----------------------------------------------------------------------------

class GrammarError(YaccError):
    pass

class Grammar(object):
    def __init__(self, terminals):
        self.Productions  = [None]  # A list of all of the productions.  The first
                                    # entry is always reserved for the purpose of
                                    # building an augmented grammar

        self.Prodnames    = {}      # A dictionary mapping the names of nonterminals to a list of all
                                    # productions of that nonterminal.

        self.Prodmap      = {}      # A dictionary that is only used to detect duplicate
                                    # productions.

        self.Terminals    = {}      # A dictionary mapping the names of terminal symbols to a
                                    # list of the rules where they are used.

        for term in terminals:
            self.Terminals[term] = []

        self.Terminals['error'] = []

        self.Nonterminals = {}      # A dictionary mapping names of nonterminals to a list
                                    # of rule numbers where they are used.

        self.First        = {}      # A dictionary of precomputed FIRST(x) symbols

        self.Follow       = {}      # A dictionary of precomputed FOLLOW(x) symbols

        self.Precedence   = {}      # Precedence rules for each terminal. Contains tuples of the
                                    # form ('right',level) or ('nonassoc', level) or ('left',level)

        self.UsedPrecedence = set() # Precedence rules that were actually used by the grammer.
                                    # This is only used to provide error checking and to generate
                                    # a warning about unused precedence rules.

        self.Start = None           # Starting symbol for the grammar


    def __len__(self):
        return len(self.Productions)

    def __getitem__(self, index):
        return self.Productions[index]

    # -----------------------------------------------------------------------------
    # set_precedence()
    #
    # Sets the precedence for a given terminal. assoc is the associativity such as
    # 'left','right', or 'nonassoc'.  level is a numeric level.
    #
    # -----------------------------------------------------------------------------

    def set_precedence(self, term, assoc, level):
        assert self.Productions == [None], 'Must call set_precedence() before add_production()'
        if term in self.Precedence:
            raise GrammarError(f'Precedence already specified for terminal {term!r}')
        if assoc not in ['left', 'right', 'nonassoc']:
            raise GrammarError(f"Associativity of {term!r} must be one of 'left','right', or 'nonassoc'")
        self.Precedence[term] = (assoc, level)

    # -----------------------------------------------------------------------------
    # add_production()
    #
    # Given an action function, this function assembles a production rule and
    # computes its precedence level.
    #
    # The production rule is supplied as a list of symbols.   For example,
    # a rule such as 'expr : expr PLUS term' has a production name of 'expr' and
    # symbols ['expr','PLUS','term'].
    #
    # Precedence is determined by the precedence of the right-most non-terminal
    # or the precedence of a terminal specified by %prec.
    #
    # A variety of error checks are performed to make sure production symbols
    # are valid and that %prec is used correctly.
    # -----------------------------------------------------------------------------

    def add_production(self, prodname, syms, func=None, file='', line=0):

        if prodname in self.Terminals:
            raise GrammarError(f'{file}:{line}: Illegal rule name {prodname!r}. Already defined as a token')
        if prodname == 'error':
            raise GrammarError(f'{file}:{line}: Illegal rule name {prodname!r}. error is a reserved word')

        # Look for literal tokens
        for n, s in enumerate(syms):
            if s[0] in "'\"" and s[0] == s[-1]:
                c = s[1:-1]
                if (len(c) != 1):
                    raise GrammarError(f'{file}:{line}: Literal token {s} in rule {prodname!r} may only be a single character')
                if c not in self.Terminals:
                    self.Terminals[c] = []
                syms[n] = c
                continue

        # Determine the precedence level
        if '%prec' in syms:
            if syms[-1] == '%prec':
                raise GrammarError(f'{file}:{line}: Syntax error. Nothing follows %%prec')
            if syms[-2] != '%prec':
                raise GrammarError(f'{file}:{line}: Syntax error. %prec can only appear at the end of a grammar rule')
            precname = syms[-1]
            prodprec = self.Precedence.get(precname)
            if not prodprec:
                raise GrammarError(f'{file}:{line}: Nothing known about the precedence of {precname!r}')
            else:
                self.UsedPrecedence.add(precname)
            del syms[-2:]     # Drop %prec from the rule
        else:
            # If no %prec, precedence is determined by the rightmost terminal symbol
            precname = rightmost_terminal(syms, self.Terminals)
            prodprec = self.Precedence.get(precname, ('right', 0))

        # See if the rule is already in the rulemap
        map = '%s -> %s' % (prodname, syms)
        if map in self.Prodmap:
            m = self.Prodmap[map]
            raise GrammarError(f'{file}:{line}: Duplicate rule {m}. ' +
                               f'Previous definition at {m.file}:{m.line}')

        # From this point on, everything is valid.  Create a new Production instance
        pnumber  = len(self.Productions)
        if prodname not in self.Nonterminals:
            self.Nonterminals[prodname] = []

        # Add the production number to Terminals and Nonterminals
        for t in syms:
            if t in self.Terminals:
                self.Terminals[t].append(pnumber)
            else:
                if t not in self.Nonterminals:
                    self.Nonterminals[t] = []
                self.Nonterminals[t].append(pnumber)

        # Create a production and add it to the list of productions
        p = Production(pnumber, prodname, syms, prodprec, func, file, line)
        self.Productions.append(p)
        self.Prodmap[map] = p

        # Add to the global productions list
        try:
            self.Prodnames[prodname].append(p)
        except KeyError:
            self.Prodnames[prodname] = [p]

    # -----------------------------------------------------------------------------
    # set_start()
    #
    # Sets the starting symbol and creates the augmented grammar.  Production
    # rule 0 is S' -> start where start is the start symbol.
    # -----------------------------------------------------------------------------

    def set_start(self, start=None):
        if callable(start):
            start = start.__name__

        if not start:
            start = self.Productions[1].name

        if start not in self.Nonterminals:
            raise GrammarError(f'start symbol {start} undefined')
        self.Productions[0] = Production(0, "S'", [start])
        self.Nonterminals[start].append(0)
        self.Start = start

    # -----------------------------------------------------------------------------
    # find_unreachable()
    #
    # Find all of the nonterminal symbols that can't be reached from the starting
    # symbol.  Returns a list of nonterminals that can't be reached.
    # -----------------------------------------------------------------------------

    def find_unreachable(self):

        # Mark all symbols that are reachable from a symbol s
        def mark_reachable_from(s):
            if s in reachable:
                return
            reachable.add(s)
            for p in self.Prodnames.get(s, []):
                for r in p.prod:
                    mark_reachable_from(r)

        reachable = set()
        mark_reachable_from(self.Productions[0].prod[0])
        return [s for s in self.Nonterminals if s not in reachable]

    # -----------------------------------------------------------------------------
    # infinite_cycles()
    #
    # This function looks at the various parsing rules and tries to detect
    # infinite recursion cycles (grammar rules where there is no possible way
    # to derive a string of only terminals).
    # -----------------------------------------------------------------------------

    def infinite_cycles(self):
        terminates = {}

        # Terminals:
        for t in self.Terminals:
            terminates[t] = True

        terminates['$end'] = True

        # Nonterminals:

        # Initialize to false:
        for n in self.Nonterminals:
            terminates[n] = False

        # Then propagate termination until no change:
        while True:
            some_change = False
            for (n, pl) in self.Prodnames.items():
                # Nonterminal n terminates iff any of its productions terminates.
                for p in pl:
                    # Production p terminates iff all of its rhs symbols terminate.
                    for s in p.prod:
                        if not terminates[s]:
                            # The symbol s does not terminate,
                            # so production p does not terminate.
                            p_terminates = False
                            break
                    else:
                        # didn't break from the loop,
                        # so every symbol s terminates
                        # so production p terminates.
                        p_terminates = True

                    if p_terminates:
                        # symbol n terminates!
                        if not terminates[n]:
                            terminates[n] = True
                            some_change = True
                        # Don't need to consider any more productions for this n.
                        break

            if not some_change:
                break

        infinite = []
        for (s, term) in terminates.items():
            if not term:
                if s not in self.Prodnames and s not in self.Terminals and s != 'error':
                    # s is used-but-not-defined, and we've already warned of that,
                    # so it would be overkill to say that it's also non-terminating.
                    pass
                else:
                    infinite.append(s)

        return infinite

    # -----------------------------------------------------------------------------
    # undefined_symbols()
    #
    # Find all symbols that were used the grammar, but not defined as tokens or
    # grammar rules.  Returns a list of tuples (sym, prod) where sym in the symbol
    # and prod is the production where the symbol was used.
    # -----------------------------------------------------------------------------
    def undefined_symbols(self):
        result = []
        for p in self.Productions:
            if not p:
                continue

            for s in p.prod:
                if s not in self.Prodnames and s not in self.Terminals and s != 'error':
                    result.append((s, p))
        return result

    # -----------------------------------------------------------------------------
    # unused_terminals()
    #
    # Find all terminals that were defined, but not used by the grammar.  Returns
    # a list of all symbols.
    # -----------------------------------------------------------------------------
    def unused_terminals(self):
        unused_tok = []
        for s, v in self.Terminals.items():
            if s != 'error' and not v:
                unused_tok.append(s)

        return unused_tok

    # ------------------------------------------------------------------------------
    # unused_rules()
    #
    # Find all grammar rules that were defined,  but not used (maybe not reachable)
    # Returns a list of productions.
    # ------------------------------------------------------------------------------

    def unused_rules(self):
        unused_prod = []
        for s, v in self.Nonterminals.items():
            if not v:
                p = self.Prodnames[s][0]
                unused_prod.append(p)
        return unused_prod

    # -----------------------------------------------------------------------------
    # unused_precedence()
    #
    # Returns a list of tuples (term,precedence) corresponding to precedence
    # rules that were never used by the grammar.  term is the name of the terminal
    # on which precedence was applied and precedence is a string such as 'left' or
    # 'right' corresponding to the type of precedence.
    # -----------------------------------------------------------------------------

    def unused_precedence(self):
        unused = []
        for termname in self.Precedence:
            if not (termname in self.Terminals or termname in self.UsedPrecedence):
                unused.append((termname, self.Precedence[termname][0]))

        return unused

    # -------------------------------------------------------------------------
    # _first()
    #
    # Compute the value of FIRST1(beta) where beta is a tuple of symbols.
    #
    # During execution of compute_first1, the result may be incomplete.
    # Afterward (e.g., when called from compute_follow()), it will be complete.
    # -------------------------------------------------------------------------
    def _first(self, beta):

        # We are computing First(x1,x2,x3,...,xn)
        result = []
        for x in beta:
            x_produces_empty = False

            # Add all the non-<empty> symbols of First[x] to the result.
            for f in self.First[x]:
                if f == '<empty>':
                    x_produces_empty = True
                else:
                    if f not in result:
                        result.append(f)

            if x_produces_empty:
                # We have to consider the next x in beta,
                # i.e. stay in the loop.
                pass
            else:
                # We don't have to consider any further symbols in beta.
                break
        else:
            # There was no 'break' from the loop,
            # so x_produces_empty was true for all x in beta,
            # so beta produces empty as well.
            result.append('<empty>')

        return result

    # -------------------------------------------------------------------------
    # compute_first()
    #
    # Compute the value of FIRST1(X) for all symbols
    # -------------------------------------------------------------------------
    def compute_first(self):
        if self.First:
            return self.First

        # Terminals:
        for t in self.Terminals:
            self.First[t] = [t]

        self.First['$end'] = ['$end']

        # Nonterminals:

        # Initialize to the empty set:
        for n in self.Nonterminals:
            self.First[n] = []

        # Then propagate symbols until no change:
        while True:
            some_change = False
            for n in self.Nonterminals:
                for p in self.Prodnames[n]:
                    for f in self._first(p.prod):
                        if f not in self.First[n]:
                            self.First[n].append(f)
                            some_change = True
            if not some_change:
                break

        return self.First

    # ---------------------------------------------------------------------
    # compute_follow()
    #
    # Computes all of the follow sets for every non-terminal symbol.  The
    # follow set is the set of all symbols that might follow a given
    # non-terminal.  See the Dragon book, 2nd Ed. p. 189.
    # ---------------------------------------------------------------------
    def compute_follow(self, start=None):
        # If already computed, return the result
        if self.Follow:
            return self.Follow

        # If first sets not computed yet, do that first.
        if not self.First:
            self.compute_first()

        # Add '$end' to the follow list of the start symbol
        for k in self.Nonterminals:
            self.Follow[k] = []

        if not start:
            start = self.Productions[1].name

        self.Follow[start] = ['$end']

        while True:
            didadd = False
            for p in self.Productions[1:]:
                # Here is the production set
                for i, B in enumerate(p.prod):
                    if B in self.Nonterminals:
                        # Okay. We got a non-terminal in a production
                        fst = self._first(p.prod[i+1:])
                        hasempty = False
                        for f in fst:
                            if f != '<empty>' and f not in self.Follow[B]:
                                self.Follow[B].append(f)
                                didadd = True
                            if f == '<empty>':
                                hasempty = True
                        if hasempty or i == (len(p.prod)-1):
                            # Add elements of follow(a) to follow(b)
                            for f in self.Follow[p.name]:
                                if f not in self.Follow[B]:
                                    self.Follow[B].append(f)
                                    didadd = True
            if not didadd:
                break
        return self.Follow


    # -----------------------------------------------------------------------------
    # build_lritems()
    #
    # This function walks the list of productions and builds a complete set of the
    # LR items.  The LR items are stored in two ways:  First, they are uniquely
    # numbered and placed in the list _lritems.  Second, a linked list of LR items
    # is built for each production.  For example:
    #
    #   E -> E PLUS E
    #
    # Creates the list
    #
    #  [E -> . E PLUS E, E -> E . PLUS E, E -> E PLUS . E, E -> E PLUS E . ]
    # -----------------------------------------------------------------------------

    def build_lritems(self):
        for p in self.Productions:
            lastlri = p
            i = 0
            lr_items = []
            while True:
                if i > len(p):
                    lri = None
                else:
                    lri = LRItem(p, i)
                    # Precompute the list of productions immediately following
                    try:
                        lri.lr_after = self.Prodnames[lri.prod[i+1]]
                    except (IndexError, KeyError):
                        lri.lr_after = []
                    try:
                        lri.lr_before = lri.prod[i-1]
                    except IndexError:
                        lri.lr_before = None

                lastlri.lr_next = lri
                if not lri:
                    break
                lr_items.append(lri)
                lastlri = lri
                i += 1
            p.lr_items = lr_items


    # ----------------------------------------------------------------------
    # Debugging output.  Printing the grammar will produce a detailed
    # description along with some diagnostics.
    # ----------------------------------------------------------------------
    def __str__(self):
        out = []
        out.append('Grammar:\n')
        for n, p in enumerate(self.Productions):
            out.append(f'Rule {n:<5d} {p}')
        
        unused_terminals = self.unused_terminals()
        if unused_terminals:
            out.append('\nUnused terminals:\n')
            for term in unused_terminals:
                out.append(f'    {term}')

        out.append('\nTerminals, with rules where they appear:\n')
        for term in sorted(self.Terminals):
            out.append('%-20s : %s' % (term, ' '.join(str(s) for s in self.Terminals[term])))

        out.append('\nNonterminals, with rules where they appear:\n')
        for nonterm in sorted(self.Nonterminals):
            out.append('%-20s : %s' % (nonterm, ' '.join(str(s) for s in self.Nonterminals[nonterm])))

        out.append('')
        return '\n'.join(out)

# -----------------------------------------------------------------------------
#                           === LR Generator ===
#
# The following classes and functions are used to generate LR parsing tables on
# a grammar.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# digraph()
# traverse()
#
# The following two functions are used to compute set valued functions
# of the form:
#
#     F(x) = F'(x) U U{F(y) | x R y}
#
# This is used to compute the values of Read() sets as well as FOLLOW sets
# in LALR(1) generation.
#
# Inputs:  X    - An input set
#          R    - A relation
#          FP   - Set-valued function
# ------------------------------------------------------------------------------

def digraph(X, R, FP):
    N = {}
    for x in X:
        N[x] = 0
    stack = []
    F = {}
    for x in X:
        if N[x] == 0:
            traverse(x, N, stack, F, X, R, FP)
    return F

def traverse(x, N, stack, F, X, R, FP):
    stack.append(x)
    d = len(stack)
    N[x] = d
    F[x] = FP(x)             # F(X) <- F'(x)

    rel = R(x)               # Get y's related to x
    for y in rel:
        if N[y] == 0:
            traverse(y, N, stack, F, X, R, FP)
        N[x] = min(N[x], N[y])
        for a in F.get(y, []):
            if a not in F[x]:
                F[x].append(a)
    if N[x] == d:
        N[stack[-1]] = MAXINT
        F[stack[-1]] = F[x]
        element = stack.pop()
        while element != x:
            N[stack[-1]] = MAXINT
            F[stack[-1]] = F[x]
            element = stack.pop()

class LALRError(YaccError):
    pass

# -----------------------------------------------------------------------------
#                             == LRGeneratedTable ==
#
# This class implements the LR table generation algorithm.  There are no
# public methods except for write()
# -----------------------------------------------------------------------------

class LRTable(object):
    def __init__(self, grammar):
        self.grammar = grammar

        # Internal attributes
        self.lr_action     = {}        # Action table
        self.lr_goto       = {}        # Goto table
        self.lr_productions  = grammar.Productions    # Copy of grammar Production array
        self.lr_goto_cache = {}        # Cache of computed gotos
        self.lr0_cidhash   = {}        # Cache of closures
        self._add_count    = 0         # Internal counter used to detect cycles

        # Diagonistic information filled in by the table generator
        self.state_descriptions = OrderedDict()
        self.sr_conflict   = 0
        self.rr_conflict   = 0
        self.conflicts     = []        # List of conflicts

        self.sr_conflicts  = []
        self.rr_conflicts  = []

        # Build the tables
        self.grammar.build_lritems()
        self.grammar.compute_first()
        self.grammar.compute_follow()
        self.lr_parse_table()

        # Build default states
        # This identifies parser states where there is only one possible reduction action.
        # For such states, the parser can make a choose to make a rule reduction without consuming
        # the next look-ahead token.  This delayed invocation of the tokenizer can be useful in
        # certain kinds of advanced parsing situations where the lexer and parser interact with
        # each other or change states (i.e., manipulation of scope, lexer states, etc.).
        #
        # See:  http://www.gnu.org/software/bison/manual/html_node/Default-Reductions.html#Default-Reductions
        self.defaulted_states = {}
        for state, actions in self.lr_action.items():
            rules = list(actions.values())
            if len(rules) == 1 and rules[0] < 0:
                self.defaulted_states[state] = rules[0]

    # Compute the LR(0) closure operation on I, where I is a set of LR(0) items.
    def lr0_closure(self, I):
        self._add_count += 1

        # Add everything in I to J
        J = I[:]
        didadd = True
        while didadd:
            didadd = False
            for j in J:
                for x in j.lr_after:
                    if getattr(x, 'lr0_added', 0) == self._add_count:
                        continue
                    # Add B --> .G to J
                    J.append(x.lr_next)
                    x.lr0_added = self._add_count
                    didadd = True

        return J

    # Compute the LR(0) goto function goto(I,X) where I is a set
    # of LR(0) items and X is a grammar symbol.   This function is written
    # in a way that guarantees uniqueness of the generated goto sets
    # (i.e. the same goto set will never be returned as two different Python
    # objects).  With uniqueness, we can later do fast set comparisons using
    # id(obj) instead of element-wise comparison.

    def lr0_goto(self, I, x):
        # First we look for a previously cached entry
        g = self.lr_goto_cache.get((id(I), x))
        if g:
            return g

        # Now we generate the goto set in a way that guarantees uniqueness
        # of the result

        s = self.lr_goto_cache.get(x)
        if not s:
            s = {}
            self.lr_goto_cache[x] = s

        gs = []
        for p in I:
            n = p.lr_next
            if n and n.lr_before == x:
                s1 = s.get(id(n))
                if not s1:
                    s1 = {}
                    s[id(n)] = s1
                gs.append(n)
                s = s1
        g = s.get('$end')
        if not g:
            if gs:
                g = self.lr0_closure(gs)
                s['$end'] = g
            else:
                s['$end'] = gs
        self.lr_goto_cache[(id(I), x)] = g
        return g

    # Compute the LR(0) sets of item function
    def lr0_items(self):
        C = [self.lr0_closure([self.grammar.Productions[0].lr_next])]
        i = 0
        for I in C:
            self.lr0_cidhash[id(I)] = i
            i += 1

        # Loop over the items in C and each grammar symbols
        i = 0
        while i < len(C):
            I = C[i]
            i += 1

            # Collect all of the symbols that could possibly be in the goto(I,X) sets
            asyms = {}
            for ii in I:
                for s in ii.usyms:
                    asyms[s] = None

            for x in asyms:
                g = self.lr0_goto(I, x)
                if not g or id(g) in self.lr0_cidhash:
                    continue
                self.lr0_cidhash[id(g)] = len(C)
                C.append(g)

        return C

    # -----------------------------------------------------------------------------
    #                       ==== LALR(1) Parsing ====
    #
    # LALR(1) parsing is almost exactly the same as SLR except that instead of
    # relying upon Follow() sets when performing reductions, a more selective
    # lookahead set that incorporates the state of the LR(0) machine is utilized.
    # Thus, we mainly just have to focus on calculating the lookahead sets.
    #
    # The method used here is due to DeRemer and Pennelo (1982).
    #
    # DeRemer, F. L., and T. J. Pennelo: "Efficient Computation of LALR(1)
    #     Lookahead Sets", ACM Transactions on Programming Languages and Systems,
    #     Vol. 4, No. 4, Oct. 1982, pp. 615-649
    #
    # Further details can also be found in:
    #
    #  J. Tremblay and P. Sorenson, "The Theory and Practice of Compiler Writing",
    #      McGraw-Hill Book Company, (1985).
    #
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    # compute_nullable_nonterminals()
    #
    # Creates a dictionary containing all of the non-terminals that might produce
    # an empty production.
    # -----------------------------------------------------------------------------

    def compute_nullable_nonterminals(self):
        nullable = set()
        num_nullable = 0
        while True:
            for p in self.grammar.Productions[1:]:
                if p.len == 0:
                    nullable.add(p.name)
                    continue
                for t in p.prod:
                    if t not in nullable:
                        break
                else:
                    nullable.add(p.name)
            if len(nullable) == num_nullable:
                break
            num_nullable = len(nullable)
        return nullable

    # -----------------------------------------------------------------------------
    # find_nonterminal_trans(C)
    #
    # Given a set of LR(0) items, this functions finds all of the non-terminal
    # transitions.    These are transitions in which a dot appears immediately before
    # a non-terminal.   Returns a list of tuples of the form (state,N) where state
    # is the state number and N is the nonterminal symbol.
    #
    # The input C is the set of LR(0) items.
    # -----------------------------------------------------------------------------

    def find_nonterminal_transitions(self, C):
        trans = []
        for stateno, state in enumerate(C):
            for p in state:
                if p.lr_index < p.len - 1:
                    t = (stateno, p.prod[p.lr_index+1])
                    if t[1] in self.grammar.Nonterminals:
                        if t not in trans:
                            trans.append(t)
        return trans

    # -----------------------------------------------------------------------------
    # dr_relation()
    #
    # Computes the DR(p,A) relationships for non-terminal transitions.  The input
    # is a tuple (state,N) where state is a number and N is a nonterminal symbol.
    #
    # Returns a list of terminals.
    # -----------------------------------------------------------------------------

    def dr_relation(self, C, trans, nullable):
        dr_set = {}
        state, N = trans
        terms = []

        g = self.lr0_goto(C[state], N)
        for p in g:
            if p.lr_index < p.len - 1:
                a = p.prod[p.lr_index+1]
                if a in self.grammar.Terminals:
                    if a not in terms:
                        terms.append(a)

        # This extra bit is to handle the start state
        if state == 0 and N == self.grammar.Productions[0].prod[0]:
            terms.append('$end')

        return terms

    # -----------------------------------------------------------------------------
    # reads_relation()
    #
    # Computes the READS() relation (p,A) READS (t,C).
    # -----------------------------------------------------------------------------

    def reads_relation(self, C, trans, empty):
        # Look for empty transitions
        rel = []
        state, N = trans

        g = self.lr0_goto(C[state], N)
        j = self.lr0_cidhash.get(id(g), -1)
        for p in g:
            if p.lr_index < p.len - 1:
                a = p.prod[p.lr_index + 1]
                if a in empty:
                    rel.append((j, a))

        return rel

    # -----------------------------------------------------------------------------
    # compute_lookback_includes()
    #
    # Determines the lookback and includes relations
    #
    # LOOKBACK:
    #
    # This relation is determined by running the LR(0) state machine forward.
    # For example, starting with a production "N : . A B C", we run it forward
    # to obtain "N : A B C ."   We then build a relationship between this final
    # state and the starting state.   These relationships are stored in a dictionary
    # lookdict.
    #
    # INCLUDES:
    #
    # Computes the INCLUDE() relation (p,A) INCLUDES (p',B).
    #
    # This relation is used to determine non-terminal transitions that occur
    # inside of other non-terminal transition states.   (p,A) INCLUDES (p', B)
    # if the following holds:
    #
    #       B -> LAT, where T -> epsilon and p' -L-> p
    #
    # L is essentially a prefix (which may be empty), T is a suffix that must be
    # able to derive an empty string.  State p' must lead to state p with the string L.
    #
    # -----------------------------------------------------------------------------

    def compute_lookback_includes(self, C, trans, nullable):
        lookdict = {}          # Dictionary of lookback relations
        includedict = {}       # Dictionary of include relations

        # Make a dictionary of non-terminal transitions
        dtrans = {}
        for t in trans:
            dtrans[t] = 1

        # Loop over all transitions and compute lookbacks and includes
        for state, N in trans:
            lookb = []
            includes = []
            for p in C[state]:
                if p.name != N:
                    continue

                # Okay, we have a name match.  We now follow the production all the way
                # through the state machine until we get the . on the right hand side

                lr_index = p.lr_index
                j = state
                while lr_index < p.len - 1:
                    lr_index = lr_index + 1
                    t = p.prod[lr_index]

                    # Check to see if this symbol and state are a non-terminal transition
                    if (j, t) in dtrans:
                        # Yes.  Okay, there is some chance that this is an includes relation
                        # the only way to know for certain is whether the rest of the
                        # production derives empty

                        li = lr_index + 1
                        while li < p.len:
                            if p.prod[li] in self.grammar.Terminals:
                                break      # No forget it
                            if p.prod[li] not in nullable:
                                break
                            li = li + 1
                        else:
                            # Appears to be a relation between (j,t) and (state,N)
                            includes.append((j, t))

                    g = self.lr0_goto(C[j], t)               # Go to next set
                    j = self.lr0_cidhash.get(id(g), -1)      # Go to next state

                # When we get here, j is the final state, now we have to locate the production
                for r in C[j]:
                    if r.name != p.name:
                        continue
                    if r.len != p.len:
                        continue
                    i = 0
                    # This look is comparing a production ". A B C" with "A B C ."
                    while i < r.lr_index:
                        if r.prod[i] != p.prod[i+1]:
                            break
                        i = i + 1
                    else:
                        lookb.append((j, r))
            for i in includes:
                if i not in includedict:
                    includedict[i] = []
                includedict[i].append((state, N))
            lookdict[(state, N)] = lookb

        return lookdict, includedict

    # -----------------------------------------------------------------------------
    # compute_read_sets()
    #
    # Given a set of LR(0) items, this function computes the read sets.
    #
    # Inputs:  C        =  Set of LR(0) items
    #          ntrans   = Set of nonterminal transitions
    #          nullable = Set of empty transitions
    #
    # Returns a set containing the read sets
    # -----------------------------------------------------------------------------

    def compute_read_sets(self, C, ntrans, nullable):
        FP = lambda x: self.dr_relation(C, x, nullable)
        R =  lambda x: self.reads_relation(C, x, nullable)
        F = digraph(ntrans, R, FP)
        return F

    # -----------------------------------------------------------------------------
    # compute_follow_sets()
    #
    # Given a set of LR(0) items, a set of non-terminal transitions, a readset,
    # and an include set, this function computes the follow sets
    #
    # Follow(p,A) = Read(p,A) U U {Follow(p',B) | (p,A) INCLUDES (p',B)}
    #
    # Inputs:
    #            ntrans     = Set of nonterminal transitions
    #            readsets   = Readset (previously computed)
    #            inclsets   = Include sets (previously computed)
    #
    # Returns a set containing the follow sets
    # -----------------------------------------------------------------------------

    def compute_follow_sets(self, ntrans, readsets, inclsets):
        FP = lambda x: readsets[x]
        R  = lambda x: inclsets.get(x, [])
        F = digraph(ntrans, R, FP)
        return F

    # -----------------------------------------------------------------------------
    # add_lookaheads()
    #
    # Attaches the lookahead symbols to grammar rules.
    #
    # Inputs:    lookbacks         -  Set of lookback relations
    #            followset         -  Computed follow set
    #
    # This function directly attaches the lookaheads to productions contained
    # in the lookbacks set
    # -----------------------------------------------------------------------------

    def add_lookaheads(self, lookbacks, followset):
        for trans, lb in lookbacks.items():
            # Loop over productions in lookback
            for state, p in lb:
                if state not in p.lookaheads:
                    p.lookaheads[state] = set()

                f = followset.get(trans, [])
                for a in f:
                    p.lookaheads[state].add(a)

    # -----------------------------------------------------------------------------
    # add_lalr_lookaheads()
    #
    # This function does all of the work of adding lookahead information for use
    # with LALR parsing
    # -----------------------------------------------------------------------------

    def add_lalr_lookaheads(self, C):
        # Determine all of the nullable nonterminals
        nullable = self.compute_nullable_nonterminals()

        # Find all non-terminal transitions
        trans = self.find_nonterminal_transitions(C)

        # Compute read sets
        readsets = self.compute_read_sets(C, trans, nullable)

        # Compute lookback/includes relations
        lookd, included = self.compute_lookback_includes(C, trans, nullable)

        # Compute LALR FOLLOW sets
        followsets = self.compute_follow_sets(trans, readsets, included)

        # Add all of the lookaheads
        self.add_lookaheads(lookd, followsets)

    # -----------------------------------------------------------------------------
    # lr_parse_table()
    #
    # This function constructs the final LALR parse table.  Touch this code and die.
    # -----------------------------------------------------------------------------
    def lr_parse_table(self):
        Productions = self.grammar.Productions
        Precedence  = self.grammar.Precedence
        goto   = self.lr_goto         # Goto array
        action = self.lr_action       # Action array

        actionp = {}                  # Action production array (temporary)

        # Step 1: Construct C = { I0, I1, ... IN}, collection of LR(0) items
        # This determines the number of states

        C = self.lr0_items()
        self.add_lalr_lookaheads(C)

        # Build the parser table, state by state
        for st, I in enumerate(C):
            descrip = []
            # Loop over each production in I
            actlist = []              # List of actions
            st_action  = {}
            st_actionp = {}
            st_goto    = {}

            descrip.append(f'\nstate {st}\n')
            for p in I:
                descrip.append(f'    ({p.number}) {p}')

            for p in I:
                    if p.len == p.lr_index + 1:
                        if p.name == "S'":
                            # Start symbol. Accept!
                            st_action['$end'] = 0
                            st_actionp['$end'] = p
                        else:
                            # We are at the end of a production.  Reduce!
                            laheads = p.lookaheads[st]
                            for a in laheads:
                                actlist.append((a, p, f'reduce using rule {p.number} ({p})'))
                                r = st_action.get(a)
                                if r is not None:
                                    # Have a shift/reduce or reduce/reduce conflict
                                    if r > 0:
                                        # Need to decide on shift or reduce here
                                        # By default we favor shifting. Need to add
                                        # some precedence rules here.

                                        # Shift precedence comes from the token
                                        sprec, slevel = Precedence.get(a, ('right', 0))

                                        # Reduce precedence comes from rule being reduced (p)
                                        rprec, rlevel = Productions[p.number].prec

                                        if (slevel < rlevel) or ((slevel == rlevel) and (rprec == 'left')):
                                            # We really need to reduce here.
                                            st_action[a] = -p.number
                                            st_actionp[a] = p
                                            if not slevel and not rlevel:
                                                descrip.append(f'  ! shift/reduce conflict for {a} resolved as reduce')
                                                self.sr_conflicts.append((st, a, 'reduce'))
                                            Productions[p.number].reduced += 1
                                        elif (slevel == rlevel) and (rprec == 'nonassoc'):
                                            st_action[a] = None
                                        else:
                                            # Hmmm. Guess we'll keep the shift
                                            if not rlevel:
                                                descrip.append(f'  ! shift/reduce conflict for {a} resolved as shift')
                                                self.sr_conflicts.append((st, a, 'shift'))
                                    elif r <= 0:
                                        # Reduce/reduce conflict.   In this case, we favor the rule
                                        # that was defined first in the grammar file
                                        oldp = Productions[-r]
                                        pp = Productions[p.number]
                                        if oldp.line > pp.line:
                                            st_action[a] = -p.number
                                            st_actionp[a] = p
                                            chosenp, rejectp = pp, oldp
                                            Productions[p.number].reduced += 1
                                            Productions[oldp.number].reduced -= 1
                                        else:
                                            chosenp, rejectp = oldp, pp
                                        self.rr_conflicts.append((st, chosenp, rejectp))
                                        descrip.append('  ! reduce/reduce conflict for %s resolved using rule %d (%s)' % 
                                                       (a, st_actionp[a].number, st_actionp[a]))
                                    else:
                                        raise LALRError(f'Unknown conflict in state {st}')
                                else:
                                    st_action[a] = -p.number
                                    st_actionp[a] = p
                                    Productions[p.number].reduced += 1
                    else:
                        i = p.lr_index
                        a = p.prod[i+1]       # Get symbol right after the "."
                        if a in self.grammar.Terminals:
                            g = self.lr0_goto(I, a)
                            j = self.lr0_cidhash.get(id(g), -1)
                            if j >= 0:
                                # We are in a shift state
                                actlist.append((a, p, f'shift and go to state {j}'))
                                r = st_action.get(a)
                                if r is not None:
                                    # Whoa have a shift/reduce or shift/shift conflict
                                    if r > 0:
                                        if r != j:
                                            raise LALRError(f'Shift/shift conflict in state {st}')
                                    elif r <= 0:
                                        # Do a precedence check.
                                        #   -  if precedence of reduce rule is higher, we reduce.
                                        #   -  if precedence of reduce is same and left assoc, we reduce.
                                        #   -  otherwise we shift
                                        rprec, rlevel = Productions[st_actionp[a].number].prec
                                        sprec, slevel = Precedence.get(a, ('right', 0))
                                        if (slevel > rlevel) or ((slevel == rlevel) and (rprec == 'right')):
                                            # We decide to shift here... highest precedence to shift
                                            Productions[st_actionp[a].number].reduced -= 1
                                            st_action[a] = j
                                            st_actionp[a] = p
                                            if not rlevel:
                                                descrip.append(f'  ! shift/reduce conflict for {a} resolved as shift')
                                                self.sr_conflicts.append((st, a, 'shift'))
                                        elif (slevel == rlevel) and (rprec == 'nonassoc'):
                                            st_action[a] = None
                                        else:
                                            # Hmmm. Guess we'll keep the reduce
                                            if not slevel and not rlevel:
                                                descrip.append(f'  ! shift/reduce conflict for {a} resolved as reduce')
                                                self.sr_conflicts.append((st, a, 'reduce'))

                                    else:
                                        raise LALRError(f'Unknown conflict in state {st}')
                                else:
                                    st_action[a] = j
                                    st_actionp[a] = p

            # Print the actions associated with each terminal
            _actprint = {}
            for a, p, m in actlist:
                if a in st_action:
                    if p is st_actionp[a]:
                        descrip.append(f'    {a:<15s} {m}')
                        _actprint[(a, m)] = 1
            descrip.append('')

            # Construct the goto table for this state
            nkeys = {}
            for ii in I:
                for s in ii.usyms:
                    if s in self.grammar.Nonterminals:
                        nkeys[s] = None
            for n in nkeys:
                g = self.lr0_goto(I, n)
                j = self.lr0_cidhash.get(id(g), -1)
                if j >= 0:
                    st_goto[n] = j
                    descrip.append(f'    {n:<30s} shift and go to state {j}')

            action[st] = st_action
            actionp[st] = st_actionp
            goto[st] = st_goto
            self.state_descriptions[st] = '\n'.join(descrip)

    # ----------------------------------------------------------------------
    # Debugging output.   Printing the LRTable object will produce a listing
    # of all of the states, conflicts, and other details.
    # ----------------------------------------------------------------------
    def __str__(self):
        out = []
        for descrip in self.state_descriptions.values():
            out.append(descrip)
            
        if self.sr_conflicts or self.rr_conflicts:
            out.append('\nConflicts:\n')

            for state, tok, resolution in self.sr_conflicts:
                out.append(f'shift/reduce conflict for {tok} in state {state} resolved as {resolution}')

            already_reported = set()
            for state, rule, rejected in self.rr_conflicts:
                if (state, id(rule), id(rejected)) in already_reported:
                    continue
                out.append(f'reduce/reduce conflict in state {state} resolved using rule {rule}')
                out.append(f'rejected rule ({rejected}) in state {state}')
                already_reported.add((state, id(rule), id(rejected)))

            warned_never = set()
            for state, rule, rejected in self.rr_conflicts:
                if not rejected.reduced and (rejected not in warned_never):
                    out.append(f'Rule ({rejected}) is never reduced')
                    warned_never.add(rejected)

        return '\n'.join(out)

# Collect grammar rules from a function
def _collect_grammar_rules(func):
    grammar = []
    while func:
        prodname = func.__name__
        unwrapped = inspect.unwrap(func)
        filename = unwrapped.__code__.co_filename
        lineno = unwrapped.__code__.co_firstlineno
        for rule, lineno in zip(func.rules, range(lineno+len(func.rules)-1, 0, -1)):
            syms = rule.split()
            ebnf_prod = []
            while ('{' in syms) or ('[' in syms):
                for s in syms:
                    if s == '[':
                        syms, prod = _replace_ebnf_optional(syms)
                        ebnf_prod.extend(prod)
                        break
                    elif s == '{':
                        syms, prod = _replace_ebnf_repeat(syms)
                        ebnf_prod.extend(prod)
                        break
                    elif '|' in s:
                        syms, prod = _replace_ebnf_choice(syms)
                        ebnf_prod.extend(prod)
                        break

            if syms[1:2] == [':'] or syms[1:2] == ['::=']:
                grammar.append((func, filename, lineno, syms[0], syms[2:]))
            else:
                grammar.append((func, filename, lineno, prodname, syms))
            grammar.extend(ebnf_prod)
            
        func = getattr(func, 'next_func', None)

    return grammar

# Replace EBNF repetition
def _replace_ebnf_repeat(syms):
    syms = list(syms)
    first = syms.index('{')
    end = syms.index('}', first)

    # Look for choices inside
    repeated_syms = syms[first+1:end]
    if any('|' in sym for sym in repeated_syms):
        repeated_syms, prods = _replace_ebnf_choice(repeated_syms)
    else:
        prods = []

    symname, moreprods = _generate_repeat_rules(repeated_syms)
    syms[first:end+1] = [symname]
    return syms, prods + moreprods

def _replace_ebnf_optional(syms):
    syms = list(syms)
    first = syms.index('[')
    end = syms.index(']', first)
    symname, prods = _generate_optional_rules(syms[first+1:end])
    syms[first:end+1] = [symname]
    return syms, prods

def _replace_ebnf_choice(syms):
    syms = list(syms)
    newprods = [ ]
    n = 0
    while n < len(syms):
        if '|' in syms[n]:
            symname, prods = _generate_choice_rules(syms[n].split('|'))
            syms[n] = symname
            newprods.extend(prods)
        n += 1
    return syms, newprods
    
# Generate grammar rules for repeated items
_gencount = 0

# Dictionary mapping name aliases generated by EBNF rules.

_name_aliases = { }

def _sanitize_symbols(symbols):
    for sym in symbols:
        if sym.startswith("'"):
            yield str(hex(ord(sym[1])))
        elif sym.isidentifier():
            yield sym
        else:
            yield sym.encode('utf-8').hex()
            
def _generate_repeat_rules(symbols):
    '''
    Symbols is a list of grammar symbols [ symbols ]. This
    generates code corresponding to these grammar construction:
  
       @('repeat : many')
       def repeat(self, p):
           return p.many

       @('repeat :')
       def repeat(self, p):
           return []

       @('many : many symbols')
       def many(self, p):
           p.many.append(symbols)
           return p.many

       @('many : symbols')
       def many(self, p):
           return [ p.symbols ]
    '''
    global _gencount
    _gencount += 1
    basename = f'_{_gencount}_' + '_'.join(_sanitize_symbols(symbols))
    name = f'{basename}_repeat'
    oname = f'{basename}_items'
    iname = f'{basename}_item'
    symtext = ' '.join(symbols)

    _name_aliases[name] = symbols

    productions = [ ]
    _ = _decorator

    @_(f'{name} : {oname}')
    def repeat(self, p):
        return getattr(p, oname)

    @_(f'{name} : ')
    def repeat2(self, p):
        return []
    productions.extend(_collect_grammar_rules(repeat))
    productions.extend(_collect_grammar_rules(repeat2))

    @_(f'{oname} : {oname} {iname}')
    def many(self, p):
        items = getattr(p, oname)
        items.append(getattr(p, iname))
        return items

    @_(f'{oname} : {iname}')
    def many2(self, p):
        return [ getattr(p, iname) ]

    productions.extend(_collect_grammar_rules(many))
    productions.extend(_collect_grammar_rules(many2))

    @_(f'{iname} : {symtext}')
    def item(self, p):
        return tuple(p)

    productions.extend(_collect_grammar_rules(item))
    return name, productions

def _generate_optional_rules(symbols):
    '''
    Symbols is a list of grammar symbols [ symbols ]. This
    generates code corresponding to these grammar construction:
  
       @('optional : symbols')
       def optional(self, p):
           return p.symbols

       @('optional :')
       def optional(self, p):
           return None
    '''
    global _gencount
    _gencount += 1
    basename = f'_{_gencount}_' + '_'.join(_sanitize_symbols(symbols))
    name = f'{basename}_optional'
    symtext = ' '.join(symbols)
    
    _name_aliases[name] = symbols

    productions = [ ]
    _ = _decorator

    no_values = (None,) * len(symbols)

    @_(f'{name} : {symtext}')
    def optional(self, p):
        return tuple(p)

    @_(f'{name} : ')
    def optional2(self, p):
        return no_values

    productions.extend(_collect_grammar_rules(optional))
    productions.extend(_collect_grammar_rules(optional2))
    return name, productions

def _generate_choice_rules(symbols):
    '''
    Symbols is a list of grammar symbols such as [ 'PLUS', 'MINUS' ].
    This generates code corresponding to the following construction:
    
    @('PLUS', 'MINUS')
    def choice(self, p):
        return p[0]
    '''
    global _gencount
    _gencount += 1
    basename = f'_{_gencount}_' + '_'.join(_sanitize_symbols(symbols))
    name = f'{basename}_choice'

    _ = _decorator
    productions = [ ]


    def choice(self, p):
        return p[0]
    choice.__name__ = name
    choice = _(*symbols)(choice)
    productions.extend(_collect_grammar_rules(choice))
    return name, productions
    
class ParserMetaDict(dict):
    '''
    Dictionary that allows decorated grammar rule functions to be overloaded
    '''
    def __setitem__(self, key, value):
        if key in self and callable(value) and hasattr(value, 'rules'):
            value.next_func = self[key]
            if not hasattr(value.next_func, 'rules'):
                raise GrammarError(f'Redefinition of {key}. Perhaps an earlier {key} is missing @_')
        super().__setitem__(key, value)
    
    def __getitem__(self, key):
        if key not in self and key.isupper() and key[:1] != '_':
            return key.upper()
        else:
            return super().__getitem__(key)

def _decorator(rule, *extra):
     rules = [rule, *extra]
     def decorate(func):
         func.rules = [ *getattr(func, 'rules', []), *rules[::-1] ]
         return func
     return decorate

class ParserMeta(type):
    @classmethod
    def __prepare__(meta, *args, **kwargs):
        d = ParserMetaDict()
        d['_'] = _decorator
        return d

    def __new__(meta, clsname, bases, attributes):
        del attributes['_']
        cls = super().__new__(meta, clsname, bases, attributes)
        cls._build(list(attributes.items()))
        return cls

class Parser(metaclass=ParserMeta):
    # Automatic tracking of position information
    track_positions = True
    
    # Logging object where debugging/diagnostic messages are sent
    log = SlyLogger(sys.stderr)     

    # Debugging filename where parsetab.out data can be written
    debugfile = None

    @classmethod
    def __validate_tokens(cls):
        if not hasattr(cls, 'tokens'):
            cls.log.error('No token list is defined')
            return False

        if not cls.tokens:
            cls.log.error('tokens is empty')
            return False

        if 'error' in cls.tokens:
            cls.log.error("Illegal token name 'error'. Is a reserved word")
            return False

        return True

    @classmethod
    def __validate_precedence(cls):
        if not hasattr(cls, 'precedence'):
            cls.__preclist = []
            return True

        preclist = []
        if not isinstance(cls.precedence, (list, tuple)):
            cls.log.error('precedence must be a list or tuple')
            return False

        for level, p in enumerate(cls.precedence, start=1):
            if not isinstance(p, (list, tuple)):
                cls.log.error(f'Bad precedence table entry {p!r}. Must be a list or tuple')
                return False

            if len(p) < 2:
                cls.log.error(f'Malformed precedence entry {p!r}. Must be (assoc, term, ..., term)')
                return False

            if not all(isinstance(term, str) for term in p):
                cls.log.error('precedence items must be strings')
                return False
            
            assoc = p[0]
            preclist.extend((term, assoc, level) for term in p[1:])

        cls.__preclist = preclist
        return True

    @classmethod
    def __validate_specification(cls):
        '''
        Validate various parts of the grammar specification
        '''
        if not cls.__validate_tokens():
            return False
        if not cls.__validate_precedence():
            return False
        return True

    @classmethod
    def __build_grammar(cls, rules):
        '''
        Build the grammar from the grammar rules
        '''
        grammar_rules = []
        errors = ''
        # Check for non-empty symbols
        if not rules:
            raise YaccError('No grammar rules are defined')

        grammar = Grammar(cls.tokens)

        # Set the precedence level for terminals
        for term, assoc, level in cls.__preclist:
            try:
                grammar.set_precedence(term, assoc, level)
            except GrammarError as e:
                errors += f'{e}\n'

        for name, func in rules:
            try:
                parsed_rule = _collect_grammar_rules(func)
                for pfunc, rulefile, ruleline, prodname, syms in parsed_rule:
                    try:
                        grammar.add_production(prodname, syms, pfunc, rulefile, ruleline)
                    except GrammarError as e:
                        errors += f'{e}\n'
            except SyntaxError as e:
                errors += f'{e}\n'
        try:
            grammar.set_start(getattr(cls, 'start', None))
        except GrammarError as e:
            errors += f'{e}\n'

        undefined_symbols = grammar.undefined_symbols()
        for sym, prod in undefined_symbols:
            errors += '%s:%d: Symbol %r used, but not defined as a token or a rule\n' % (prod.file, prod.line, sym)

        unused_terminals = grammar.unused_terminals()
        if unused_terminals:
            unused_str = '{' + ','.join(unused_terminals) + '}'
            cls.log.warning(f'Token{"(s)" if len(unused_terminals) >1 else ""} {unused_str} defined, but not used')

        unused_rules = grammar.unused_rules()
        for prod in unused_rules:
            cls.log.warning('%s:%d: Rule %r defined, but not used', prod.file, prod.line, prod.name)

        if len(unused_terminals) == 1:
            cls.log.warning('There is 1 unused token')
        if len(unused_terminals) > 1:
            cls.log.warning('There are %d unused tokens', len(unused_terminals))

        if len(unused_rules) == 1:
            cls.log.warning('There is 1 unused rule')
        if len(unused_rules) > 1:
            cls.log.warning('There are %d unused rules', len(unused_rules))

        unreachable = grammar.find_unreachable()
        for u in unreachable:
           cls.log.warning('Symbol %r is unreachable', u)

        if len(undefined_symbols) == 0:
            infinite = grammar.infinite_cycles()
            for inf in infinite:
                errors += 'Infinite recursion detected for symbol %r\n' % inf

        unused_prec = grammar.unused_precedence()
        for term, assoc in unused_prec:
            errors += 'Precedence rule %r defined for unknown symbol %r\n' % (assoc, term)

        cls._grammar = grammar
        if errors:
            raise YaccError('Unable to build grammar.\n'+errors)

    @classmethod
    def __build_lrtables(cls):
        '''
        Build the LR Parsing tables from the grammar
        '''
        lrtable = LRTable(cls._grammar)
        num_sr = len(lrtable.sr_conflicts)

        # Report shift/reduce and reduce/reduce conflicts
        if num_sr != getattr(cls, 'expected_shift_reduce', None):
            if num_sr == 1:
                cls.log.warning('1 shift/reduce conflict')
            elif num_sr > 1:
                cls.log.warning('%d shift/reduce conflicts', num_sr)

        num_rr = len(lrtable.rr_conflicts)
        if num_rr != getattr(cls, 'expected_reduce_reduce', None):
            if num_rr == 1:
                cls.log.warning('1 reduce/reduce conflict')
            elif num_rr > 1:
                cls.log.warning('%d reduce/reduce conflicts', num_rr)

        cls._lrtable = lrtable
        return True

    @classmethod
    def __collect_rules(cls, definitions):
        '''
        Collect all of the tagged grammar rules
        '''
        rules = [ (name, value) for name, value in definitions
                  if callable(value) and hasattr(value, 'rules') ]
        return rules

    # ----------------------------------------------------------------------
    # Build the LALR(1) tables. definitions is a list of (name, item) tuples
    # of all definitions provided in the class, listed in the order in which
    # they were defined.  This method is triggered by a metaclass.
    # ----------------------------------------------------------------------
    @classmethod
    def _build(cls, definitions):
        if vars(cls).get('_build', False):
            return

        # Collect all of the grammar rules from the class definition
        rules = cls.__collect_rules(definitions)

        # Validate other parts of the grammar specification
        if not cls.__validate_specification():
            raise YaccError('Invalid parser specification')

        # Build the underlying grammar object
        cls.__build_grammar(rules)

        # Build the LR tables
        if not cls.__build_lrtables():
            raise YaccError('Can\'t build parsing tables')

        if cls.debugfile:
            with open(cls.debugfile, 'w') as f:
                f.write(str(cls._grammar))
                f.write('\n')
                f.write(str(cls._lrtable))
            cls.log.info('Parser debugging for %s written to %s', cls.__qualname__, cls.debugfile)

    # ----------------------------------------------------------------------
    # Parsing Support.  This is the parsing runtime that users use to
    # ----------------------------------------------------------------------
    def error(self, token, expected_tokens=None):
        '''
        Default error handling function.  This may be subclassed.
        '''
        if token:
            lineno = getattr(token, 'lineno', 0)
            if lineno:
                sys.stderr.write(f'sly: Syntax error at line {lineno}, token={token.type}\n')
            else:
                sys.stderr.write(f'sly: Syntax error, token={token.type}')
        else:
            sys.stderr.write('sly: Parse error in input. EOF\n')
 
    def errok(self):
        '''
        Clear the error status
        '''
        self.errorok = True

    def restart(self):
        '''
        Force the parser to restart from a fresh state. Clears the statestack
        '''
        del self.statestack[:]
        del self.symstack[:]
        sym = YaccSymbol()
        sym.type = '$end'
        self.symstack.append(sym)
        self.statestack.append(0)
        self.state = 0

    def parse(self, tokens):
        '''
        Parse the given input tokens.
        '''
        lookahead = None                                  # Current lookahead symbol
        lookaheadstack = []                               # Stack of lookahead symbols
        actions = self._lrtable.lr_action                 # Local reference to action table (to avoid lookup on self.)
        goto    = self._lrtable.lr_goto                   # Local reference to goto table (to avoid lookup on self.)
        prod    = self._grammar.Productions               # Local reference to production list (to avoid lookup on self.)
        defaulted_states = self._lrtable.defaulted_states # Local reference to defaulted states
        pslice  = YaccProduction(None)                    # Production object passed to grammar rules
        errorcount = 0                                    # Used during error recovery

        # Set up the state and symbol stacks
        self.tokens = tokens
        self.used_tokens = []
        self.statestack = statestack = []                 # Stack of parsing states
        self.symstack = symstack = []                     # Stack of grammar symbols
        pslice._stack = symstack                          # Associate the stack with the production
        self.restart()

        # Set up position tracking
        track_positions = self.track_positions
        if not hasattr(self, '_line_positions'):
            self._line_positions = { }           # id: -> lineno
            self._index_positions = { }          # id: -> (start, end)

        errtoken   = None                                 # Err token
        while True:
            # Get the next symbol on the input.  If a lookahead symbol
            # is already set, we just use that. Otherwise, we'll pull
            # the next token off of the lookaheadstack or from the lexer
            if self.state not in defaulted_states:
                if not lookahead:
                    if not lookaheadstack:
                        lookahead = next(tokens, None)  # Get the next token
                        self.used_tokens.append(lookahead)
                    else:
                        lookahead = lookaheadstack.pop()
                    if not lookahead:
                        lookahead = YaccSymbol()
                        lookahead.type = '$end'
                    
                # Check the action table
                ltype = lookahead.type
                t = actions[self.state].get(ltype)
            else:
                t = defaulted_states[self.state]

            if t is not None:
                if t > 0:
                    # shift a symbol on the stack
                    statestack.append(t)
                    self.state = t

                    symstack.append(lookahead)
                    lookahead = None

                    # Decrease error count on successful shift
                    if errorcount:
                        errorcount -= 1
                    continue

                if t < 0:
                    # reduce a symbol on the stack, emit a production
                    self.production = p = prod[-t]
                    pname = p.name
                    plen  = p.len
                    pslice._namemap = p.namemap

                    # Call the production function
                    pslice._slice = symstack[-plen:] if plen else []

                    sym = YaccSymbol()
                    sym.type = pname       
                    value = p.func(self, pslice)
                    if value is pslice:
                        value = (pname, *(s.value for s in pslice._slice))

                    sym.value = value
                        
                    # Record positions
                    if track_positions:
                        if plen:
                            sym.lineno = symstack[-plen].lineno
                            sym.index = symstack[-plen].index
                            sym.end = symstack[-1].end
                        else:
                            # A zero-length production  (what to put here?)
                            sym.lineno = None
                            sym.index = None
                            sym.end = None
                        self._line_positions[id(value)] = sym.lineno
                        self._index_positions[id(value)] = (sym.index, sym.end)
                            
                    if plen:
                        del symstack[-plen:]
                        del statestack[-plen:]

                    symstack.append(sym)
                    self.state = goto[statestack[-1]][pname]
                    statestack.append(self.state)
                    continue

                if t == 0:
                    n = symstack[-1]
                    result = getattr(n, 'value', None)
                    return result

            if t is None:
                # We have some kind of parsing error here.  To handle
                # this, we are going to push the current token onto
                # the tokenstack and replace it with an 'error' token.
                # If there are any synchronization rules, they may
                # catch it.
                #
                # In addition to pushing the error token, we call call
                # the user defined error() function if this is the
                # first syntax error.  This function is only called if
                # errorcount == 0.
                if errorcount == 0 or self.errorok:
                    errorcount = ERROR_COUNT
                    self.errorok = False
                    if lookahead.type == '$end':
                        errtoken = None               # End of file!
                    else:
                        errtoken = lookahead

                    tok = self.error(errtoken, expected_tokens=list(actions[self.state].keys()))
                    if tok:
                        # User must have done some kind of panic
                        # mode recovery on their own.  The
                        # returned token is the next lookahead
                        lookahead = tok
                        self.errorok = True
                        continue
                    else:
                        # If at EOF. We just return. Basically dead.
                        if not errtoken:
                            return
                else:
                    # Reset the error count.  Unsuccessful token shifted
                    errorcount = ERROR_COUNT

                # case 1:  the statestack only has 1 entry on it.  If we're in this state, the
                # entire parse has been rolled back and we're completely hosed.   The token is
                # discarded and we just keep going.

                if len(statestack) <= 1 and lookahead.type != '$end':
                    lookahead = None
                    self.state = 0
                    # Nuke the lookahead stack
                    del lookaheadstack[:]
                    continue

                # case 2: the statestack has a couple of entries on it, but we're
                # at the end of the file. nuke the top entry and generate an error token

                # Start nuking entries on the stack
                if lookahead.type == '$end':
                    # Whoa. We're really hosed here. Bail out
                    return

                if lookahead.type != 'error':
                    sym = symstack[-1]
                    if sym.type == 'error':
                        # Hmmm. Error is on top of stack, we'll just nuke input
                        # symbol and continue
                        lookahead = None
                        continue

                    # Create the error symbol for the first time and make it the new lookahead symbol
                    t = YaccSymbol()
                    t.type = 'error'

                    if hasattr(lookahead, 'lineno'):
                        t.lineno = lookahead.lineno
                    if hasattr(lookahead, 'index'):
                        t.index = lookahead.index
                    if hasattr(lookahead, 'end'):
                        t.end = lookahead.end
                    t.value = lookahead
                    lookaheadstack.append(lookahead)
                    lookahead = t
                else:
                    sym = symstack.pop()
                    statestack.pop()
                    self.state = statestack[-1]
                continue

            # Call an error function here
            raise RuntimeError('sly: internal parser error!!!\n')

    # Return position tracking information
    def line_position(self, value):
        return self._line_positions[id(value)]

    def index_position(self, value):
        return self._index_positions[id(value)]
    
