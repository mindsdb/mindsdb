from dateinfer.date_elements import Filler


class If(object):
    """
    Top-level rule
    """
    def __init__(self, condition, action):
        """
        Initialize the rule with a condition clause and an action clause that will be executed
        if condition clause is true.
        """
        self.condition = condition
        self.action = action

    def execute(self, elem_list):
        """
        If condition, return a new elem_list provided by executing action.
        """
        if self.condition.is_true(elem_list):
            return self.action.act(elem_list)
        else:
            return elem_list


class ConditionClause(object):
    """
    Abstract class for a condition clause
    """

    def is_true(self, elem_list):
        """
        Return true if condition is true for the given input.
        """
        raise NotImplementedError()


class ActionClause(object):
    """
    Abstract class for an action clause
    """

    def act(self, elem_list):
        """
        Return a new instance of elem_list permuted by the action
        """
        raise NotImplementedError()


class And(ConditionClause):
    """
    Returns true if all conditions are true.
    """
    def __init__(self, *clauses):
        self.clauses = clauses

    def is_true(self, elem_list):
        for clause in self.clauses:
            if not clause.is_true(elem_list):
                return False
        return True


class Contains(ConditionClause):
    """
    Returns true if all requirements are found in the input
    """

    def __init__(self, *requirements):
        self.requirements = requirements

    def is_true(self, elem_list):
        for requirement in self.requirements:
            if requirement not in elem_list:
                return False
        return True


class Duplicate(ConditionClause):
    """
    Returns true if there is more than one instance of elem in elem_list.
    """
    def __init__(self, elem):
        self.elem = elem

    def is_true(self, elem_list):
        return elem_list.count(self.elem) > 1


class KeepOriginal(object):
    """
    In sequences, this stands for 'keep the original value'
    """
    pass


class Next(ConditionClause):
    """
    Return true if A and B are found next to each other in the elem_list (with zero or more Filler elements
    between them).
    """
    def __init__(self, a_elem, b_elem):
        self.a_elem = a_elem
        self.b_elem = b_elem

    def is_true(self, elem_list):
        a_positions = []
        b_positions = []
        for index, elem in enumerate(elem_list):
            if elem == self.a_elem:
                a_positions.append(index)
            elif elem == self.b_elem:
                b_positions.append(index)

        for a_position in a_positions:
            for b_position in b_positions:
                left = min(a_position, b_position)
                right = max(a_position, b_position)
                between = elem_list[left + 1:right - 1]
                if len(between) == 0 or all([type(e) is Filler] for e in between):
                    return True
        return False


class Sequence(ConditionClause):
    """
    Returns true if the given sequence is found in elem_list. The sequence consists of date elements
    and wild cards.

    Wild cards:
    . (period): Any single date element (including Filler)
    """

    def __init__(self, *sequence):
        self.sequence = sequence

    def is_true(self, elem_list):
        seq_pos = 0  # if we find every element in sequence (pos == length(self.sequence), then a match is found

        for elem in elem_list:
            if self.match(elem, self.sequence[seq_pos]):
                seq_pos += 1
                if seq_pos == len(self.sequence):
                    return True
            else:
                seq_pos = 0  # reset if we exit sequence
        return False

    @staticmethod
    def match(elem, seq_expr):
        """
        Return True if elem (an element of elem_list) matches seq_expr, an element in self.sequence
        """
        if type(seq_expr) is str:  # wild-card
            if seq_expr == '.':  # match any element
                return True
            elif seq_expr == '\d':
                return elem.is_numerical()
            elif seq_expr == '\D':
                return not elem.is_numerical()
            else:  # invalid wild-card specified
                raise LookupError('{0} is not a valid wild-card'.format(seq_expr))
        else:  # date element
            return elem == seq_expr

    @staticmethod
    def find(find_seq, elem_list):
        """
        Return the first position in elem_list where find_seq starts
        """
        seq_pos = 0
        for index, elem in enumerate(elem_list):
            if Sequence.match(elem, find_seq[seq_pos]):
                seq_pos += 1
                if seq_pos == len(find_seq):  # found matching sequence
                    return index - seq_pos + 1
            else:  # exited sequence
                seq_pos = 0
        raise LookupError('Failed to find sequence in elem_list')


class Swap(ActionClause):
    """
    Returns elem_list with one element replaced by another
    """

    def __init__(self, remove_me, insert_me):
        self.remove_me = remove_me
        self.insert_me = insert_me

    def act(self, elem_list):
        copy = elem_list[:]
        pos = copy.index(self.remove_me)
        copy[pos] = self.insert_me
        return copy


class SwapDuplicateWhereSequenceNot(ActionClause):
    """
    Replace remove_me with insert_me in the case where remove_me is not part of the sequence.
    """
    def __init__(self, remove_me, insert_me, seq):
        self.remove_me = remove_me
        self.insert_me = insert_me
        self.seq = seq

    def act(self, elem_list):
        copy = elem_list[:]

        start_pos = Sequence.find(self.seq, copy)
        end_pos = start_pos + len(self.seq)  # do not replace within [start_pos, end_pos)

        for index, elem in enumerate(copy):
            if start_pos <= index < end_pos:  # within sequence
                continue
            else:  # outside of sequence
                if elem == self.remove_me:
                    copy[index] = self.insert_me
                    return copy

        raise LookupError('Failed to find element {0} to replace with {1} in {2} ignoring {3} between [{4},{5})'.format(self.remove_me, self.insert_me, copy, self.seq, start_pos, end_pos))


class SwapSequence(ActionClause):
    """
    Returns elem_list with sequence replaced with another sequence
    """
    def __init__(self, find_seq, swap_seq):
        self.find_seq = find_seq
        self.swap_seq = swap_seq

    def act(self, elem_list):
        copy = elem_list[:]

        start_pos = Sequence.find(self.find_seq, copy)
        for index, replacement in enumerate(self.swap_seq):
            if replacement is not KeepOriginal:
                copy[start_pos + index] = replacement

        # If we intend to delete items, we put None in the swap_seq and then clean up the list here
        while None in copy:
            copy.remove(None)

        return copy
