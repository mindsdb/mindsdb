import collections
import itertools
import string
from dateinfer.date_elements import *
from dateinfer.ruleproc import *

# DATE_ELEMENTS is an ordered sequence of date elements, but does not include filler. It is ordered in
# descending order of "restrictivity", the size of the range of acceptable inputs. The order is a little loose
# since date element domains do not necessarily overlap (the range of Jan .. Dec is 12, but the domain is
# independent of hours 0 .. 23, but overall a lesser value should be preferred over a greater value. The RULES
# will be applied after the list is generated following these precedence rules.
DATE_ELEMENTS = (AMPM(),
                 MonthNum(),
                 Hour12(),
                 Hour24(),
                 DayOfMonth(),
                 Minute(),
                 Second(),
                 Year2(),
                 Year4(),
                 UTCOffset(),
                 MonthTextShort(),
                 MonthTextLong(),
                 WeekdayShort(),
                 WeekdayLong(),
                 Timezone())

F = Filler  # short-hand to clarify rules
RULES = [
    If(Sequence(MonthNum, F(':'), '\d', F(':'), '\d'),
       SwapSequence([MonthNum, F(':'), '\d', F(':'), '\d'], [Hour12, F(':'), Minute, F(':'), Second])),
    If(Sequence(Hour24, F(':'), '\d', F(':'), '\d'),
       SwapSequence([Hour24, F(':'), '\d', F(':'), '\d'], [Hour24, F(':'), Minute, F(':'), Second])),
    If(Sequence(MonthNum, F(':'), '\d', '\D'),
       SwapSequence([MonthNum, F(':'), '.'], [Hour12, F(':'), Minute])),
    If(Sequence(Hour24, F(':'), '\d', '\D'),
       SwapSequence([Hour24, F(':'), '\d'], [Hour24, F(':'), Minute])),
    If(And(
        Sequence(Hour12, F(':'), Minute),
        Contains(Hour24)),
       Swap(Hour24, DayOfMonth)
    ),
    If(And(
        Sequence(Hour12, F(':'), Minute),
        Duplicate(Hour12)),
       SwapDuplicateWhereSequenceNot(Hour12, MonthNum, (Hour12, F(':')))
    ),
    If(And(
        Sequence(Hour24, F(':'), Minute),
        Duplicate(Hour24)),
       SwapDuplicateWhereSequenceNot(Hour24, DayOfMonth, [Hour24, F(':')])
    ),
    If(Contains(MonthNum, MonthTextLong), Swap(MonthNum, DayOfMonth)),
    If(Contains(MonthNum, MonthTextShort), Swap(MonthNum, DayOfMonth)),
    If(Sequence(MonthNum, '.', Hour12), SwapSequence([MonthNum, '.', Hour12], [MonthNum, KeepOriginal, DayOfMonth])),
    If(Sequence(MonthNum, '.', Hour24), SwapSequence([MonthNum, '.', Hour24], [MonthNum, KeepOriginal, DayOfMonth])),
    If(Sequence(Hour12, '.', MonthNum), SwapSequence([Hour24, '.', MonthNum], [DayOfMonth, KeepOriginal, MonthNum])),
    If(Sequence(Hour24, '.', MonthNum), SwapSequence([Hour24, '.', MonthNum], [DayOfMonth, KeepOriginal, MonthNum])),
    If(Duplicate(MonthNum), Swap(MonthNum, DayOfMonth)),
    If(Sequence(F('+'), Year4), SwapSequence([F('+'), Year4], [UTCOffset, None])),
    If(Sequence(F('-'), Year4), SwapSequence([F('+'), Year4], [UTCOffset, None]))
]


def infer(examples, alt_rules=None):
    """
    Returns a datetime.strptime-compliant format string for parsing the *most likely* date format
    used in examples. examples is a list containing example date strings.
    """
    date_classes = _tag_most_likely(examples)

    if alt_rules:
        date_classes = _apply_rewrites(date_classes, alt_rules)
    else:
        date_classes = _apply_rewrites(date_classes, RULES)

    date_string = ''
    for date_class in date_classes:
        date_string += date_class.directive

    return date_string


def _apply_rewrites(date_classes, rules):
    """
    Return a list of date elements by applying rewrites to the initial date element list
    """
    for rule in rules:
        date_classes = rule.execute(date_classes)

    return date_classes


def _mode(elems):
    """
    Find the mode (most common element) in list elems. If there are ties, this function returns the least value.

    If elems is an empty list, returns None.
    """
    if len(elems) == 0:
        return None

    c = collections.Counter()
    c.update(elems)

    most_common = c.most_common(1)
    most_common.sort()
    return most_common[0][0]  # most_common[0] is a tuple of key and count; no need for the count


def _most_restrictive(date_elems):
    """
    Return the date_elem that has the most restrictive range from date_elems
    """
    most_index = len(DATE_ELEMENTS)
    for date_elem in date_elems:
        if date_elem in DATE_ELEMENTS and DATE_ELEMENTS.index(date_elem) < most_index:
            most_index = DATE_ELEMENTS.index(date_elem)
    if most_index < len(DATE_ELEMENTS):
        return DATE_ELEMENTS[most_index]
    else:
        raise KeyError('No least restrictive date element found')


def _percent_match(date_classes, tokens):
    """
    For each date class, return the percentage of tokens that the class matched (floating point [0.0 - 1.0]). The
    returned value is a tuple of length patterns. Tokens should be a list.
    """
    match_count = [0] * len(date_classes)

    for i, date_class in enumerate(date_classes):
        for token in tokens:
            if date_class.is_match(token):
                match_count[i] += 1

    percentages = tuple([float(m) / len(tokens) for m in match_count])
    return percentages


def _tag_most_likely(examples):
    """
    Return a list of date elements by choosing the most likely element for a token within examples (context-free).
    """
    tokenized_examples = [_tokenize_by_character_class(example) for example in examples]

    # We currently need the tokenized_examples to all have the same length, so drop instances that have a length
    # that does not equal the mode of lengths within tokenized_examples
    token_lengths = [len(e) for e in tokenized_examples]
    token_lengths_mode = _mode(token_lengths)
    tokenized_examples = [example for example in tokenized_examples if len(example) == token_lengths_mode]

    # Now, we iterate through the tokens, assigning date elements based on their likelihood. In cases where
    # the assignments are unlikely for all date elements, assign filler.
    most_likely = []
    for token_index in range(0, token_lengths_mode):
        tokens = [token[token_index] for token in tokenized_examples]
        probabilities = _percent_match(DATE_ELEMENTS, tokens)
        max_prob = max(probabilities)
        if max_prob < 0.5:
            most_likely.append(Filler(_mode(tokens)))
        else:
            if probabilities.count(max_prob) == 1:
                most_likely.append(DATE_ELEMENTS[probabilities.index(max_prob)])
            else:
                choices = []
                for index, prob in enumerate(probabilities):
                    if prob == max_prob:
                        choices.append(DATE_ELEMENTS[index])
                most_likely.append(_most_restrictive(choices))

    return most_likely


def _tokenize_by_character_class(s):
    """
    Return a list of strings by splitting s (tokenizing) by character class.

    For example:
    _tokenize_by_character_class('Sat Jan 11 19:54:52 MST 2014') => ['Sat', ' ', 'Jan', ' ', '11', ' ', '19', ':',
        '54', ':', '52', ' ', 'MST', ' ', '2014']
    _tokenize_by_character_class('2013-08-14') => ['2013', '-', '08', '-', '14']
    """
    character_classes = [string.digits, string.ascii_letters, string.punctuation, string.whitespace]

    result = []
    rest = list(s)
    while rest:
        progress = False
        for character_class in character_classes:
            if rest[0] in character_class:
                progress = True
                token = ''
                for take_away in itertools.takewhile(lambda c: c in character_class, rest[:]):
                    token += take_away
                    rest.pop(0)
                result.append(token)
                break
        if not progress:  # none of the character classes matched; unprintable character?
            result.append(rest[0])
            rest = rest[1:]

    return result
