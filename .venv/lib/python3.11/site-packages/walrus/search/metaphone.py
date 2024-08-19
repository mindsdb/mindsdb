#!python
#coding= utf-8
#This script implements the Double Metaphone algorithm (c) 1998, 1999 by
#Lawrence Philips
#it was translated to Python from the C source written by Kevin Atkinson
#(http://aspell.net/metaphone/)
#By Andrew Collins - January 12, 2007 who claims no rights to this work
#http://www.atomodo.com/code/double-metaphone
#Tested with Pyhon 2.4.3
#Updated Feb 14, 2007 - Found a typo in the 'gh' section
#Updated Dec 17, 2007 - Bugs fixed in 'S', 'Z', and 'J' sections.
#Thanks Chris Leong!
#Updated June 25, 2010 - several bugs fixed thanks to Nils Johnsson for a
#   spectacular bug squashing effort. There were many cases where this
#   function wouldn't give the same output
#   as the original C source that were fixed by his careful attention and
#   excellent communication.
#   The script was also updated to use utf-8 rather than latin-1.
import sys
try:
    NNNN = unicode('N')
    decode = lambda x: x.decode('utf-8', 'ignore')
except:
    NNNN = 'N'
    decode = lambda x: x

CCCC = 'Ç'
VOWELS = frozenset((decode(x) for x in ('A', 'E', 'I', 'O', 'U', 'Y')))
GNKN = frozenset((decode(x) for x in ('GN', 'KN', 'PN', 'WR', 'PS')))


def dm(st) :
    """dm(string) -> (string, string or None)
    returns the double metaphone codes for given string - always a tuple
    there are no checks done on the input string, but it should be a single
    word or name."""
    st = decode(st)
    # st is short for string. I usually prefer descriptive over short,
    # but this var is used a lot!
    st = st.upper()
    is_slavo_germanic = (st.find('W') > -1 or st.find('K') > -1 or
                        st.find('CZ') > -1 or st.find('WITZ') > -1)
    length = len(st)
    first = 2
    # so we can index beyond the beginning and end of the input string
    st = ('-') * first + st + (' ' * 5)
    last = first + length -1
    pos = first # pos is short for position
    pri = sec = '' # primary and secondary metaphone codes
    #skip these silent letters when at start of word
    if st[first:first+2] in GNKN:
        pos += 1
    # Initial 'X' is pronounced 'Z' e.g. 'Xavier'
    if st[first] == 'X' :
        pri = sec = 'S' #'Z' maps to 'S'
        pos += 1
    # main loop through chars in st
    while pos <= last :
        #print str(pos) + '\t' + st[pos]
        ch = st[pos] # ch is short for character
        # nxt (short for next characters in metaphone code) is set to  a
        # tuple of the next characters in
        # the primary and secondary codes and how many characters to move
        # forward in the string.
        # the secondary code letter is given only when it is different than
        # the primary.
        # This is just a trick to make the code easier to write and read.
        #
        # default action is to add nothing and move to next char
        nxt = (None, 1)
        if ch in VOWELS :
            nxt = (None, 1)
            if pos == first : # all init VOWELS now map to 'A'
                nxt = ('A', 1)
        elif ch == 'B' :
            #"-mb", e.g", "dumb", already skipped over... see 'M' below
            if st[pos+1] == 'B' :
                nxt = ('P', 2)
            else :
                nxt = ('P', 1)
        elif ch == 'C' :
            # various germanic
            if (pos > (first + 1) and st[pos-2] not in VOWELS and
                    st[pos-1:pos+2] == 'ACH' and
                    (st[pos+2] not in ['I', 'E'] or
                     st[pos-2:pos+4] in ['BACHER', 'MACHER'])):
                nxt = ('K', 2)
            # special case 'CAESAR'
            elif pos == first and st[first:first+6] == 'CAESAR' :
                nxt = ('S', 2)
            elif st[pos:pos+4] == 'CHIA' : #italian 'chianti'
                nxt = ('K', 2)
            elif st[pos:pos+2] == 'CH' :
                # find 'michael'
                if pos > first and st[pos:pos+4] == 'CHAE' :
                    nxt = ('K', 'X', 2)
                elif pos == first and (st[pos+1:pos+6] in ['HARAC', 'HARIS'] or \
                   st[pos+1:pos+4] in ["HOR", "HYM", "HIA", "HEM"]) and st[first:first+5] != 'CHORE' :
                    nxt = ('K', 2)
                #germanic, greek, or otherwise 'ch' for 'kh' sound
                elif st[first:first+4] in ['VAN ', 'VON '] or st[first:first+3] == 'SCH' \
                   or st[pos-2:pos+4] in ["ORCHES", "ARCHIT", "ORCHID"] \
                   or st[pos+2] in ['T', 'S'] \
                   or ((st[pos-1] in ["A", "O", "U", "E"] or pos == first) \
                   and st[pos+2] in ["L", "R", "N", "M", "B", "H", "F", "V", "W", " "]) :
                    nxt = ('K', 1)
                else :
                    if pos > first :
                        if st[first:first+2] == 'MC' :
                            nxt = ('K', 2)
                        else :
                            nxt = ('X', 'K', 2)
                    else :
                        nxt = ('X', 2)
            #e.g, 'czerny'
            elif st[pos:pos+2] == 'CZ' and st[pos-2:pos+2] != 'WICZ' :
                nxt = ('S', 'X', 2)
            #e.g., 'focaccia'
            elif st[pos+1:pos+4] == 'CIA' :
                nxt = ('X', 3)
            #double 'C', but not if e.g. 'McClellan'
            elif st[pos:pos+2] == 'CC' and not (pos == (first +1) and st[first] == 'M') :
                #'bellocchio' but not 'bacchus'
                if st[pos+2] in ["I", "E", "H"] and st[pos+2:pos+4] != 'HU' :
                    #'accident', 'accede' 'succeed'
                    if (pos == (first +1) and st[first] == 'A') or \
                       st[pos-1:pos+4] in ['UCCEE', 'UCCES'] :
                        nxt = ('KS', 3)
                    #'bacci', 'bertucci', other italian
                    else:
                        nxt = ('X', 3)
                else :
                    nxt = ('K', 2)
            elif st[pos:pos+2] in ["CK", "CG", "CQ"] :
                nxt = ('K', 'K', 2)
            elif st[pos:pos+2] in ["CI", "CE", "CY"] :
                #italian vs. english
                if st[pos:pos+3] in ["CIO", "CIE", "CIA"] :
                    nxt = ('S', 'X', 2)
                else :
                    nxt = ('S', 2)
            else :
                #name sent in 'mac caffrey', 'mac gregor
                if st[pos+1:pos+3] in [" C", " Q", " G"] :
                    nxt = ('K', 3)
                else :
                    if st[pos+1] in ["C", "K", "Q"] and st[pos+1:pos+3] not in ["CE", "CI"] :
                        nxt = ('K', 2)
                    else : # default for 'C'
                        nxt = ('K', 1)
        #elif ch == CCCC:
        #	nxt = ('S', 1)
        elif ch == 'D' :
            if st[pos:pos+2] == 'DG' :
                if st[pos+2] in ['I', 'E', 'Y'] : #e.g. 'edge'
                    nxt = ('J', 3)
                else :
                    nxt = ('TK', 2)
            elif st[pos:pos+2] in ['DT', 'DD'] :
                nxt = ('T', 2)
            else :
                nxt = ('T', 1)
        elif ch == 'F' :
            if st[pos+1] == 'F' :
                nxt = ('F', 2)
            else :
                nxt = ('F', 1)
        elif ch == 'G' :
            if st[pos+1] == 'H' :
                if pos > first and st[pos-1] not in VOWELS :
                    nxt = ('K', 2)
                elif pos < (first + 3) :
                    if pos == first : #'ghislane', ghiradelli
                        if st[pos+2] == 'I' :
                            nxt = ('J', 2)
                        else :
                            nxt = ('K', 2)
                #Parker's rule (with some further refinements) - e.g., 'hugh'
                elif (pos > (first + 1) and st[pos-2] in ['B', 'H', 'D'] ) \
                   or (pos > (first + 2) and st[pos-3] in ['B', 'H', 'D'] ) \
                   or (pos > (first + 3) and st[pos-4] in ['B', 'H'] ) :
                    nxt = (None, 2)
                else :
                    # e.g., 'laugh', 'McLaughlin', 'cough', 'gough', 'rough', 'tough'
                    if pos > (first + 2) and st[pos-1] == 'U' \
                       and st[pos-3] in ["C", "G", "L", "R", "T"] :
                        nxt = ('F', 2)
                    else :
                        if pos > first and st[pos-1] != 'I' :
                            nxt = ('K', 2)
            elif st[pos+1] == 'N' :
                if pos == (first +1) and st[first] in VOWELS and not is_slavo_germanic :
                    nxt = ('KN', 'N', 2)
                else :
                    # not e.g. 'cagney'
                    if st[pos+2:pos+4] != 'EY' and st[pos+1] != 'Y' and not is_slavo_germanic :
                        nxt = ('N', 'KN', 2)
                    else :
                        nxt = ('KN', 2)
            # 'tagliaro'
            elif st[pos+1:pos+3] == 'LI' and not is_slavo_germanic :
                nxt = ('KL', 'L', 2)
            # -ges-,-gep-,-gel-, -gie- at beginning
            elif pos == first and (st[pos+1] == 'Y' \
               or st[pos+1:pos+3] in ["ES", "EP", "EB", "EL", "EY", "IB", "IL", "IN", "IE", "EI", "ER"]) :
                nxt = ('K', 'J', 2)
            # -ger-,  -gy-
            elif (st[pos+1:pos+2] == 'ER' or st[pos+1] == 'Y') \
               and st[first:first+6] not in ["DANGER", "RANGER", "MANGER"] \
               and st[pos-1] not in ['E', 'I'] and st[pos-1:pos+2] not in ['RGY', 'OGY'] :
                nxt = ('K', 'J', 2)
            # italian e.g, 'biaggi'
            elif st[pos+1] in ['E', 'I', 'Y'] or st[pos-1:pos+3] in ["AGGI", "OGGI"] :
                # obvious germanic
                if st[first:first+4] in ['VON ', 'VAN '] or st[first:first+3] == 'SCH' \
                   or st[pos+1:pos+3] == 'ET' :
                    nxt = ('K', 2)
                else :
                    # always soft if french ending
                    if st[pos+1:pos+5] == 'IER ' :
                        nxt = ('J', 2)
                    else :
                        nxt = ('J', 'K', 2)
            elif st[pos+1] == 'G' :
                nxt = ('K', 2)
            else :
                nxt = ('K', 1)
        elif ch == 'H' :
            # only keep if first & before vowel or btw. 2 VOWELS
            if (pos == first or st[pos-1] in VOWELS) and st[pos+1] in VOWELS :
                nxt = ('H', 2)
            else : # (also takes care of 'HH')
                nxt = (None, 1)
        elif ch == 'J' :
            # obvious spanish, 'jose', 'san jacinto'
            if st[pos:pos+4] == 'JOSE' or st[first:first+4] == 'SAN ' :
                if (pos == first and st[pos+4] == ' ') or st[first:first+4] == 'SAN ' :
                    nxt = ('H',)
                else :
                    nxt = ('J', 'H')
            elif pos == first and st[pos:pos+4] != 'JOSE' :
                nxt = ('J', 'A') # Yankelovich/Jankelowicz
            else :
                # spanish pron. of e.g. 'bajador'
                if st[pos-1] in VOWELS and not is_slavo_germanic \
                   and st[pos+1] in ['A', 'O'] :
                    nxt = ('J', 'H')
                else :
                    if pos == last :
                        nxt = ('J', ' ')
                    else :
                        if st[pos+1] not in ["L", "T", "K", "S", "N", "M", "B", "Z"] \
                           and st[pos-1] not in ["S", "K", "L"] :
                            nxt = ('J',)
                        else :
                            nxt = (None, )
            if st[pos+1] == 'J' :
                nxt = nxt + (2,)
            else :
                nxt = nxt + (1,)
        elif ch == 'K' :
            if st[pos+1] == 'K' :
                nxt = ('K', 2)
            else :
                nxt = ('K', 1)
        elif ch == 'L' :
            if st[pos+1] == 'L' :
                # spanish e.g. 'cabrillo', 'gallegos'
                if (pos == (last - 2) and st[pos-1:pos+3] in ["ILLO", "ILLA", "ALLE"]) \
                   or ((st[last-1:last+1] in ["AS", "OS"] or st[last] in ["A", "O"]) \
                   and st[pos-1:pos+3] == 'ALLE') :
                    nxt = ('L', '', 2)
                else :
                    nxt = ('L', 2)
            else :
                nxt = ('L', 1)
        elif ch == 'M' :
            if st[pos+1:pos+4] == 'UMB' \
               and (pos + 1 == last or st[pos+2:pos+4] == 'ER') \
               or st[pos+1] == 'M' :
                nxt = ('M', 2)
            else :
                nxt = ('M', 1)
        elif ch == 'N' :
            if st[pos+1] == 'N' :
                nxt = ('N', 2)
            else :
                nxt = ('N', 1)
        elif ch == NNNN:
            nxt = ('N', 1)
        elif ch == 'P' :
            if st[pos+1] == 'H' :
                nxt = ('F', 2)
            elif st[pos+1] in ['P', 'B'] : # also account for "campbell", "raspberry"
                nxt = ('P', 2)
            else :
                nxt = ('P', 1)
        elif ch == 'Q' :
            if st[pos+1] == 'Q' :
                nxt = ('K', 2)
            else :
                nxt = ('K', 1)
        elif ch == 'R' :
            # french e.g. 'rogier', but exclude 'hochmeier'
            if pos == last and not is_slavo_germanic \
               and st[pos-2:pos] == 'IE' and st[pos-4:pos-2] not in ['ME', 'MA'] :
                nxt = ('', 'R')
            else :
                nxt = ('R',)
            if st[pos+1] == 'R' :
                nxt = nxt + (2,)
            else :
                nxt = nxt + (1,)
        elif ch == 'S' :
            # special cases 'island', 'isle', 'carlisle', 'carlysle'
            if st[pos-1:pos+2] in ['ISL', 'YSL'] :
                nxt = (None, 1)
            # special case 'sugar-'
            elif pos == first and st[first:first+5] == 'SUGAR' :
                nxt =('X', 'S', 1)
            elif st[pos:pos+2] == 'SH' :
                # germanic
                if st[pos+1:pos+5] in ["HEIM", "HOEK", "HOLM", "HOLZ"] :
                    nxt = ('S', 2)
                else :
                    nxt = ('X', 2)
            # italian & armenian
            elif st[pos:pos+3] in ["SIO", "SIA"] or st[pos:pos+4] == 'SIAN' :
                if not is_slavo_germanic :
                    nxt = ('S', 'X', 3)
                else :
                    nxt = ('S', 3)
            # german & anglicisations, e.g. 'smith' match 'schmidt', 'snider'
            # match 'schneider'
            # also, -sz- in slavic language altho in hungarian it is
            # pronounced 's'
            elif (pos == first and st[pos+1] in ["M", "N", "L", "W"]) or st[pos+1] == 'Z' :
                nxt = ('S', 'X')
                if st[pos+1] == 'Z' :
                    nxt = nxt + (2,)
                else :
                    nxt = nxt + (1,)
            elif st[pos:pos+2] == 'SC' :
                # Schlesinger's rule
                if st[pos+2] == 'H' :
                    # dutch origin, e.g. 'school', 'schooner'
                    if st[pos+3:pos+5] in ["OO", "ER", "EN", "UY", "ED", "EM"] :
                        # 'schermerhorn', 'schenker'
                        if st[pos+3:pos+5] in ['ER', 'EN'] :
                            nxt = ('X', 'SK', 3)
                        else :
                            nxt = ('SK', 3)
                    else :
                        if pos == first and st[first+3] not in VOWELS and st[first+3] != 'W' :
                            nxt = ('X', 'S', 3)
                        else :
                            nxt = ('X', 3)
                elif st[pos+2] in ['I', 'E', 'Y'] :
                    nxt = ('S', 3)
                else :
                    nxt = ('SK', 3)
            # french e.g. 'resnais', 'artois'
            elif pos == last and st[pos-2:pos] in ['AI', 'OI'] :
                nxt = ('', 'S', 1)
            else :
                nxt = ('S',)
                if st[pos+1] in ['S', 'Z'] :
                    nxt = nxt + (2,)
                else :
                    nxt = nxt + (1,)
        elif ch == 'T' :
            if st[pos:pos+4] == 'TION' :
                nxt = ('X', 3)
            elif st[pos:pos+3] in ['TIA', 'TCH'] :
                nxt = ('X', 3)
            elif st[pos:pos+2] == 'TH' or st[pos:pos+3] == 'TTH' :
                # special case 'thomas', 'thames' or germanic
                if st[pos+2:pos+4] in ['OM', 'AM'] or st[first:first+4] in ['VON ', 'VAN '] \
                   or st[first:first+3] == 'SCH' :
                    nxt = ('T', 2)
                else :
                    nxt = ('0', 'T', 2)
            elif st[pos+1] in ['T', 'D'] :
                nxt = ('T', 2)
            else :
                nxt = ('T', 1)
        elif ch == 'V' :
            if st[pos+1] == 'V' :
                nxt = ('F', 2)
            else :
                nxt = ('F', 1)
        elif ch == 'W' :
            # can also be in middle of word
            if st[pos:pos+2] == 'WR' :
                nxt = ('R', 2)
            elif pos == first and (st[pos+1] in VOWELS or st[pos:pos+2] == 'WH') :
                # Wasserman should match Vasserman
                if st[pos+1] in VOWELS :
                    nxt = ('A', 'F', 1)
                else :
                    nxt = ('A', 1)
            # Arnow should match Arnoff
            elif (pos == last and st[pos-1] in VOWELS) \
               or st[pos-1:pos+5] in ["EWSKI", "EWSKY", "OWSKI", "OWSKY"] \
               or st[first:first+3] == 'SCH' :
                nxt = ('', 'F', 1)
            # polish e.g. 'filipowicz'
            elif st[pos:pos+4] in ["WICZ", "WITZ"] :
                nxt = ('TS', 'FX', 4)
            else : # default is to skip it
                nxt = (None, 1)
        elif ch == 'X' :
            # french e.g. breaux
            nxt = (None,)
            if not(pos == last and (st[pos-3:pos] in ["IAU", "EAU"] \
               or st[pos-2:pos] in ['AU', 'OU'])):
                nxt = ('KS',)
            if st[pos+1] in ['C', 'X'] :
                nxt = nxt + (2,)
            else :
                nxt = nxt + (1,)
        elif ch == 'Z' :
            # chinese pinyin e.g. 'zhao'
            if st[pos+1] == 'H' :
                nxt = ('J',)
            elif st[pos+1:pos+3] in ["ZO", "ZI", "ZA"] \
               or (is_slavo_germanic and pos > first and st[pos-1] != 'T') :
                nxt = ('S', 'TS')
            else :
                nxt = ('S',)
            if st[pos+1] == 'Z' :
                nxt = nxt + (2,)
            else :
                nxt = nxt + (1,)
        # ----------------------------------
        # --- end checking letters------
        # ----------------------------------
        #print str(nxt)
        if len(nxt) == 2 :
            if nxt[0] :
                pri += nxt[0]
                sec += nxt[0]
            pos += nxt[1]
        elif len(nxt) == 3 :
            if nxt[0] :
                pri += nxt[0]
            if nxt[1] :
                sec += nxt[1]
            pos += nxt[2]
    if pri == sec :
        return (pri, None)
    else :
        return (pri, sec)
