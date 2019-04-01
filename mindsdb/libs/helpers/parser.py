

test = """

SELECT DISTINCT c as cosa, col2,
col3 as "year", col5 as "select", * FROM (
  select * FROM schemab.my_teable WHERE   c = 10 AND col3 < 5 and col2 in (20, 30) Or ( c in (SELECT count(distinct names) from name_list where area = 1) and col2 >= 5 )
) table_2

where (col4 > 10   AND col5  lIKE '%noname''myname )'  ) OR ( col5 = '(select * Fom ) ()' AND cold3 = 2 )

"""


#first step replace '' for __ESC_QUOTES__

# replace all text with __VAR_varindex__ build a map

# replace all double space  or greater with single space
# replace all '( ' and ' )' with '(' ')'
# upper case all reserved words that are not enclosed by ""

# replace each sub statement with  __SUB:INDEX.child.grandchild..___ build a map



def replaceTexts(str):
    """
    The whole point of this function is to
    replace strings with __TEXTVAR_index__ and return a variable map dictionary

    :param str: the string that we want to make replacements on
    :return: replaced_str, text_var_map
    """

    # these are the variables to be returned
    text_var_map = {}
    ret = ''


    inside = False # as we walk the string, if this is true is because we are inside a text variable
    str_len = len(str)
    collected_text = '' # as we find one text variable we collect it here
    text_var_count = 0 # the variable count
    skip_next = False

    # here we walk oall the string in search for ' and store its contents replace the string and update the map
    for i,c in enumerate(str):

        if skip_next == True:
            skip_next = False
            continue

        next = '' if i>=(str_len-1) else str[i+1]
        if c == "'" and not inside:
            inside = True
            text_var_count +=1
            continue
        if c == "'" and inside and next!="'":
            inside = False
            map_key = '__TEXTVAR_{text_var_count}__'.format(text_var_count=text_var_count)
            text_var_map[map_key] = collected_text
            collected_text = ''
            ret += map_key
            continue
        elif c == "'" and inside and next=="'":
            skip_next = True
            collected_text += "'"

        if inside == True:
            collected_text += c
        else:
            ret += c

    return ret, text_var_map


def cleanStr(str):
    """
    Do some cleaning, remove double spaces new lines, clean commas and others
    :param str:
    :return:
    """
    str = str.replace("\n", ' ')

    clean_space_from = ['( ', ' )', ', ', '[ ', ' ]']
    for str_to_replace in clean_space_from:
        actual = str_to_replace.replace(' ', '')
        str = str.replace(actual, str_to_replace)

    str = ' (' + str + ') '
    str = ' '.join(str.split())

    return str

def replaceSubStatements(str, count = 0, node='0'):

    ret = ''

    ret_map = {}

    for i, c in enumerate(str):

        if c == '(':
            count += 1
            subnode = '.'.join(node.split('.')[-1])
            node = '{node}.{count}'.format(node=subnode,count=count)

            node_str, count,  map = replaceSubStatements(str[i+1:], count, node)
            ret_map[node] = map

            ret += '__NODE:{node}__'.format(node=node)

            break

        if c == ')':
            count -= 1

            return ret, count, ret_map


        ret += c

    return ret, count, ret_map



def parse(str):

    str, text_var_map = replaceTexts(str)
    str = cleanStr(str)


    log.info(str)


'''
 ( select a from ( select u from b ) where u > 10 )
'''



map = {

    'expr': '__0__',
    'parts': [
        {
            'expr': ' select a from __0__ where u > 10 ',
            'parts': [
                {
                    'expr': ' select u from b ',
                    'parts': []
                }
            ]
        }
    ]
}



parse(test)