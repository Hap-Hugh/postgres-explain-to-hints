import json
'''
Decode the pg_explain result (json format) into readable format

Input: postgres explain (json format)

Output:

Merge Join(Merge Join(Merge Join(Merge Join(Nested Loop(yuxi,mc),Nested Loop(yuxi,mi_idx)),Nested Loop(yuxi,ct)),Nested Loop(yuxi,t)),Nested Loop(yuxi,it))
['Nested Loop', 'Nested Loop', 'Merge Join(mc.movie_id = mi_idx.movie_id)', 'Nested Loop', 'Merge Join(mc.company_type_id = ct.id)', 'Nested Loop', 'Merge Join(mc.movie_id = t.id)', 'Nested Loop', 'Merge Join(mi_idx.info_type_id = it.id)']
['Index Scan(mc)', 'Index Scan(mi_idx)', 'Index Scan(ct)', 'Index Scan(t)', 'Index Scan(it)']

'''

join_type_to_cond_field = {
    'Merge Join' : 'Merge Cond',
    'Hash Join' : 'Hash Cond',
    'Nested Loop' : 'Join Filter'
}

def load(filename):
    with open(filename) as fin:
        data = json.load(fin)
    return data[0][0][0]['Plan']


def decode(plans, parent):
    if len(plans) < 1 or len(plans) > 2:
        raise ValueError('incorrect number of plans')
        
    join_order = ''
    single_scans = []
    join_conds = []
    
    if len(plans) == 2:
        if plans[0]['Parent Relationship'] == 'Inner' and plans[1]['Parent Relationship'] == 'Outer':
            plans[0], plans[1] = plans[1], plans[0]
        if plans[0]['Parent Relationship'] != 'Outer' or plans[1]['Parent Relationship'] != 'Inner':
            raise ValueError('missing inner/outer relationship')
            
    for i in range(len(plans)):
        node_type = plans[i]['Node Type']
        if 'Plans' not in plans[i]:
            # is a leaf
            # print("!!!", node_type)
            single_scans.append(node_type + '(' + plans[i]['Alias'] + ')')

        if node_type in ['Aggregate', 'Gather', 'Sort', 'Materialize', 'Sort', 'Hash', 'Gather Merge']:
            _join_order, _join_conds, _single_scans = decode(plans[i]['Plans'], parent)
            join_order += _join_order
            join_conds += _join_conds
            single_scans.extend(_single_scans)
        elif node_type == 'Limit':
            join_order += 'yuxi'
        elif node_type in ['Merge Join', 'Hash Join', 'Nested Loop']:
            join_order += node_type + '('
            _join_order, _join_conds, _single_scans = decode(plans[i]['Plans'], node_type)
            join_order += _join_order
            join_conds += _join_conds
            cond_field = join_type_to_cond_field[node_type]
            merge_cond = '' if cond_field not in plans[i] else plans[i][cond_field]
            join_conds.append(node_type + merge_cond)
            single_scans.extend(_single_scans)
            join_order += ')'
        elif node_type in ['Index Scan', 'Seq Scan', 'Bitmap Scan', 'Index Only Scan']:
            join_order += plans[i]['Alias']
        else:
            raise NotImplementedError(node_type)
        
        if i < len(plans) - 1:
            join_order += ','
            
    return join_order, join_conds, single_scans

# examples
plan = load('query_plan_1.json')
join_order, join_conds, single_scans = decode(plan['Plans'], plan['Node Type'])
print(join_order)
print(join_conds)
print(single_scans)


def start_with_tab(str, x):
    tab_string = ""
    for i in range(x):
        tab_string = tab_string + "    "
    if str.startswith(tab_string) and not str.startswith(tab_string + "    "):
        return True
    else:
        return False


JOIN_KEYWORD = ["MergeJoin", "HashJoin", "NestLoop"]
SCAN_KEYWORD = ["SeqScan", "IdxScan"]


join_kw_to_we_need = {
    'MergeJoin' : 'Merge Join',
    'HashJoin' : 'Hash Join',
    'NestLoop' : 'Nested Loop'
}

scan_kw_to_we_need = {
    "SeqScan": "Seq Scan",
    "IdxScan": "Index Scan"
}

def pre_build_plan_tree(input_string):
    lines = input_string.strip().split('\n')
    return_lines = []
    for i in range(len(lines)):
        flag = False
        for jkw in JOIN_KEYWORD:
            if jkw in lines[i]:
                flag = True
                break
        for sjw in SCAN_KEYWORD:
            if sjw in lines[i]:
                flag = True
                break
        if flag:
            return_lines.append(lines[i])
    return "\n".join(return_lines)



### MergeJoin(MergeJoin(HashJoin(v, p), b), MergeJoin(c, u))

def build_plan_tree(input_string, x):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Extract the operator name and its arguments from the first line
    operator_args = lines[0].split('(')[1].split(')')[0].split()
    print(operator_args)
    operator_name = lines[0].split('(')[0]
    
    if len(operator_args) == 1:
        return operator_args[0]


    left_start = False
    right_start = False
    left_lines = []
    right_lines = []
    for i in range(1, len(lines)):
        if start_with_tab(lines[i], x+1) and not left_start:
            left_start = True
            right_start = False
            left_lines.append(lines[i])
            continue
        if start_with_tab(lines[i], x+1) and left_start:
            left_start = False
            right_start = True
            right_lines.append(lines[i])
            continue
        if left_start:
            left_lines.append(lines[i])
            continue
        if right_start:
            right_lines.append(lines[i])
            continue

    left_result = build_plan_tree("\n".join(left_lines), x+1)
    right_result = build_plan_tree("\n".join(right_lines), x+1)

    return join_kw_to_we_need[operator_name] + "(" + left_result + "," + right_result + ")"


def build_scan_methods_set(lines):
    result = set()
    lines = input_string.strip().split('\n')
    for i in range(0, len(lines)):
        for k in SCAN_KEYWORD:
            if k in lines[i]:
                operator_args = lines[i].split('(')[1].split(')')[0].split()
                print(operator_args)
                operator_name = lines[i].split('(')[0].split()[0]
                result.add(scan_kw_to_we_need[operator_name] + "(" + operator_args[0] + ")")
    return result


# Read the input from "tmp_plan.txt"
with open("tmp_plan.txt") as f:
    input_string = f.read()

# Build the plan tree and print the serialized plan
preprocessed_string = pre_build_plan_tree(input_string)
print(preprocessed_string)
scan_method_set = build_scan_methods_set(preprocessed_string)
print(scan_method_set)
serialized_plan = build_plan_tree(preprocessed_string, 1)
print(serialized_plan)