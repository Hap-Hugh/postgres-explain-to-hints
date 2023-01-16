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
