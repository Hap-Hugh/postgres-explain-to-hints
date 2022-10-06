def explain_decoder(k):
    while 1:
        if 'Plan' in k:
            print("Find Plan")
            k = k['Plan']
            get_node_type(k)

            if 'Plans' in k:
                print("Find Plans")

                for i in k['Plans']:
                    explain_decoder(i)
        else:
            get_node_type(k)
            if 'Plans' in k:
                print("Find Plans")

                for i in k['Plans']:
                    explain_decoder(i)
            break

        print()
        input()


def get_node_type(k):
    print(k['Node Type'])
    if k['Node Type'] == 'Merge Join':
        print(k['Merge Cond'])
    elif k['Node Type'] == 'Hash Join':
        print(k['Join Filter'])
    elif k['Node Type'] == 'Nested Loop':
        if 'Join Filter' in k.keys():
            print(k['Join Filter'])
        else:
            print("No join cond in Nested Loop!")
            for i in k['Plans']:
                get_node_type(i)
    elif k['Node Type'] in ["Index Scan", "Bitmap Scan", "Seq Scan"]:
        print(k['Node Type'] + "on" + k['Alias'])
