def load_gf_projection():
    import pandas as pd
    import pickle

    print(0)
    gf = pd.read_table('cold/GMGC.gf.cluster_members.tsv', header=None, index_col=0, squeeze=True)
    print(1)
    r_rename = pickle.load(open('cold/r_rename.pkl', 'rb'))
    print(2)
    print(3)
    print(4)
    gf.index = gf.index.map(r_rename.get)
    print(5)
    gfd = gf.to_dict()
    print(6)
    return gfd

