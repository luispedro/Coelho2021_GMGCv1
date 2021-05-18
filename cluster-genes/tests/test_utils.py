import utils

def test_groups_of():
    for lst in ["acbdefghijklmnop"
                ,"1"
                ,"123"]:
        for bs in [1, 2, 3, 5]:
            assert sum(len(b) for b in utils.groups_of(lst, bs)) == len(lst)
            assert lst == ''.join(''.join(b) for b in utils.groups_of(lst, bs))
