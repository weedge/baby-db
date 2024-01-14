import sys,os
from util import run_script

# 测试btree有序结构
def test_btree_order_structure(db_file=""):
    script = [
        "insert 3 user3 person3@example.com",
        "insert 1 user1 person1@example.com",
        "insert 2 user2 person2@example.com",
        ".btree",
        ".exit"
    ]
    
    result = run_script(script,db_file=db_file)

    expected_result = [
        "db > Executed.",
        "db > Executed.",
        "db > Executed.",
        "db > Tree:",
        "- leaf (size 3)",
        "  - 1",
        "  - 2",
        "  - 3",
        "db > "
    ]

    print(f"result: {result}")
    assert result == expected_result
    print(f"{sys._getframe().f_code.co_name} passed")

# 测试打印3个叶子节点的btree
def test_print_structure_of_3_leaf_node_btree(db_file=""):
    script = [f"insert {i} user{i} person{i}@example.com" for i in range(1, 15)]
    script.append(".btree")
    script.append("insert 15 user15 person15@example.com")
    script.append(".exit")
    result = run_script(script,db_file=db_file)

    expected_result = [
        "db > Tree:",
        "- internal (size 1)",
        "  - leaf (size 7)",
        "    - 1",
        "    - 2",
        "    - 3",
        "    - 4",
        "    - 5",
        "    - 6",
        "    - 7",
        "  - key 7",
        "  - leaf (size 7)",
        "    - 8",
        "    - 9",
        "    - 10",
        "    - 11",
        "    - 12",
        "    - 13",
        "    - 14",
        "db > Need to implement searching an internal node",
    ]

    print(f"result: {result}")
    assert result[14:] == expected_result
    print(f"{sys._getframe().f_code.co_name} passed")

if len(sys.argv)<2:
    print(f"need db file path")
    exit(0)
db_file = sys.argv[1]
if os.path.exists(db_file):
    print(f"{db_file} exists, remove it first")
    os.remove(db_file)

test_btree_order_structure(db_file)
test_print_structure_of_3_leaf_node_btree(db_file)

print("all tests passed.")