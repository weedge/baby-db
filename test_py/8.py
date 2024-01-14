import sys,os
from util import run_script

# 测试btree结构
def test_one_node_btree_structure(db_file=""):
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
        "leaf (size 3)",
        "  - 0 : 3",
        "  - 1 : 1",
        "  - 2 : 2",
        "db > "
    ]

    #print(f"result: {result}")
    assert result == expected_result
    print(f"{sys._getframe().f_code.co_name} passed")


# 测试打印常量
def test_print_constants(db_file=""):
    script = [
        ".constants",
        ".exit",
    ]
    result = run_script(script,db_file=db_file)

    expected_result = [
        "db > Constants:",
        "ROW_SIZE: 293",
        "COMMON_NODE_HEADER_SIZE: 6",
        "LEAF_NODE_HEADER_SIZE: 10",
        "LEAF_NODE_CELL_SIZE: 297",
        "LEAF_NODE_SPACE_FOR_CELLS: 4086",
        "LEAF_NODE_MAX_CELLS: 13",
        "db > ",
    ]

    assert result == expected_result
    print(f"{sys._getframe().f_code.co_name} passed")

if len(sys.argv)<2:
    print(f"need db file path")
    exit(0)
db_file = sys.argv[1]
if os.path.exists(db_file):
    os.remove(db_file)

test_one_node_btree_structure(db_file)
test_print_constants(db_file)

print("all tests passed.")