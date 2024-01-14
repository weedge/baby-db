import sys,os
from util import run_script

def test_prints_all_rows_in_multi_level_tree(db_file=""):
    script = []
    for i in range(1, 16):
        script.append(f"insert {i} user{i} person{i}@example.com")
    script.append("select")
    script.append(".exit")
    result = run_script(script,db_file=db_file)

    expected_output = [
        "db > (1, user1, person1@example.com)",
        "(2, user2, person2@example.com)",
        "(3, user3, person3@example.com)",
        "(4, user4, person4@example.com)",
        "(5, user5, person5@example.com)",
        "(6, user6, person6@example.com)",
        "(7, user7, person7@example.com)",
        "(8, user8, person8@example.com)",
        "(9, user9, person9@example.com)",
        "(10, user10, person10@example.com)",
        "(11, user11, person11@example.com)",
        "(12, user12, person12@example.com)",
        "(13, user13, person13@example.com)",
        "(14, user14, person14@example.com)",
        "(15, user15, person15@example.com)",
        "Executed.", "db > ",
    ]
    print(f"result: {result}")
    assert result[15:] == expected_output
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
        "LEAF_NODE_HEADER_SIZE: 14",
        "LEAF_NODE_CELL_SIZE: 297",
        "LEAF_NODE_SPACE_FOR_CELLS: 4082",
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
    print(f"{db_file} exists, remove it first")
    os.remove(db_file)

test_prints_all_rows_in_multi_level_tree(db_file=db_file)
test_print_constants(db_file=db_file)

print("all tests passed.")