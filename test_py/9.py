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
        "leaf (size 3)",
        "  - 0 : 1",
        "  - 1 : 2",
        "  - 2 : 3",
        "db > "
    ]

    print(f"result: {result}")
    assert result == expected_result
    print(f"{sys._getframe().f_code.co_name} passed")

def test_duplicate_id_error_message(db_file=""):
    script = [
        "insert 1 user1 person1@example.com",
        "insert 1 user1 person1@example.com",
        "select",
        ".exit",
    ]
    result = run_script(script,db_file=db_file,is_remove=True)
    expected_result = [
        "db > Executed.",
        "db > Error: Duplicate key.",
        "db > (1, user1, person1@example.com)",
        "Executed.",
        "db > ",
    ]
    print(f"result: {result}")
    assert result == expected_result
    print(f"{sys._getframe().f_code.co_name} passed")


if len(sys.argv)<2:
    print(f"need db file path")
    exit(0)
db_file = sys.argv[1]
if os.path.exists(db_file):
    print(f"{db_file} exists, remove it first")
    os.remove(db_file)

test_btree_order_structure(db_file)
test_duplicate_id_error_message(db_file)

print("all tests passed.")