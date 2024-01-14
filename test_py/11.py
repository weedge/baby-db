import sys,os
from util import run_script

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
        "db > Executed.",
        "db > ",
    ]

    print(f"result: {result}")
    assert result[14:] == expected_result
    print(f"{sys._getframe().f_code.co_name} passed")

# 数据库现在可以容纳1400行，因为我们将最大页面数设置为100，并且一页可以容纳14行。
# 测试表已满的情况
def test_prints_error_message_when_table_is_full(db_file=""):
    script = [f"insert {i} user{i} person{i}@example.com" for i in range(1, 1402)]
    script.append(".exit")
    result = run_script(script,db_file=db_file,is_remove=True)
    expected_result = [
        "db > Executed.", 
        "db > Need to implement updating parent after split",
    ]
    print(f"result: {result}")
    assert result[-2:] == expected_result, "Test failed"
    print(f"{sys._getframe().f_code.co_name} passed")


if len(sys.argv)<2:
    print(f"need db file path")
    exit(0)
db_file = sys.argv[1]
if os.path.exists(db_file):
    print(f"{db_file} exists, remove it first")
    os.remove(db_file)

test_print_structure_of_3_leaf_node_btree(db_file)
test_prints_error_message_when_table_is_full(db_file)

print("all tests passed.")