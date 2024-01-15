import sys,os
from util import run_script

# 数据库现在可以容纳1400行，因为我们将最大页面数设置为100，并且一页可以容纳14行。
# 测试表已满的情况
def test_prints_error_message_when_table_is_full(db_file=""):
    script = [f"insert {i} user{i} person{i}@example.com" for i in range(1, 1402)]
    script.append(".exit")
    result = run_script(script,db_file=db_file,is_remove=True)
    expected_result = [
        "db > Executed.", 
        "db > Need to implement splitting internal node",
    ]
    print(f"result: {result}")
    assert result[-2:] == expected_result, "Test failed"
    print(f"{sys._getframe().f_code.co_name} passed")

# 测试4个叶子节点的B+树的结构
def test_prints_structure_of_4_leaf_node_btree(db_file=""):
    script = [
        "insert 18 user18 person18@example.com",
        "insert 7 user7 person7@example.com",
        "insert 10 user10 person10@example.com",
        "insert 29 user29 person29@example.com",
        "insert 23 user23 person23@example.com",
        "insert 4 user4 person4@example.com",
        "insert 14 user14 person14@example.com",
        "insert 30 user30 person30@example.com",
        "insert 15 user15 person15@example.com",
        "insert 26 user26 person26@example.com",
        "insert 22 user22 person22@example.com",
        "insert 19 user19 person19@example.com",
        "insert 2 user2 person2@example.com",
        "insert 1 user1 person1@example.com",
        "insert 21 user21 person21@example.com",
        "insert 11 user11 person11@example.com",
        "insert 6 user6 person6@example.com",
        "insert 20 user20 person20@example.com",
        "insert 5 user5 person5@example.com",
        "insert 8 user8 person8@example.com",
        "insert 9 user9 person9@example.com",
        "insert 3 user3 person3@example.com",
        "insert 12 user12 person12@example.com",
        "insert 27 user27 person27@example.com",
        "insert 17 user17 person17@example.com",
        "insert 16 user16 person16@example.com",
        "insert 13 user13 person13@example.com",
        "insert 24 user24 person24@example.com",
        "insert 25 user25 person25@example.com",
        "insert 28 user28 person28@example.com",
        ".btree",
        ".exit",
    ]
    result = run_script(script,db_file=db_file)

    expected_output = [
        "db > Tree:",
        "- internal (size 3)",
        "  - leaf (size 7)",
        "    - 1",
        "    - 2",
        "    - 3",
        "    - 4",
        "    - 5",
        "    - 6",
        "    - 7",
        "  - key 7",
        "  - leaf (size 8)",
        "    - 8",
        "    - 9",
        "    - 10",
        "    - 11",
        "    - 12",
        "    - 13",
        "    - 14",
        "    - 15",
        "  - key 15",
        "  - leaf (size 7)",
        "    - 16",
        "    - 17",
        "    - 18",
        "    - 19",
        "    - 20",
        "    - 21",
        "    - 22",
        "  - key 22",
        "  - leaf (size 8)",
        "    - 23",
        "    - 24",
        "    - 25",
        "    - 26",
        "    - 27",
        "    - 28",
        "    - 29",
        "    - 30",
        "db > ",
    ]

    print(f"result: {result}")
    assert result[30:] == expected_output
    print(f"{sys._getframe().f_code.co_name} passed")


if len(sys.argv)<2:
    print(f"need db file path")
    exit(0)
db_file = sys.argv[1]
if os.path.exists(db_file):
    print(f"{db_file} exists, remove it first")
    os.remove(db_file)

test_prints_error_message_when_table_is_full(db_file)
test_prints_structure_of_4_leaf_node_btree(db_file)

print("all tests passed.")