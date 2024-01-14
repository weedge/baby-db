import sys,os
from util import run_script

# 测试数据库关闭后，数据是否在数据库中
def test_keeps_data_after_closing_connection(db_file):
    result1 = run_script([
        "insert 1 user1 person1@example.com",
        ".exit",
    ],db_file=db_file)
    assert result1 == [
        "db > Executed.",
        "db > ",
    ]

    result2 = run_script([
        "select",
        ".exit",
    ],db_file=db_file)
    #print(f"result2: {result2}")
    assert result2 == [
        "db > (1, user1, person1@example.com)",
        "Executed.",
        "db > ",
    ]
    print(f"{sys._getframe().f_code.co_name} passed")

if len(sys.argv)<2:
    print(f"need db file path")
    exit(0)
db_file = sys.argv[1]
if os.path.exists(db_file):
    os.remove(db_file)

test_keeps_data_after_closing_connection(db_file)

print("all tests passed.")