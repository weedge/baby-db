import sys,os
from util import run_script

# 测试插入和查询
def test_inserts_and_retrieves_row(db_file=""):
    result = run_script([
        "insert 1 user1 person1@example.com",
        "select",
        ".exit"
    ],db_file=db_file)
    expected_result = [
        "db > Executed.",
        "db > (1, user1, person1@example.com)",
        "Executed.",
        "db > "
    ]
    #print(f"result: {result}")
    assert result == expected_result, "Test failed"
    print(f"{sys._getframe().f_code.co_name} passed")

# 数据库现在可以容纳1400行，因为我们将最大页面数设置为100，并且一页可以容纳14行。
# 测试表已满的情况
def test_prints_error_message_when_table_is_full(db_file=""):
    script = [f"insert {i} user{i} person{i}@example.com" for i in range(1, 1402)]
    script.append(".exit")
    result = run_script(script,db_file=db_file)
    expected_result = "db > Error: Table full."
    assert result[-2] == expected_result, "Test failed"
    print(f"{sys._getframe().f_code.co_name} passed")


# 测试允许插入最大长度的字符串。
def test_allows_inserting_strings_that_are_maximum_length(db_file=""):
    long_username = "a" * 32
    long_email = "a" * 255
    script = [
        f"insert 1 {long_username} {long_email}",
        "select",
        ".exit",
    ]
    result = run_script(script,db_file=db_file)
    expected_result = [
        "db > Executed.",
        f"db > (1, {long_username}, {long_email})",
        "Executed.",
        "db > "
    ]
    print(f"result: {result}")
    assert result == expected_result, "Test failed"
    print(f"{sys._getframe().f_code.co_name} passed")


# 测试如果字符串太长，则会打印错误消息。
def test_prints_error_message_if_strings_are_too_long(db_file=""):
    long_username = "a" * 33
    long_email = "a" * 256
    script = [
        f"insert 1 {long_username} {long_email}",
        "select",
        ".exit",
    ]
    result = run_script(script,db_file=db_file)
    expected_result = [
        "db > String is too long.",
        "db > Executed.",
        "db > "
    ]
    assert result == expected_result, "Test failed"
    print(f"{sys._getframe().f_code.co_name} passed")


# 当尝试插入带有负 ID 的行时打印错误信息
def test_negative_id_error_message(db_file=""):
    script = [
        "insert -1 cstack foo@bar.com",
        "select",
        ".exit",
    ]
    result = run_script(script,db_file=db_file)
    expected_output = [
        "db > ID must be positive.",
        "db > Executed.",
        "db > ",
    ]
    assert result == expected_output, f"Expected: {expected_output}, but got: {result}"
    print(f"{sys._getframe().f_code.co_name} passed")


db_file = sys.argv[1] if len(sys.argv)>1 else ""
if os.path.exists(db_file):
    os.remove(db_file)

# Run the test
test_inserts_and_retrieves_row(db_file)
test_prints_error_message_when_table_is_full(db_file)
test_allows_inserting_strings_that_are_maximum_length(db_file)
test_prints_error_message_if_strings_are_too_long(db_file)
test_negative_id_error_message(db_file)

print("all tests passed.")