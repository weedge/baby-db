clean:
	rm -f db *.db

format: format_c format_golang

format_c: c/*.c
	clang-format -style=Google -i c/*.c

format_golang: golang/*.go
	gofmt -w golang/*.go