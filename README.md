## mit6.824 2020
[schedule](https://pdos.csail.mit.edu/6.824/schedule.html)

## lab1
[lab1](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

```
install go 1.13.1
go version go1.13.1 darwin/amd64

git clone git://g.csail.mit.edu/6.824-golabs-2020 6.824

build erros then

go get github.com/maxfy1992/6.824-2020
cd src/mrapps
go build -buildmode=plugin wc.go
go run mrsequential.go ../mrapps/wc.so pg*.txt
more mr-out-0

sh test-mr.sh
*** Starting wc test.
*** Starting wc test.
2020/01/22 17:05:14 rpc.Register: method "Done" has 1 input parameters; needs exactly three
sort: cannot read: 'mr-out*': No such file or directory
cmp: EOF on mr-wc-all
--- wc output is not the same as mr-correct-wc.txt
--- wc test: FAIL
```